"""Offline train-commute model centred on Mäntsälä.

A small, fully self-contained rail network: station coordinates and line
topology are embedded as module constants, so this works on Streamlit Cloud
with zero file or network IO. Segment travel times are *synthesised* from
geometry (great-circle distance / line speed + per-stop dwell) unless a line
carries explicit ``segment_min`` timetable values, and are calibrated against
known real journeys (see ``tests/test_transit.py``).

Line changes (e.g. Mäntsälä -> Järvenpää means leaving the oikorata train
at Kerava and boarding a päärata train) add a fixed transfer surcharge,
so routed times include the mandatory connection wait. Two connection
qualities are available: ``connection="best"`` (tightest possible) and
``connection="rush"`` (typical morning-rush wait) — the difference between
them is the commute-time variance the timetable's connection quality adds.

The public surface is deliberately small so a real schedule-based backend
(e.g. Digitransit / OpenTripPlanner) could later replace the internals without
touching callers:

* :func:`rail_minutes_from` -- train-only minutes from an origin to every station
* :func:`commute_minutes`   -- door-to-door estimate (rail + last-mile access)
* :func:`reachable_stations`-- isochrone disks for a time budget
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .distance import haversine_km

DEFAULT_ORIGIN = "Mäntsälä"

# Last-mile access speeds (km/h) from the alighting station to the destination.
ACCESS_SPEEDS_KMH: Dict[str, float] = {
    "walk": 4.5,
    "bike": 15.0,
    "bus": 22.0,
}

# Dwell time added per intermediate stop (minutes). Accounts for braking,
# stopping and re-accelerating; folded into each synthesised edge.
DWELL_MIN = 0.7

# Surcharge when the shortest path switches lines at a shared station — the
# real journey means stepping off one train and waiting for the next.
# Calibrated on the Mäntsälä -> Riihimäki connection: Z to Kerava + change +
# R onwards is ~62 min door-to-station (verified against the timetable).
TRANSFER_MIN = 5.0

# The same connection on a morning-rush departure. The timetable pairs some
# departures badly (e.g. Mäntsälä -> Järvenpää: 6–7 o'clock departures are
# ~58 min door-to-station, from 9 o'clock only ~36 min) because the wait for
# the connecting train at Kerava dominates. ``connection="rush"`` prices that
# typical morning wait; ``connection="best"`` keeps the tightest possible
# connection. Calibrated so that rush Järvenpää + the dashboard's default
# 10 min overhead ≈ the observed 58 min.
TRANSFER_RUSH_MIN = 25.0


@dataclass(frozen=True)
class Line:
    """An ordered run of stations sharing an effective line speed (km/h).

    ``segment_min`` optionally carries explicit timetable minutes per gap
    (one entry per adjacent station pair). ``None`` entries fall back to
    synthesising the gap from geometry at the line speed. Use these where
    straight-line geometry badly underestimates the real track (e.g. the
    curving Hyvinkää -> Riihimäki section) so calibration stays anchored.
    """

    name: str
    speed_kmh: float
    stations: Tuple[str, ...]
    segment_min: Optional[Tuple[Optional[float], ...]] = None


# Station coordinates (WGS84). Only the stops relevant to a Mäntsälä commute.
STATION_COORDS: Dict[str, Tuple[float, float]] = {
    "Helsinki": (60.1718, 24.9414),
    "Pasila": (60.1987, 24.9334),
    "Tikkurila": (60.2925, 25.0439),
    "Savio": (60.3804, 25.0977),
    "Kerava": (60.4043, 25.1057),
    "Järvenpää": (60.4733, 25.0903),
    "Hyvinkää": (60.6310, 24.8590),
    "Riihimäki": (60.7186, 24.7740),
    "Haarajoki": (60.5363, 25.1198),
    "Mäntsälä": (60.6113, 25.3186),
    "Henna": (60.7284, 25.5270),
    "Lahti": (60.9764, 25.6574),
    "Kouvola": (60.8664, 26.7045),
}

# Effective line speeds are calibrated, not nominal track speeds: the dense
# Helsinki–Kerava section is slow despite high track limits, while the Kerava–
# Lahti direct line (oikorata) is genuinely fast. The Kerava–Riihimäki gaps use
# explicit R-train timetable minutes (geometry underestimates the curving
# track by ~20% there and the synthesised 90 km/h was far too optimistic).
# Verified against the real timetable 2026-10: Mäntsälä -> Järvenpää ~36 min,
# Mäntsälä -> Riihimäki ~1 h 02 via the R train from Kerava.
RAIL_LINES: Tuple[Line, ...] = (
    Line(
        "Päärata (Helsinki–Riihimäki)",
        72.0,
        ("Helsinki", "Pasila", "Tikkurila", "Savio", "Kerava", "Järvenpää", "Hyvinkää", "Riihimäki"),
        segment_min=(None, None, None, None, 8.0, 15.0, 19.0),
    ),
    # The Z runs Lahti -> Helsinki *through* Kerava on one train, so journeys
    # towards Helsinki must not pay a transfer surcharge. Without this line the
    # router would charge the (rush: heavy) Kerava connection for a ride the
    # commuter never leaves. Kerava -> Helsinki gaps are explicit timetable
    # minutes; the oikorata gaps synthesise at the same 120 km/h as the line
    # below, so the two representations agree. One entry per adjacent pair:
    # Kerava->Tikkurila 7, Tikkurila->Pasila 8, Pasila->Helsinki 4.
    Line(
        "Z-juna (Lahti–Helsinki)",
        120.0,
        ("Lahti", "Henna", "Mäntsälä", "Haarajoki", "Kerava", "Tikkurila", "Pasila", "Helsinki"),
        segment_min=(None, None, None, None, 7.0, 8.0, 4.0),
    ),
    Line("Oikorata (Kerava–Lahti)", 120.0, ("Kerava", "Haarajoki", "Mäntsälä", "Henna", "Lahti")),
    Line("Lahti–Riihimäki", 95.0, ("Lahti", "Riihimäki")),
    Line("Lahti–Kouvola", 110.0, ("Lahti", "Kouvola")),
)


def _edge_minutes(line: Line, i: int) -> float:
    """Minutes for gap ``i`` of ``line``: timetable value or synthesised."""
    if line.segment_min is not None and line.segment_min[i] is not None:
        return float(line.segment_min[i])
    a, b = line.stations[i], line.stations[i + 1]
    return _segment_minutes(a, b, line.speed_kmh)


def _segment_minutes(a: str, b: str, speed_kmh: float) -> float:
    """Travel minutes between adjacent stations, including arrival dwell."""
    lat1, lon1 = STATION_COORDS[a]
    lat2, lon2 = STATION_COORDS[b]
    km = haversine_km(lat1, lon1, lat2, lon2)
    return km / speed_kmh * 60.0 + DWELL_MIN


def build_graph(
    lines: Tuple[Line, ...] = RAIL_LINES,
) -> Dict[str, List[Tuple[str, float]]]:
    """Undirected adjacency list {station: [(neighbour, minutes), ...]}.

    Stations shared between lines (e.g. Kerava, Lahti) merge into one node, so
    Dijkstra can route via whichever line is fastest.
    """
    adj: Dict[str, List[Tuple[str, float]]] = {name: [] for name in STATION_COORDS}
    for line in lines:
        for i in range(len(line.stations) - 1):
            a, b = line.stations[i], line.stations[i + 1]
            minutes = _edge_minutes(line, i)
            adj[a].append((b, minutes))
            adj[b].append((a, minutes))
    return adj


def _line_adjacency(
    lines: Tuple[Line, ...],
) -> Dict[str, List[Tuple[str, float, int]]]:
    """Line-tagged adjacency {station: [(neighbour, minutes, line_index)]}.

    ``build_graph`` merges lines at shared stations, which loses the identity
    of the train a leg runs on; the router needs it to know when a path
    changes trains and must pay :data:`TRANSFER_MIN`.
    """
    adj: Dict[str, List[Tuple[str, float, int]]] = {name: [] for name in STATION_COORDS}
    for idx, line in enumerate(lines):
        for i in range(len(line.stations) - 1):
            a, b = line.stations[i], line.stations[i + 1]
            minutes = _edge_minutes(line, i)
            adj[a].append((b, minutes, idx))
            adj[b].append((a, minutes, idx))
    return adj


def rail_minutes_from(
    origin: str = DEFAULT_ORIGIN,
    lines: Tuple[Line, ...] = RAIL_LINES,
    connection: str = "best",
) -> Dict[str, float]:
    """Shortest train-only minutes from ``origin`` to every reachable station.

    Dijkstra runs over ``(station, line)`` states: boarding the first train is
    free, but every later line change costs a transfer surcharge on top of the
    edge, so journeys that need a change of trains (Mäntsälä -> Järvenpää
    changes at Kerava; Mäntsälä -> Kouvola changes at Lahti) are not priced
    as zero-cost connections. Through services that stay on one line
    (Helsinki -> Riihimäki on the merged päärata, or Mäntsälä -> Helsinki on
    the Z) pay nothing.

    ``connection`` picks the transfer surcharge:

    * ``"best"``  — tightest possible connection (:data:`TRANSFER_MIN`);
    * ``"rush"``  — the typical morning-rush connection wait
      (:data:`TRANSFER_RUSH_MIN`), so the range between the two tells how
      much the timetable's connection quality can add to a commute.
    """
    if origin not in STATION_COORDS:
        raise KeyError(f"Unknown origin station: {origin!r}")
    if connection == "best":
        transfer_cost = TRANSFER_MIN
    elif connection == "rush":
        transfer_cost = TRANSFER_RUSH_MIN
    else:
        raise ValueError(f"Unknown connection mode: {connection!r} (use 'best' or 'rush')")
    adj = _line_adjacency(lines)
    BOARDING = -1  # pseudo-line of the origin state: boarding costs nothing
    best: Dict[Tuple[str, int], float] = {(origin, BOARDING): 0.0}
    heap: List[Tuple[float, str, int]] = [(0.0, origin, BOARDING)]
    out: Dict[str, float] = {}
    while heap:
        cost, node, li = heapq.heappop(heap)
        if cost > best.get((node, li), float("inf")):
            continue
        # States pop in increasing cost, so the first visit of a station is
        # its shortest rail time regardless of which line arrived. The state
        # must still expand, though: a slightly costlier arrival on another
        # line (e.g. the Z at Pasila when the päärata got there first) may be
        # the only way to continue without paying a transfer, and dropping it
        # would lose through-service routes.
        if node not in out:
            out[node] = cost
        for nbr, w, lj in adj[node]:
            transfer = transfer_cost if li not in (BOARDING, lj) else 0.0
            nxt = cost + w + transfer
            if nxt < best.get((nbr, lj), float("inf")):
                best[(nbr, lj)] = nxt
                heapq.heappush(heap, (nxt, nbr, lj))
    return out


def access_minutes(distance_km: float, mode: str = "walk") -> float:
    """Last-mile minutes from a station to a point ``distance_km`` away."""
    speed = ACCESS_SPEEDS_KMH.get(mode, ACCESS_SPEEDS_KMH["walk"])
    return float(distance_km) / speed * 60.0


def commute_minutes(
    nearest_station: str,
    distance_km: float,
    *,
    origin: str = DEFAULT_ORIGIN,
    mode: str = "walk",
    rail_minutes: Optional[Dict[str, float]] = None,
    overhead_min: float = 0.0,
) -> Optional[float]:
    """Door-to-door estimate: rail time + last-mile + a fixed overhead.

    ``overhead_min`` accounts for what in-vehicle time ignores — average wait for
    the (infrequent) train and getting to/from the platforms — so the estimate
    isn't faster than the real journey. Returns ``None`` when the station is
    unknown to the network or the inputs are missing.
    """
    if not nearest_station or nearest_station not in STATION_COORDS:
        return None
    if distance_km is None:
        return None
    rail = rail_minutes if rail_minutes is not None else rail_minutes_from(origin)
    leg = rail.get(nearest_station)
    if leg is None:
        return None
    return leg + access_minutes(distance_km, mode) + float(overhead_min)


@dataclass(frozen=True)
class ReachableStation:
    """A station inside the time budget and the disk it can still reach."""

    name: str
    lat: float
    lon: float
    rail_minutes: float
    reach_km: float


def reachable_stations(
    budget_min: float,
    *,
    origin: str = DEFAULT_ORIGIN,
    mode: str = "walk",
    lines: Tuple[Line, ...] = RAIL_LINES,
) -> List[ReachableStation]:
    """Isochrone as a union of disks: one per station reachable within budget.

    Each reachable station contributes a disk whose radius is the distance the
    remaining time budget buys at the chosen access speed. The union of these
    disks approximates the area reachable from ``origin`` within ``budget_min``.
    """
    speed = ACCESS_SPEEDS_KMH.get(mode, ACCESS_SPEEDS_KMH["walk"])
    rail = rail_minutes_from(origin, lines)
    out: List[ReachableStation] = []
    for name, minutes in rail.items():
        if minutes > budget_min:
            continue
        reach_km = max(0.0, (budget_min - minutes) / 60.0 * speed)
        lat, lon = STATION_COORDS[name]
        out.append(ReachableStation(name, lat, lon, minutes, reach_km))
    out.sort(key=lambda r: r.rail_minutes)
    return out
