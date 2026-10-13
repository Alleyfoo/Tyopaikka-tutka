"""Public-facing dashboard — Tyopaikka-tutka.

Browse employers near Finnish railway stations.
Deploy to Streamlit Community Cloud (streamlit.io/cloud) — reads
out/companies.csv which is committed to the repo.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st

# Make the src-layout `apprscan` package importable on Streamlit Cloud, where
# only requirements.txt is installed (the package itself is not pip-installed).
_SRC = Path(__file__).parent / "src"
if _SRC.is_dir() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from apprscan import transit

# Carto positron basemap: renders without a Mapbox token, so the map works on
# Streamlit Cloud and locally without any secret configured.
CARTO_BASEMAP = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"

# Finnish labels for the last-mile access modes exposed by the commute model.
ACCESS_MODE_LABELS = {
    "walk": "🚶 Kävely",
    "bike": "🚲 Pyörä",
    "bus": "🚌 Bussi / liityntä",
}

# ---- Lead scoring -------------------------------------------------------
# Lead scoring lives in apprscan.leads (shared with scripts/lead_targets.py).
from apprscan.leads import LEAD_AXES, LEAD_SCORE_MAX, add_lead_scores  # noqa: E402
from apprscan import feedback  # noqa: E402
from apprscan.employer_fit import (  # noqa: E402
    EMPLOYER_AXES,
    EMPLOYER_SCORE_MAX,
    add_employer_scores,
)

# Map axes: sales-lead axes + employer-fit axes + the employer total. Each
# axis normalises its colour against its own max (leads max 8, employer 10).
MAP_AXES = {**LEAD_AXES, **EMPLOYER_AXES, "Kokonaisuus": "e_fit"}
AXIS_MAX = {col: LEAD_SCORE_MAX for col in LEAD_AXES.values()}
AXIS_MAX.update({col: EMPLOYER_SCORE_MAX for col in EMPLOYER_AXES.values()})
AXIS_MAX["e_fit"] = EMPLOYER_SCORE_MAX

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Tyopaikka-tutka",
    page_icon="🔍",
    layout="wide",
)

DATA_PATH = Path(__file__).parent / "out" / "companies.csv"
WEBSHOPS_PATH = Path(__file__).parent / "out" / "webshops.csv"

# Coordinates come from apprscan.transit (single source of truth — the real
# railway stations), so map markers, distance, and the rail model all agree.
STATION_INFO = {
    key: {"label": label, "lat": transit.STATION_COORDS[key][0], "lon": transit.STATION_COORDS[key][1]}
    for key, label in [
        ("Lahti", "Lahti"),
        ("Kerava", "Kerava"),
        ("Savio", "Savio (Kerava Etelä)"),
        ("Pasila", "Helsinki Pasila"),
        ("Tikkurila", "Tikkurila (Vantaa)"),
        ("Järvenpää", "Järvenpää"),
        ("Hyvinkää", "Hyvinkää"),
        ("Riihimäki", "Riihimäki"),
        ("Haarajoki", "Haarajoki (Järvenpää)"),
        ("Henna", "Henna (Mäntsälä)"),
    ]
}

INDUSTRY_LABELS = {
    "it": "💻 IT & ohjelmistot",
    "construction": "🏗️ Rakentaminen",
    "wholesale": "📦 Tukkukauppa",
    "marketing": "📣 Markkinointi & konsultointi",
    "health": "🏥 Terveys",
    "manufacturing": "🏭 Teollisuus & valmistus",
    "logistics": "🚚 Kuljetus & logistiikka",
    "finance": "💰 Rahoitus",
    "retail": "🛒 Vähittäiskauppa",
    "hospitality": "🍽️ Ravintola & majoitus",
    "education": "🎓 Koulutus",
    "engineering": "⚙️ Tekniikka & suunnittelu",
    "staffing": "👥 Henkilöstöpalvelut",
    "real_estate": "🏢 Kiinteistöala",
    "other": "📋 Muut",
}

INDUSTRY_COLORS = {
    "it": [0, 120, 215],
    "construction": [255, 140, 0],
    "wholesale": [0, 180, 100],
    "marketing": [180, 0, 180],
    "health": [220, 50, 50],
    "manufacturing": [80, 80, 80],
    "logistics": [200, 160, 0],
    "finance": [0, 160, 200],
    "retail": [255, 80, 120],
    "hospitality": [255, 120, 0],
    "education": [100, 60, 200],
    "engineering": [0, 140, 140],
    "staffing": [160, 80, 0],
    "real_estate": [120, 120, 40],
    "other": [160, 160, 160],
}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_data(_mtime: float) -> pd.DataFrame:
    # _mtime is part of the cache key, so editing out/companies.csv invalidates
    # the cache and the app reloads the fresh data instead of stale columns.
    df = pd.read_csv(DATA_PATH, encoding="utf-8-sig")
    df["distance_km"] = df["distance_km"].round(2)
    # Effective distance shown/used everywhere: walking road distance when
    # available (more honest), falling back to straight-line.
    if "road_distance_km" in df.columns:
        df["dist_km"] = (
            pd.to_numeric(df["road_distance_km"], errors="coerce").round(2).fillna(df["distance_km"])
        )
    else:
        df["dist_km"] = df["distance_km"]
    df["registered"] = pd.to_numeric(
        df["registered"].astype(str).str[:4], errors="coerce"
    )
    # Merge PRH website + DDG-discovered website into one column
    if "found_website" in df.columns:
        df["best_website"] = df["website"].combine_first(df["found_website"])
    else:
        df["best_website"] = df["website"]
    df["color"] = df["industry"].map(lambda x: INDUSTRY_COLORS.get(x, [160, 160, 160]))
    return df


@st.cache_data(show_spinner=False)
def load_webshops(_mtime: float) -> pd.DataFrame:
    """out/webshops.csv: every live retail/wholesale company with its shop probe.

    Written by scripts/find_webshops.py — carries all companies.csv columns
    plus shop_* probe columns and _host. Same distance/website fixes as
    load_data so commute math works here too.
    """
    shops = pd.read_csv(WEBSHOPS_PATH, encoding="utf-8-sig")
    for col in ("shop_catalog_estimate", "shop_score", "shop_sitemap_products",
                "shop_homepage_product_links"):
        if col in shops.columns:
            shops[col] = pd.to_numeric(shops[col], errors="coerce")
    shops["shop_catalog_estimate"] = shops["shop_catalog_estimate"].fillna(0).astype(int)
    shops["shop_score"] = shops["shop_score"].fillna(0).astype(int)
    # The sitemap counter stopped at its cap for giant shops — the catalog
    # estimate is then a floor, shown as "100 000+".
    if "shop_sitemap_capped" in shops.columns:
        shops["shop_sitemap_capped"] = (
            shops["shop_sitemap_capped"].astype(str).str.lower() == "true"
        )
    else:
        shops["shop_sitemap_capped"] = False
    if "road_distance_km" in shops.columns:
        shops["dist_km"] = (
            pd.to_numeric(shops["road_distance_km"], errors="coerce")
            .round(2)
            .fillna(pd.to_numeric(shops["distance_km"], errors="coerce").round(2))
        )
    else:
        shops["dist_km"] = pd.to_numeric(shops["distance_km"], errors="coerce").round(2)
    shops["registered"] = pd.to_numeric(
        shops["registered"].astype(str).str[:4], errors="coerce"
    )
    if "found_website" in shops.columns:
        shops["best_website"] = shops["website"].combine_first(shops["found_website"])
    else:
        shops["best_website"] = shops["website"]
    return shops


# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("🔍 Tyopaikka-tutka")
    st.caption("Etsi työnantajia asemasi läheltä")
    st.divider()

    df_raw = load_data(DATA_PATH.stat().st_mtime if DATA_PATH.exists() else 0.0)

    # Station selector
    st.subheader("📍 Asema / kaupunki")
    station_options = {
        k: v["label"]
        for k, v in STATION_INFO.items()
        if k in df_raw["nearest_station"].unique()
    }
    selected_keys = st.multiselect(
        "Valitse asemat",
        options=list(station_options.keys()),
        default=list(station_options.keys()),
        format_func=lambda k: station_options[k],
        key="station_select",
    )

    # Distance slider
    st.subheader("📏 Etäisyys asemalta")
    max_dist = st.slider(
        "Enintään (km)",
        min_value=0.5,
        max_value=5.0,
        value=4.0,
        step=0.5,
        key="max_dist",
    )
    st.caption("⚠️ Etäisyys on kävelymatka kadulta asemalle (arvio; ei katuosoitteen tarkkuutta).")

    # Train commute from a home station
    st.subheader("🚆 Työmatka junalla")
    origin_station = st.selectbox(
        "Mistä matkustat",
        options=sorted(transit.STATION_COORDS.keys()),
        index=sorted(transit.STATION_COORDS.keys()).index(transit.DEFAULT_ORIGIN),
        help="Junamatkan lähtöasema. Työmatka = juna lähtöasemalta + viime kilometri asemalta yritykselle.",
        key="origin_station",
    )
    _access_keys = list(ACCESS_MODE_LABELS.keys())
    access_mode = st.selectbox(
        "Viime kilometri",
        options=_access_keys,
        index=_access_keys.index("bus"),
        format_func=lambda k: ACCESS_MODE_LABELS[k],
        help="Oletus: bussi/liityntä — kävely on epärealistinen useamman kilometrin matkalla.",
        key="access_mode",
    )
    overhead_min = st.slider(
        "Odotus + liityntä (min)",
        min_value=0,
        max_value=30,
        value=10,
        step=1,
        help="Junan keskimääräinen odotusaika + matka laiturille. Lisätään työmatka-aikaan, "
        "jotta arvio ei ole nopeampi kuin todellinen matka.",
        key="overhead_min",
    )
    commute_limit_on = st.toggle(
        "Rajaa työmatkan keston mukaan", value=False, key="commute_limit_on"
    )
    max_commute = st.slider(
        "Enintään (min)",
        min_value=15,
        max_value=120,
        value=60,
        step=5,
        disabled=not commute_limit_on,
        key="max_commute",
    )

    # Industry filter
    st.subheader("🏭 Toimiala")
    available_industries = sorted(df_raw["industry"].dropna().unique())
    industry_options = {k: INDUSTRY_LABELS.get(k, k) for k in available_industries}
    selected_industries = st.multiselect(
        "Valitse toimialat",
        options=list(industry_options.keys()),
        default=list(industry_options.keys()),
        format_func=lambda k: industry_options[k],
        key="industry_select",
    )

    # Website toggle
    only_with_website = st.toggle(
        "Vain yritykset joilla on verkkosivut", value=False, key="only_with_website"
    )
    only_live = st.toggle(
        "Vain toimivat sivut",
        value=False,
        help="Näytä vain yritykset joiden verkkosivu vastaa ja näyttää oikealta "
        "(ei kuollut tai parkkeerattu) — karkea merkki siitä että yritys on aktiivinen.",
        key="only_live",
    )
    only_hiring = st.toggle(
        "Vain rekrytoivat (AI)",
        value=False,
        help="Näytä vain yritykset joiden sivulla AI-analyysi havaitsi "
        "rekrytointisignaalin (vain analysoiduille yrityksille).",
        key="only_hiring",
    )

    # Registration year
    min_year, max_year = int(df_raw["registered"].min()), int(
        df_raw["registered"].max()
    )
    reg_range = st.slider(
        "Rekisteröintivuosi",
        min_value=min_year,
        max_value=max_year,
        value=(min_year, max_year),
        key="reg_range",
    )

    st.divider()
    st.caption("Lähde: PRH avoin yritysrekisteri · Päivitetty huhtikuu 2026")


# ---------------------------------------------------------------------------
# Apply filters
# ---------------------------------------------------------------------------
df = df_raw.copy()

if selected_keys:
    df = df[df["nearest_station"].isin(selected_keys)]
else:
    df = df.iloc[:0]  # empty if nothing selected

df = df[df["dist_km"] <= max_dist]
df = df[df["industry"].isin(selected_industries)]
df = df[df["registered"].between(reg_range[0], reg_range[1], inclusive="both")]

if only_with_website:
    df = df[df["best_website"].notna()]
if only_live and "website_status" in df.columns:
    df = df[df["website_status"] == "live"]
if only_hiring and "llm_is_hiring" in df.columns:
    df = df[df["llm_is_hiring"].astype(str).str.lower().isin(["true", "1", "yes"])]

# Train commute from the chosen origin: rail time + last-mile (walking road
# distance, dist_km) + a fixed wait/access overhead so the estimate isn't faster
# than the real journey. Two connection qualities: the best case (tightest
# possible change of trains) and the rush case (typical morning connection wait),
# so the UI can show the range the timetable's connection quality adds.
_rail = transit.rail_minutes_from(origin_station)
_rail_rush = transit.rail_minutes_from(origin_station, connection="rush")


def _commute_minutes(row_rail, station, dist):
    return transit.commute_minutes(
        station, dist, mode=access_mode, rail_minutes=row_rail, overhead_min=overhead_min
    )


def _commute_display(best, rush):
    """Format the commute range: typical first, best case in parentheses.

    E.g. "58 min (nopein 36)" when the Kerava connection wait dominates the
    rush trip, plain "31 min" for destinations where the two qualities agree.
    """
    if best is None or pd.isna(best):
        return "–"
    if rush is not None and pd.notna(rush) and rush > best + 0.5:
        return f"{rush:.0f} min (nopein {best:.0f})"
    return f"{best:.0f} min"


df["commute_min"] = [
    _commute_minutes(_rail, s, d) for s, d in zip(df["nearest_station"], df["dist_km"])
]
df["commute_min_rush"] = [
    _commute_minutes(_rail_rush, s, d) for s, d in zip(df["nearest_station"], df["dist_km"])
]
# The commute limit is checked against the rush estimate: an employer that is
# only reachable inside the limit with a perfect connection is not inside it.
if commute_limit_on:
    df = df[
        df["commute_min_rush"].notna() & (df["commute_min_rush"] <= max_commute)
    ]

# Lead scores (s_webshop/s_pim/s_data) for the map and the Liidit tab, and
# employer-fit scores (e_industry/e_hiring/e_commute/e_fit) for the
# Työnantajasopivuus tab. commute_min is already computed above, so the fit
# scores respond to the sidebar's origin/mode/overhead choices.
df = add_lead_scores(df)
df = add_employer_scores(df)

# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------
st.title("Tyopaikka-tutka 🔍")
st.subheader("Työnantajat asemasi lähialueella")

if df.empty:
    st.warning("Ei yrityksiä valituilla hakuehdoilla. Laajenna hakuasi.")
    st.stop()

# Metric cards
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Yrityksiä yhteensä", len(df))
if "website_status" in df.columns:
    _live = int((df["website_status"] == "live").sum())
    col2.metric(
        "Toimivia sivuja",
        _live,
        help="Verkkosivu vastaa ja näyttää oikealta (ei kuollut/parkkeerattu).",
    )
else:
    col2.metric("Joilla verkkosivut", int(df["best_website"].notna().sum()))
col3.metric("Eri toimialoja", df["industry"].nunique())
col4.metric("Alueita", df["nearest_station"].nunique())
_fastest = df["commute_min"].min()
_fastest_rush = df["commute_min_rush"].min()
col5.metric(
    "Nopein työmatka",
    (
        _commute_display(_fastest, _fastest_rush)
        if pd.notna(_fastest)
        else "–"
    ),
    help=(
        f"Junalla asemalta {origin_station}, sis. viime kilometrin. Suluissa "
        "nopein mahdollinen vaihtoyhteys — ruuhka-aamun vaihto voi olla hitaampi."
    ),
)

st.divider()

# ---- Map ----------------------------------------------------------------
tab_map, tab_reach, tab_table, tab_shops, tab_stats, tab_leads, tab_fit = st.tabs(
    ["🗺️ Kartta", "🚆 Junasaavutettavuus", "📋 Yritykset", "🛒 Verkkokaupat", "📊 Tilastot", "🎯 Liidit", "💼 Työnantajasopivuus"]
)

with tab_map:
    valid_coords = df.dropna(subset=["lat", "lon"]).copy()
    if valid_coords.empty:
        st.info("Ei koordinaatteja saatavilla valituille yrityksille.")
    else:
        # The map is filled from lead scoring: points are coloured and sized by
        # their lead score for the chosen axis, browsable in pages and sortable
        # by city (station). With the station filter this gives e.g. "top 100
        # leads of Kerava, next 100, ...".
        c1, c2, c3 = st.columns(3)
        axis_label = c1.selectbox("Pisteytysakseli", list(MAP_AXES.keys()))
        axis_col = MAP_AXES[axis_label]
        sort_choice = c2.selectbox(
            "Järjestys", ["Liidipisteet", "Kaupunki (asema)", "Lähin asemaa"]
        )
        page_size_label = c3.selectbox(
            "Näytä kerralla", ["100", "250", "500", "1000", "Kaikki"], index=0
        )

        if sort_choice == "Kaupunki (asema)":
            valid_coords = valid_coords.sort_values(
                ["nearest_station", axis_col], ascending=[True, False], na_position="last"
            )
        elif sort_choice == "Lähin asemaa":
            valid_coords = valid_coords.sort_values("dist_km", na_position="last")
        else:
            valid_coords = valid_coords.sort_values(
                [axis_col, "commute_min_rush"], ascending=[False, True], na_position="last"
            )

        total = len(valid_coords)
        page_size = total if page_size_label == "Kaikki" else int(page_size_label)
        n_pages = max(1, (total + page_size - 1) // page_size)
        page = 1
        if n_pages > 1:
            page = st.number_input(
                "Sivu", min_value=1, max_value=n_pages, value=1, step=1, key="map_page"
            )
        lo = (int(page) - 1) * page_size
        hi = min(lo + page_size, total)
        page_df = valid_coords.iloc[lo:hi].copy()
        st.caption(
            f"Näytetään {lo + 1}–{hi} / {total} yritystä · väri & koko = {axis_label}"
        )

        # Many companies geocode to the same street point, so a page of 100 can
        # collapse to a handful of visible dots. Spread each point
        # deterministically (~±180 m) around its street so every lead shows.
        _h = pd.util.hash_pandas_object(
            page_df["business_id"].astype(str), index=False
        ).to_numpy()
        page_df["lat"] = page_df["lat"].astype(float) + ((_h % 1000) / 1000.0 - 0.5) * 0.0032
        page_df["lon"] = page_df["lon"].astype(float) + (((_h // 1000) % 1000) / 1000.0 - 0.5) * 0.0064

        page_df["commute_disp"] = page_df.apply(
            lambda r: _commute_display(r["commute_min"], r["commute_min_rush"]), axis=1
        )
        page_df["lead_score"] = page_df[axis_col]

        # Normalise against the chosen axis's own max, not a hardcoded one.
        _axis_max = AXIS_MAX.get(axis_col, LEAD_SCORE_MAX)

        def _lead_color(s):
            t = max(0.0, min(1.0, (float(s) if pd.notna(s) else 0) / _axis_max))
            return [int(70 + 170 * t), int(170 - 110 * t), 70, 200]

        page_df["color"] = page_df["lead_score"].map(_lead_color)
        page_df["radius"] = page_df["lead_score"].map(
            lambda s: 50 + (float(s) if pd.notna(s) else 0) / _axis_max * 160
        )

        # What the LLM wrote about the site (+ hiring/shop flags), for the tooltip.
        def _ai_note(row):
            sells, desc = row.get("llm_sells"), row.get("llm_description")
            text = sells if (isinstance(sells, str) and sells.strip()) else desc
            if not (isinstance(text, str) and text.strip()):
                return ""
            flags = []
            if str(row.get("llm_is_hiring")).lower() in ("true", "1", "yes"):
                flags.append("📢 rekrytoi")
            if str(row.get("llm_has_shop")).lower() in ("true", "1", "yes"):
                flags.append("🛒 verkkokauppa")
            prefix = (" · ".join(flags) + " · ") if flags else ""
            return (prefix + text.strip())[:170]

        if "llm_description" in page_df.columns:
            page_df["ai_note"] = page_df.apply(_ai_note, axis=1)
        else:
            page_df["ai_note"] = ""

        scatter = pdk.Layer(
            "ScatterplotLayer",
            data=page_df[
                ["lat", "lon", "name", "nearest_station", "lead_score", "industry",
                 "commute_disp", "ai_note", "best_website", "color", "radius"]
            ].copy(),
            get_position=["lon", "lat"],
            get_fill_color="color",
            get_radius="radius",
            pickable=True,
            opacity=0.8,
        )
        # Station markers carry the same tooltip keys so hovering them shows the
        # station name rather than a literal "{name}".
        station_data = [
            {
                "lat": v["lat"], "lon": v["lon"], "name": f"🚉 {v['label']}",
                "nearest_station": k, "lead_score": "", "industry": "Asema",
                "commute_disp": "", "ai_note": "", "best_website": "",
            }
            for k, v in STATION_INFO.items()
            if k in selected_keys
        ]
        station_layer = pdk.Layer(
            "ScatterplotLayer",
            data=station_data,
            get_position=["lon", "lat"],
            get_fill_color=[255, 30, 30, 220],
            get_radius=220,
            pickable=True,
        )
        # Auto-fit the view to the points actually shown.
        try:
            view = pdk.data_utils.compute_view(
                page_df[["lon", "lat"]].astype(float).values.tolist()
            )
            view.pitch = 0
        except Exception:
            view = pdk.ViewState(
                latitude=page_df["lat"].astype(float).mean(),
                longitude=page_df["lon"].astype(float).mean(),
                zoom=10,
                pitch=0,
            )
        chart = pdk.Deck(
            layers=[scatter, station_layer],
            initial_view_state=view,
            tooltip={
                "text": "{name}\nKaupunki: {nearest_station}\nLiidipisteet: {lead_score}\nToimiala: {industry}\nTyömatka: {commute_disp}\n{ai_note}\n{best_website}"
            },
            map_style=CARTO_BASEMAP,
        )
        st.pydeck_chart(chart)
        st.caption(
            "🔴 Asemat · pisteet = yritykset (hajautettu ~±180 m kadun ympärille näkyvyyden vuoksi) "
            "· vihreä→punainen = matalat→korkeat liidipisteet"
        )

# ---- Train reach (isochrone) -------------------------------------------
with tab_reach:
    st.subheader(f"🚆 Kuinka kauas pääset asemalta {origin_station}")
    st.caption(
        f"Aikabudjetin sisällä juna­matkalla saavutettavat asemat ({ACCESS_MODE_LABELS[access_mode]} "
        "viime kilometrillä). Varjostettu alue on karkea arvio saavutettavuudesta — "
        "offline-malli ilman aikatauluja. Varjennut näyttävät parhaan mahdollisen "
        "vaihtoyhteyden; harmaa yrityspiste on sellainen, jonka ruuhka-aamun "
        "vaihto vie budjetin yli."
    )
    reach_budget = st.slider(
        "Aikabudjetti (min)", min_value=15, max_value=120, value=60, step=5, key="reach_budget"
    )
    reach = transit.reachable_stations(
        float(reach_budget), origin=origin_station, mode=access_mode
    )
    if not reach:
        st.info("Ei saavutettavia asemia tällä budjetilla.")
    else:
        o_lat, o_lon = transit.STATION_COORDS[origin_station]
        max_rail = max((r.rail_minutes for r in reach), default=1.0) or 1.0
        disk_rows = []
        for r in reach:
            t = min(1.0, r.rail_minutes / max_rail)
            disk_rows.append(
                {
                    "name": r.name,
                    "lat": r.lat,
                    "lon": r.lon,
                    "rail_minutes": round(r.rail_minutes, 1),
                    "reach_km": round(r.reach_km, 2),
                    "radius_m": r.reach_km * 1000.0,
                    "color": [int(255 * t), int(180 * (1 - t)) + 40, 60, 70],
                }
            )
        disk_df = pd.DataFrame(disk_rows)

        # Companies shaded by whether they fall inside the budget (rush estimate:
        # a bad morning connection must not count as reaching the budget).
        comp = df.dropna(subset=["lat", "lon"]).copy()
        comp["in_reach"] = comp["commute_min_rush"].notna() & (comp["commute_min_rush"] <= reach_budget)
        comp["pt_color"] = comp["in_reach"].map(
            lambda ok: [0, 170, 90, 170] if ok else [150, 150, 150, 70]
        )

        disk_layer = pdk.Layer(
            "ScatterplotLayer",
            data=disk_df,
            get_position=["lon", "lat"],
            get_radius="radius_m",
            get_fill_color="color",
            stroked=True,
            get_line_color=[80, 80, 80, 120],
            line_width_min_pixels=1,
            pickable=True,
        )
        company_layer = pdk.Layer(
            "ScatterplotLayer",
            data=comp[["lat", "lon", "name", "pt_color"]],
            get_position=["lon", "lat"],
            get_fill_color="pt_color",
            get_radius=70,
            pickable=True,
        )
        origin_layer = pdk.Layer(
            "ScatterplotLayer",
            data=pd.DataFrame([{"name": origin_station, "lat": o_lat, "lon": o_lon}]),
            get_position=["lon", "lat"],
            get_fill_color=[0, 90, 255, 230],
            get_radius=600,
            pickable=True,
        )
        reach_view = pdk.ViewState(latitude=o_lat, longitude=o_lon, zoom=8, pitch=0)
        st.pydeck_chart(
            pdk.Deck(
                layers=[disk_layer, company_layer, origin_layer],
                initial_view_state=reach_view,
                tooltip={"text": "{name}"},
                map_style=CARTO_BASEMAP,
            )
        )
        in_reach_n = int(comp["in_reach"].sum())
        st.caption(
            f"🔵 Lähtöasema {origin_station} · 🟢 {in_reach_n} yritystä budjetin sisällä "
            f"({reach_budget} min) · {len(reach)} asemaa saavutettavissa junalla."
        )
        st.dataframe(
            disk_df[["name", "rail_minutes", "reach_km"]].rename(
                columns={"name": "Asema", "rail_minutes": "Junamatka (min)", "reach_km": "Viime km"}
            ),
            width="stretch",
            hide_index=True,
        )

# ---- Table --------------------------------------------------------------
with tab_table:
    # Name/address search runs over ALL companies (ignores the sidebar filters)
    # so a company is always findable; with no search the table reflects the
    # current filters as usual.
    search = st.text_input(
        "🔎 Hae yrityksen nimellä tai osoitteella (hakee kaikista yrityksistä)",
        "",
        key="search",
    )
    if search:
        base = df_raw.copy()
        mask = base["name"].astype(str).str.contains(search, case=False, na=False) | base[
            "address"
        ].astype(str).str.contains(search, case=False, na=False)
        source = base[mask].copy()
        # Always add the commute columns (df_raw lacks them), even for an empty
        # result set, so the column selection below never KeyErrors. Last mile
        # uses road distance when available. Both connection qualities are
        # computed so the table can show the range.
        source["commute_min"] = [
            transit.commute_minutes(
                s, d, mode=access_mode, rail_minutes=_rail, overhead_min=overhead_min
            )
            for s, d in zip(source["nearest_station"], source["dist_km"])
        ]
        source["commute_min_rush"] = [
            transit.commute_minutes(
                s, d, mode=access_mode, rail_minutes=_rail_rush, overhead_min=overhead_min
            )
            for s, d in zip(source["nearest_station"], source["dist_km"])
        ]
        if source.empty:
            st.warning("Ei hakutuloksia.")
        else:
            outside = int((~source["business_id"].isin(df["business_id"])).sum())
            if outside:
                st.info(
                    f"Haku kattaa kaikki yritykset — {outside}/{len(source)} osumaa on "
                    "nykyisten suodattimien ulkopuolella."
                )
    else:
        source = df

    extra_cols = [
        c
        for c in ["website_status", "llm_description", "llm_has_shop", "llm_is_hiring"]
        if c in source.columns
    ]
    display = source[
        [
            "name",
            "industry",
            "toi_description",
            "nearest_station",
            "dist_km",
            "address",
            "best_website",
            "registered",
        ]
        + extra_cols
    ].copy()
    # Range display: "58 min (nopein 36)" — the commute the user would usually
    # make, with the timetable-best floor alongside.
    display.insert(5, "commute_text", [
        _commute_display(b, r)
        for b, r in zip(source["commute_min"], source["commute_min_rush"])
    ])
    base_cols = [
        "Yritys",
        "Toimiala",
        "TOI-kuvaus",
        "Asema",
        "Etäisyys (km)",
        "Työmatka",
        "Osoite",
        "Verkkosivut",
        "Perustettu",
    ]
    rename_map = {
        "website_status": "Sivun tila",
        "llm_description": "AI-kuvaus",
        "llm_has_shop": "Verkkokauppa",
        "llm_is_hiring": "Rekrytoi",
    }
    display.columns = base_cols + [rename_map[c] for c in extra_cols]
    display["Toimiala"] = display["Toimiala"].map(lambda x: INDUSTRY_LABELS.get(x, x))
    display["Perustettu"] = display["Perustettu"].astype("Int64")

    st.dataframe(
        display.reset_index(drop=True),
        width="stretch",
        column_config={
            "Verkkosivut": st.column_config.LinkColumn("Verkkosivut"),
            "Etäisyys (km)": st.column_config.NumberColumn(format="%.2f km"),
        },
        height=500,
    )

    # Download
    csv_bytes = display.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button(
        "⬇️ Lataa CSV",
        data=csv_bytes,
        file_name="tyonantajat.csv",
        mime="text/csv",
    )

# ---- Webshops tab (catalog-heavy shops found by find_webshops.py) ---------
with tab_shops:
    st.subheader("🛒 Verkkokaupat — kenellä on iso tuotekatalogi verkossa?")
    st.caption(
        "Jokaisen elossa olevan vähittäis- ja tukkukaupan sivusto on kuitattu: "
        "verkkokauppa-alusta, ostoskorisignaali ja tuote-URL:ien määrä sivukartasta "
        "(scripts/find_webshops.py). Arvio katalogin koosta = tuotesivujen määrä, "
        "ei liikevaihtoa — pieni boutique ja Bygghemma-tyylinen katalogikauppa "
        "erottuvat nyt toisistaan."
    )
    if not WEBSHOPS_PATH.exists():
        st.info(
            "out/webshops.csv puuttuu — aja `python scripts/find_webshops.py` "
            "paikallisesti päivittääksesi verkkokauppalistan."
        )
    else:
        shops = load_webshops(WEBSHOPS_PATH.stat().st_mtime)

        col_f1, col_f2 = st.columns(2)
        with col_f1:
            min_products = st.slider(
                "Vähintään tuotteita (arvio)", 0, 5000, 20, step=10,
                key="shop_min_products",
                help="Alaraja katalogin koolle. 0 = myös pienet verkkokaupat.",
            )
        with col_f2:
            segment = st.radio(
                "Segmentti",
                ("Kaikki", "Vähittäiskauppa", "Tukkukauppa"),
                horizontal=True,
                key="shop_segment",
            )

        shop_station_options = sorted(shops["nearest_station"].dropna().unique())
        col_f3, col_f4 = st.columns(2)
        with col_f3:
            shop_stations = st.multiselect(
                "Asemat",
                options=shop_station_options,
                default=shop_station_options,
                format_func=lambda s: STATION_INFO.get(s, {"label": s})["label"],
                key="shop_stations",
            )
        with col_f4:
            one_per_site = st.toggle(
                "Yksi kauppa per verkkotunnus",
                value=True,
                key="shop_dedupe",
                help="Ketjut (esim. monta K-Rauta-yhtiötä samalla sivustolla) "
                     "näkyvät vain kerran. Poista päältä nähdäksesi kaikki.",
            )

        sel = shops[shops["shop_catalog_estimate"] >= min_products].copy()
        if segment == "Vähittäiskauppa":
            sel = sel[sel["toi_code"].astype(str).str.startswith("47", na=False)]
        elif segment == "Tukkukauppa":
            sel = sel[sel["toi_code"].astype(str).str.startswith("46", na=False)]
        if shop_stations:
            sel = sel[sel["nearest_station"].isin(shop_stations)]

        # Sort once: catalog first, score as tiebreak — then optionally keep only
        # the biggest shop per host (chains share one webstore).
        sel = sel.sort_values(
            ["shop_catalog_estimate", "shop_score"], ascending=False
        )
        if one_per_site and "_host" in sel.columns:
            sel = sel.drop_duplicates(subset="_host", keep="first")

        m1, m2, m3 = st.columns(3)
        m1.metric("Verkkokaupat", f"{len(sel)} kpl")
        if sel.empty:
            m2.metric("Suurin katalogi", "–")
            m3.metric("Mediaani", "–")
        else:
            m2.metric("Suurin katalogi", f"{int(sel['shop_catalog_estimate'].max()):,} tuotetta".replace(",", " "))
            m3.metric("Mediaani", f"{int(sel['shop_catalog_estimate'].median())} tuotetta")

        show_all = st.checkbox("Näytä kaikki", value=False, key="shops_show_all")
        if show_all:
            shown = sel
        else:
            n_shops = st.slider(
                "Näytä top-N", 10, 300, 50, step=10, key="shops_top_n"
            )
            shown = sel.head(n_shops)

        if shown.empty:
            st.info("Ei verkkokauppoja valituilla suodattimilla.")
        else:
            shown = shown.copy()
            # Commute range from the user's home station, same model as elsewhere.
            shown["commute_text"] = [
                _commute_display(
                    transit.commute_minutes(
                        s, d, mode=access_mode, rail_minutes=_rail,
                        overhead_min=overhead_min,
                    ),
                    transit.commute_minutes(
                        s, d, mode=access_mode, rail_minutes=_rail_rush,
                        overhead_min=overhead_min,
                    ),
                )
                for s, d in zip(shown["nearest_station"], shown["dist_km"])
            ]
            sells = shown["llm_sells"] if "llm_sells" in shown.columns else shown["toi_description"]
            shop_display = pd.DataFrame(
                {
                    "Yritys": shown["name"],
                    "Asema": shown["nearest_station"].map(
                        lambda s: STATION_INFO.get(s, {"label": s})["label"]
                    ),
                    "Työmatka": shown["commute_text"],
                    "Tuotteita": [
                        f"{int(v):,}".replace(",", " ") + ("+" if c else "")
                        for v, c in zip(
                            shown["shop_catalog_estimate"], shown["shop_sitemap_capped"]
                        )
                    ],
                    "Alusta": shown["shop_platform"].fillna("–"),
                    "Myy": sells.fillna(shown["toi_description"]),
                    "Pisteet": shown["shop_score"],
                    "Verkkosivut": shown["best_website"],
                    "Perustettu": shown["registered"].astype("Int64"),
                }
            )
            st.dataframe(
                shop_display.reset_index(drop=True),
                width="stretch",
                column_config={
                    "Verkkosivut": st.column_config.LinkColumn("Verkkosivut"),
                    "Pisteet": st.column_config.ProgressColumn(
                        "Pisteet", min_value=0, max_value=9, format="%d"
                    ),
                },
                height=500,
            )
            csv_shops = sel.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button(
                "⬇️ Lataa verkkokaupat CSV (koko suodatettu lista)",
                data=csv_shops,
                file_name="verkkokaupat.csv",
                mime="text/csv",
            )
            st.caption(
                f"Näytetään {len(shown)}/{len(sel)} — lista vie suodattimet "
                "mukaan, ja ‘Näytä kaikki’ tuo koko listan ruudulle."
            )

# ---- Leads tab (scoring helpers are defined near the top of the file) ----
with tab_leads:
    st.subheader("🎯 Liidipisteytys — keihin kannattaa ottaa ensin yhteyttä?")
    st.caption(
        "Pisteet perustuvat TOI-toimialakoodiin, yrityksen ikään (vanha = päivitystarve) "
        "ja verkkosivujen olemassaoloon (digitaalisesti valmis). "
        "Valitse kategoria joka kiinnostaa sinua eniten."
    )

    scored = df.copy()  # lead scores (s_webshop/s_pim/s_data) already computed above

    lead_axis = st.radio(
        "Palvelutyyppi",
        options=["Verkkokauppa / webshop", "PIM / tuotekataloogi", "Data-analyysi"],
        horizontal=True,
        key="lead_axis",
    )
    axis_col = {
        "Verkkokauppa / webshop": "s_webshop",
        "PIM / tuotekataloogi": "s_pim",
        "Data-analyysi": "s_data",
    }[lead_axis]

    show_all = st.checkbox(
        "Näytä kaikki",
        value=False,
        help="Näyttää ja lataa koko pisteytetyn listan, ei vain top-N.",
        key="leads_show_all",
    )
    if show_all:
        top = scored.sort_values([axis_col, "commute_min_rush"], ascending=[False, True])
    else:
        n_leads = st.slider("Näytä top-N", 5, 500, 30, step=5, key="leads_top_n")
        top = scored.sort_values([axis_col, "commute_min_rush"], ascending=[False, True]).head(
            n_leads
        )

    lead_display = top[
        [
            "name",
            "toi_description",
            "nearest_station",
            "dist_km",
            "commute_min",
            "commute_min_rush",
            "best_website",
            "registered",
            axis_col,
        ]
        + [
            c
            for c in ["llm_description", "llm_has_shop", "llm_is_hiring"]
            if c in top.columns
        ]
    ].copy()
    lead_display["commute_min"] = [
        _commute_display(b, r)
        for b, r in zip(lead_display["commute_min"], lead_display["commute_min_rush"])
    ]
    lead_display = lead_display.drop(columns=["commute_min_rush"])
    base_lead_cols = [
        "Yritys",
        "Toimiala (tarkennettu)",
        "Asema",
        "Etäisyys (km)",
        "Työmatka",
        "Verkkosivut",
        "Perustettu",
        "Pisteet",
    ]
    llm_lead_rename = {
        "llm_description": "AI-kuvaus",
        "llm_has_shop": "Verkkokauppa",
        "llm_is_hiring": "Rekrytoi",
    }
    lead_display.columns = base_lead_cols + [
        llm_lead_rename[c] for c in llm_lead_rename if c in top.columns
    ]
    lead_display["Perustettu"] = pd.to_numeric(
        lead_display["Perustettu"], errors="coerce"
    ).astype("Int64")

    st.dataframe(
        lead_display.reset_index(drop=True),
        width="stretch",
        column_config={
            "Verkkosivut": st.column_config.LinkColumn("Verkkosivut"),
            "Etäisyys (km)": st.column_config.NumberColumn(format="%.2f km"),
            "Pisteet": st.column_config.ProgressColumn(
                "Pisteet", min_value=0, max_value=8
            ),
        },
        height=420,
    )

    st.caption(f"{len(lead_display)} liidiä näkyvissä ja CSV-latauksessa.")

    csv_leads = lead_display.to_csv(index=False, encoding="utf-8-sig").encode(
        "utf-8-sig"
    )
    st.download_button(
        "⬇️ Lataa liidit CSV", data=csv_leads, file_name="liidit.csv", mime="text/csv"
    )

# ---- Employer fit tab (rule scores + feedback learning) --------------------
with tab_fit:
    st.subheader("💼 Työnantajasopivuus — sopiko tämä yritys työnantajaksi sinulle?")
    st.caption(
        "Pisteet 0–10 = alan sopivuus (TOI-koodi, 0–4) + rekrytointi (AI havainnut "
        "rekrytoinnin 0–3, muuten toimiva sivusto 0–1) + työmatka valitsemaltasi "
        "asemalta (0–3). Merkitse kiinnostavat ja ei-kiinnostavat yritykset "
        "Palautte-sarakkeeseen — valinnoistasi koulutetaan myöhemmin "
        "koneoppimismalli, joka oppii mieleisesi työnantajat."
    )

    fit_frame = df.copy()

    # M7 (train_fit_model.py) kirjoittaa fit_prob-sarakkeen. Siihen asti
    # järjestys perustuu sääntöpisteisiin e_fit.
    if "fit_prob" in fit_frame.columns:
        score_col = "fit_prob"
        st.caption("Järjestys: koneoppimispisteet (fit_prob).")
    else:
        score_col = "e_fit"
        st.info(
            "🤖 Koneoppimispisteitä ei ole vielä koulutettu. Merkitse yrityksille "
            "palautetta — 30 merkinnän jälkeen pisteet alkavat oppia valinnoistasi. "
            "Siihen asti järjestys perustuu sääntöpisteisiin (e_fit)."
        )

    show_all_fit = st.checkbox("Näytä kaikki", value=False, key="fit_show_all")
    if show_all_fit:
        top_fit = fit_frame.sort_values(
            [score_col, "commute_min_rush"], ascending=[False, True], na_position="last"
        )
    else:
        n_fit = st.slider("Näytä top-N", 5, 500, 30, step=5, key="fit_top_n")
        top_fit = fit_frame.sort_values(
            [score_col, "commute_min_rush"], ascending=[False, True], na_position="last"
        ).head(n_fit)

    # Aiemmat palautemerkit (paikallinen sqlite) -> Palaute-sarakkeen alkuarvot.
    try:
        _conn = feedback.connect()
        known = feedback.load(_conn)
        _conn.close()
    except Exception:
        known = pd.DataFrame(columns=["business_id", "label"])
    labels_by_id = (
        dict(zip(known["business_id"].astype(str), known["label"]))
        if not known.empty
        else {}
    )

    fit_cols = [
        "business_id", "name", "toi_description", "nearest_station",
        "dist_km", "commute_min", "commute_min_rush", "best_website", "registered",
        "e_industry", "e_hiring", "e_commute", score_col,
    ]
    fit_display = top_fit[fit_cols].copy()
    fit_display["Palaute"] = (
        fit_display["business_id"].astype(str).map(labels_by_id).fillna("")
    )
    # Range display replaces the numeric minute column: "58 min (nopein 36)".
    fit_display["commute_min"] = [
        _commute_display(b, r)
        for b, r in zip(fit_display["commute_min"], fit_display["commute_min_rush"])
    ]
    fit_display = fit_display.drop(columns=["commute_min_rush"])

    _score_config = (
        st.column_config.ProgressColumn(
            "Sopivuus", min_value=0, max_value=EMPLOYER_SCORE_MAX, format="%d/10"
        )
        if score_col == "e_fit"
        else st.column_config.NumberColumn("Sopivuus (ML)", format="%.3f")
    )
    edited = st.data_editor(
        fit_display.reset_index(drop=True),
        width="stretch",
        column_config={
            "business_id": "Y-tunnus",
            "name": "Yritys",
            "toi_description": "Toimiala (tarkennettu)",
            "nearest_station": "Asema",
            "dist_km": st.column_config.NumberColumn("Etäisyys (km)", format="%.2f km"),
            "commute_min": "Työmatka",
            "best_website": st.column_config.LinkColumn("Verkkosivut"),
            "registered": "Perustettu",
            "e_industry": st.column_config.NumberColumn("Alan pisteet", format="%.0f"),
            "e_hiring": st.column_config.NumberColumn("Rekrypisteet", format="%.0f"),
            "e_commute": st.column_config.NumberColumn("Matkapisteet", format="%.0f"),
            score_col: _score_config,
            "Palaute": st.column_config.SelectboxColumn(
                "Palaute", options=["", *feedback.LABELS]
            ),
        },
        disabled=[c for c in fit_cols if c != "Palaute"],
        height=420,
        key="fit_editor",
    )

    if st.button("💾 Tallenna palaute"):
        saved = 0
        _conn = feedback.connect()
        try:
            for _, row in edited.iterrows():
                bid = str(row["business_id"])
                new_label = str(row["Palaute"] or "").strip()
                if new_label == labels_by_id.get(bid, ""):
                    continue  # ei muutosta
                if new_label == "":
                    continue  # tyhjä = ei merkintää
                feedback.record(_conn, bid, new_label)
                saved += 1
        finally:
            _conn.close()
        if saved:
            st.success(f"Tallennettu {saved} uutta merkintää.")
            st.rerun()  # päivittää Palaute-sarakkeen alkuarvot
        else:
            st.info("Ei muuttuneita merkintöjä tallennettavaksi.")

    st.caption(f"{len(edited)} yritystä näkyvissä ja CSV-latauksessa.")

    csv_fit = edited.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button(
        "⬇️ Lataa työnantajasopivuus CSV",
        data=csv_fit,
        file_name="tyonantajasopivuus.csv",
        mime="text/csv",
    )
    st.caption(
        "💾 Palaute tallentuu paikalliseen data/feedback.sqlite-tiedostoon — "
        "pilvikopio sovelluksesta on vain luettavissa."
    )

# ---- Stats --------------------------------------------------------------
with tab_stats:
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Toimialajakauma")
        industry_counts = (
            df["industry"]
            .map(lambda x: INDUSTRY_LABELS.get(x, x))
            .value_counts()
            .reset_index()
        )
        industry_counts.columns = ["Toimiala", "Yrityksiä"]
        st.bar_chart(industry_counts.set_index("Toimiala"))

    with col_b:
        st.subheader("Yrityksiä alueittain")
        area_counts = df["nearest_station"].value_counts().reset_index()
        area_counts.columns = ["Alue", "Yrityksiä"]
        area_counts["Alue"] = area_counts["Alue"].map(
            lambda k: STATION_INFO.get(k, {}).get("label", k)
        )
        st.bar_chart(area_counts.set_index("Alue"))

    st.subheader("Rekisteröintivuosi")
    reg_counts = df["registered"].value_counts().sort_index()
    st.bar_chart(reg_counts)
