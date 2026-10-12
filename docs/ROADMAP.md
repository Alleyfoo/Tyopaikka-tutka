# Roadmap — what to do in next sessions

Written 2026-10-12, after the 7-milestone upgrade plan was completed
(`f31c345` → `a9146c9`, all pushed). Current state: 8 stations,
30,698 companies, 7,373 live websites, 401 Tier-A employers, feedback
store seeded with 10 `kiinnostaa` labels (IT-support shortlist).

`docs/TOMORROW_PLAN.md` is historical (pre-upgrade Places workflow) —
this file replaces it.

Tracks are ordered by payoff for the actual job search, not by effort.

---

## Track 1 — Close the ML feedback loop (M7 activation)

The LightGBM sorter is built but dormant behind the cold-start gate.

- **1.1 Reach the gate**: mark feedback in the dashboard until
  ≥30 labels with ≥8 `ei_kiinnosta` and ≥8 positives (10 already in).
  Fastest path: mark ~10 obviously-irrelevant top-list companies as
  `ei_kiinnosta`, another ~10 as `kiinnostaa`/`hakenee`.
  Then run `python scripts/train_fit_model.py` → `fit_prob` lands in
  companies.csv and the Työnantajasopivuus tab automatically sorts by it.
- **1.2 Re-train cadence**: no code needed — the script is idempotent;
  re-run it after each marking session. Optionally add a
  `--min-new-labels N` guard later so re-training can be scripted.
- **1.3 Rule vs model check**: once trained, compare LightGBM feature
  importances against the hand-written `e_fit` weights
  (`employer_fit.py` `INDUSTRY_FIT`, commute bands). If the model
  disagrees strongly, tune the rules — they keep ranking the cold start
  of any new area.

## Track 2 — Job-watch automation (the daily payoff)

The scanner finds employers; it does not yet watch them for openings.

- **2.1 Careers-page column**: during job-signal probing we discover
  careers hrefs (e.g. mysteerio.fi/tyopaikat) but never persist them.
  Add an additive `careers_url` column to companies.csv for the
  kiinnostaa/hakenee pool.
- **2.2 `scripts/watchlist.py`**: re-fetch the watchlist companies'
  homepages + careers pages on demand, diff against the last snapshot,
  and write `out/watchlist_alerts.csv` (new job signal / changed
  openings). Small scope: only the ~10–50 labeled companies, so it is
  fast and can run weekly.
- **2.3 Weekly schedule**: Windows Task Scheduler entry that runs
  website re-probe → employer_targets rebuild → watchlist → (optionally)
  LLM analysis of new Tier-A rows. Everything is cached and idempotent,
  so a weekly task is safe.

## Track 3 — Coverage expansion on existing data

- **3.1 Analyze the rest of Tier A**: 401 Tier-A companies exist, only the
  top 100 were LLM-analyzed (96 succeeded). Extend `analyze_websites.py`
  runs to the remaining ~300 — with `analysis_v2` caching this resumes
  cleanly and costs nothing for already-analyzed rows.
- **3.2 Deep-check Tier B**: re-run `employer_targets.py --deep N` to
  homepage-probe the top N Tier-B rows for hiring signals; promotions
  to Tier A get analyzed next.
- **3.3 Website health freshness**: `website_health.py` only probes rows
  without a status. Add `--reprobe-days N` to re-probe stale results so
  dead/parked sites drop out of the live pool over time.

## Track 4 — Dashboard & UX

- **4.1 Remove the obsolete "Train reach" page** from `curate_app.py`
  (tiny commit — do only after user confirms, per plan note).
- **4.2 "Hakemukset" view**: a filter over `hakenee`-labeled companies
  with application date + free-text status note, so applied places are
  tracked in one place. The `hakenee` label already exists; this is
  presentation.
- **4.3 IT-support preset**: one-click saved filter in the
  Työnantajasopivuus tab (industry = IT + support keywords
  it-tuki/atk/palvelin/hosting/helpdesk/tietoturva) reproducing the
  shortlist search from 2026-10-12 without retyping.
- **4.4 Explain the score**: once `fit_prob` exists, show the top
  contributing features per company in the tab (LightGBM `predict`
  contribution or simply the e_* axis values), so a high score is
  not a black box.

## Track 5 — Maintenance & experiments

- **5.1 pandas FutureWarnings** at `curation.py:234` — cosmetic today,
  will break on a future pandas; 30-minute fix with a test.
- **5.2 UTF-8 guard**: patch `geocode_streets.py` and
  `road_distances.py` with the `sys.stdout.reconfigure` pattern from
  `enrich_google.py` (they currently rely on `PYTHONUTF8=1`).
- **5.3 `discover_websites.py` ddgs→client rename WIP**: finish or
  explicitly shelve; it blocks `git add -A` usage forever.
- **5.4 Model comparison**: the `analysis_v2` cache is model-aware, so
  running `analyze_websites.py --model <newer tag>` on the same Tier-A
  top 100 re-analyzes without touching old results — an honest
  side-by-side quality comparison for DEFAULT_MODEL.

---

## Suggested session order

1. **Session A**: 1.1 (mark labels + train) + 4.1 cleanup → ML loop live.
2. **Session B**: 2.1 + 2.2 (careers column + watchlist) → the scanner
   starts telling you when a target company posts an opening.
3. **Session C**: 3.1 + 3.2 (coverage) — more Tier A analyzed, hidden
   hiring signals surfaced.
4. **Session D**: 2.3 weekly automation + 4.2/4.3/4.4 UX.
5. Track 5 items opportunistically.