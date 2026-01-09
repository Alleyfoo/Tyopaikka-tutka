# Milestone 0.8.0: Dossier UX + Website Resolution Fallback

## Goal
Given only a Google Maps share URL (or a pasted website), produce a dossier that is:
- conservative
- evidence-backed
- resilient when the website is missing

## Deliverables
### Resolver ladder (explicit + provenance-tagged)
Deterministic resolution pipeline in this order:
1) User-provided `website_url` (highest trust).
2) Places API `websiteUri` (server-side only, ephemeral; do not persist nonessential fields).
3) Optional constrained inference (gated, clearly labeled).

Provenance fields:
- `website_source`: `user` | `places` | `inferred_search` | `unknown`
- `resolver_notes`: e.g. "Places had no websiteUri; user did not provide website; inference disabled"

If `website_source == inferred_search`:
- cap hiring.status at `maybe` unless strong first-party evidence exists.

### Dossier UX polish (Markdown template v1)
Make `company_package.md` scan-friendly:
- Header: domain + decision + confidence
- Why (max 5 bullets)
- Hiring evidence (grouped links)
- Industry/field evidence
- Roles detected + confidence
- Unknowns & caveats (cookie wall, robots skip, JS-only, no careers page)
- Provenance footer (run_id, version, timestamp)

### Failure modes are first-class
If no website can be resolved:
- status = `uncertain`
- include explicit next action:
  - "No official website found. Paste website URL to proceed."

## Tests
- Resolver unit tests (user, places, inferred_search, unknown).
- Regression: evidence = 1 URL => downgrade from `yes`.
- Small-company fixture: one careers page + one ATS listing (should pass).

## Nice-to-have (0.8.1)
- Inbox handoff (download request JSON + watch-inbox) OR MV3 extension URL-only.
