# Segmentation reference

Part of the **serve-analytics-knowledge** skill. What Serve analyses can and cannot slice by
today. Thin by design — Serve segmentation is largely **pending officeholder data ingestion**
into civics (noted in `users_serve_activity`'s model comments); office_type / level / state
cuts will land there.

## Available now (on `users_serve_base`, no join)

| Dimension | Column(s) | Notes |
|---|---|---|
| Onboarding funnel stage | `serve_onboarding_steps_completed` (0–7), plus per-step booleans (`has_started_serve_onboarding` … `has_completed_serve_onboarding`) | The 7 steps: getting started → constituency profile → value props → strategy → image → preview → first SMS poll |
| Behavioral activation | `is_active_serve_user`, `has_pledged`, `has_sent_sms_poll` | `is_active_serve_user` = poll AND pledge (the People Served behavioral half) |
| Poll-anchored recency | `is_active_eo` | Continuity-only; see [methodology_defaults.md](methodology_defaults.md) |
| Registration cohort | `registered_at`, `registration_month` / `_quarter` / `_year` | Registrations reach back to 2020 for backfilled EOs — early cohorts predate the product |
| Activation cohort | `eo_activated_at` | The natural Serve cohort anchor; prefer it over registration for engagement analyses |

## Not available yet

- **Office type / level / state / district** — pending officeholder ingestion into civics.
  Do not promise these cuts in a brief without checking whether the ingestion has landed.
- **`pledged`-style all-product fields** — pledges on `users_serve_base` count ALL products
  (39,052 users); the Serve-relevant read is pledge AND `is_serve_user`. See
  [gotchas.md](gotchas.md).

## Default slice dimensions for the working set

`build_serve_working_set` in `analytics/lib/serve_analysis.py` carries
`serve_onboarding_steps_completed`, `is_active_serve_user`, `has_pledged`, and
`registration_year` by default so re-cuts are free. Extend per analysis via its
`slice_dims` argument.
