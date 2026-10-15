"""Assemble and headlessly execute notebooks/01_mvp.ipynb for DATA-2225 (rev 2, post-review)."""

from pathlib import Path

import nbformat as nbf
from nbclient import NotebookClient

HERE = Path(__file__).resolve().parent
NB_DIR = HERE / "notebooks"
NB_DIR.mkdir(exist_ok=True)

md = nbf.v4.new_markdown_cell
code = nbf.v4.new_code_cell

VARDICT = r"""## Variable dictionary and sources (rev 2026-08-05)

**available** = earliest point the value could be known for a new signup: `signup` / `deadline` (the race's
general filing deadline) / `election-1w` / `post-hoc` (only knowable from today's snapshot — needs PIT
hardening before forward use). An asterisk (*) means the value is knowable at signup in principle but our
pipeline reads a mutable current-state snapshot with no as-of history — usable now, harden later.

### Pass B features (the analysis filter)

| variable | what it is | source | available | PIT / leakage note |
|---|---|---|---|---|
| campaign_state | state, user-declared at signup | `users_win_candidacy` (product DB) | signup | immutable declaration; also proxies BR label coverage (in ablation drop list) |
| election_level | level of the declared office | `users_win_candidacy` (civics enrichment) | signup* | derived off the declared position; current-state read; 35% missing |
| office_type | office category of the declared position | `users_win_candidacy` ← `mart_civics` | signup* | civics-side classification, current-state read |
| br_normalized_position_type | BR's normalized type of the declared position | lookup off `ballotready_position_id` | signup* | deterministic lookup off signup-declared id; current snapshot. **Its `missing` bucket (12% of pool) is near-tautological**: BR has no position record, so it can't list a candidacy either — 1.2% corroborated, decided by rule not model (see decidable-population section) |
| campaign_party | party, user-declared | `users_win_candidacy` (product DB) | signup* | declared at signup but mutable; ~25% missing in-cycle |
| email_domain_class | goodparty / free / custom / missing | `user_email`, domain only | signup | immutable; ~zero signal |
| name_* (5 flags) | token count, length, digits, non-alpha, single-char | `user_first/last_name` | signup | immutable; ~zero signal |
| days_signup_to_election | declared election date − signup date | `user_created_at` + declared election date | signup | arithmetic on signup facts |
| days_signup_to_deadline, has_deadline, signed_up_after_deadline | signup vs the race's general filing deadline | BR raw: `race.filing_periods` → `int__ballotready_filing_period.end_on` | signup, where BR carries the race's filing period (deadline dates publish in advance; ~50-72% of races by cycle) | race-level fixed fact; missing for 41% of the pool |
| log_voter_count, voter_count_missing | L2 registered-voter count of the district | `users_win_candidacy.voter_count` | signup* | deterministic district lookup; current snapshot |
| n_br_by_deadline, any_br_by_deadline, br_arrivals_missing | of the race's eventual BR candidacies, how many were created in BR by the filing deadline (i.e. had BR started covering the race) | `..._ballotready_s3_candidacies_v3.candidacy_created_at` | **deadline** (partial counts earlier; keeps growing after; 81% missing in-cycle today) | **closest to the label**: counts the user's own BR record when it landed by deadline (v2: opponents only), and trusts BR's created_at as a faithful arrival time (unverified) |
| zip_campaign_state_match | stated zip's state == declared campaign state | product `user_zip` + offline zip lookup (`build_geo.py`) | signup | pure product-data consistency; 98.9% match where known, so the rare mismatch is the signal |
| geo_state_match, geo_zip_state_match, geo_country_nonus, geo_missing | near-signup login location (Amplitude IP-geo region/country) vs campaign state / zip state / US | `stg_..._amplitude_api_events` (`build_geo.py`) | signup: earliest geo-bearing event within signup−1d..+30d | geo is IP-derived, so server-emitted events carry none — and **~45% of post-2025-04 office-declared signups have ONLY server-side events in month 1** (no browser presence near signup; likely CRM/sales-created accounts). That, plus the geo columns existing only from 2025-04-24, gives 19.8% historical coverage (39.9% of post-era pool). `location_lat/lng` are never populated, so distance-km is NOT buildable (corrected 2026-08-06) |

### Pass A product-behavior variables (diagnostic only — never the filter)

Implemented as LIFETIME aggregates (post-hoc); the "available" column says what a PIT rebuild could anchor.

| variable | what it is | source | available | PIT note |
|---|---|---|---|---|
| log_campaigns_sent, log_recipients | outreach volume | `users_win_base` | reconstructable as-of any anchor (Amplitude timestamps) | implemented lifetime; rebuildable from event times |
| log_dash_views, viewed_dashboard | dashboard engagement | `users_win_base` / milestones | reconstructable as-of anchor | same |
| act_is_activated | sent ≥1 outreach | `users_win_base.is_activated` | reconstructable (first_campaign_sent_at) | implemented lifetime |
| act_is_pro | Pro flag today | `users_win_base.is_pro` | **post-hoc** | no product-DB first-Pro timestamp; ~54% of Pros undatable by any channel (DATA-2185) — not PIT-reconstructable for about half |
| ever_pro_upgrade | Amplitude pro-upgrade milestone | milestones `pro_upgrade_completed_at` | reconstructable where captured | server-side event; partial coverage |
| act_pledged | has a pledged campaign | `users_win_base` | partially reconstructable (pledge events exist in Amplitude, era-resolved) | flag itself is current-state |
| act_verified | has a verified campaign | `users_win_base` | **post-hoc** (no verification timestamp) | strongest behavioral signal (0.053) but **EXCLUDED from the v1 pass-A headline** per 2026-08-05 decision — not knowable up front |
| campaign_count, active_campaign_count | campaigns created / currently active | `users_win_base` | created: reconstructable; active: post-hoc | mixed |

**Slice dimensions only, never features:** `icp_office_win`, `is_pro`, `cycle_year`.
**Label:** corroborated = BR/TS/DDHQ match on `mart_civics.candidacy_stage` rolled up to `gp_candidacy_id`, completed elections only.

**Sources used (6):** `mart_analytics.users_win_candidacy`; `mart_civics.candidacy` + `candidacy_stage`;
BR raw race/election + `int__ballotready_filing_period`; `..._ballotready_s3_candidacies_v3`;
`mart_analytics.users_win_base` + `dbt.int__amplitude_user_milestones` (pass A).

**Available but not yet joined:** L2 district demographics beyond voter_count (`int__l2_*`); product
signup answers in gp_api staging (on-ballot self-report, running stage — Airbyte's insert-only history
can reconstruct true as-of-signup values); incumbency (DATA-2213); TS viability inputs (excluded pending
a PIT deep dive); web-research enrichment (website/FB/Twitter). Amplitude geo is now joined via
`build_geo.py` (2026-08-05); the product DB itself has no IP column."""

FINDINGS = r"""## Findings

1. **Promising.** Signup-time features alone (pass B, no product activity) reach **0.875 ROC-AUC / 0.888
PR-AUC** out-of-fold against a 59.4% base rate. With every BR-coverage-proxy feature removed the model
still holds **0.842 AUC** — both numbers belong in the readout: the first says how well we can predict
corroboration, the second says how much candidate-level signal exists beyond "which races BallotReady
lists." Example operating points: keep 92% of real candidates at 80% precision, or 78% at 85% precision
(all precision figures are lower bounds). **Decidable-population view (independent validation,
2026-08-05):** 12% of the pool has no BR position record for its declared office, which near-mechanically
forces label=0 (1.2% corroborated) — a rule decides those rows, not a model. Restricted to the 88% where
a judgment call exists (base rate 67.3%), HGB holds 0.826 and LR 0.800. Quote consumer operating points
from the decidable-population frontier; recommended serving shape is two-stage (rule-route the missing
bucket as its own segment, model the rest).
2. **Population accounting.** Of ~60.6k non-demo Win signups, ~34% (20.6k) never declare an office —
trivially unverifiable, no model needed, reported as its own segment. The score applies to the
office-declared flow: 20,479 labeled users (2025+), 10,527 in-cycle signups awaiting scores. The framing
pool (21,210 by loose campaign join) reconciles to the modeled 20,479 via the version-aware join
(731 users whose campaign election date mismatches the candidacy's general date).
3. **No single-marker rule competes.** The strongest fair rule (has a recoverable filing deadline)
reaches 68% precision at 68% recall; the BR-arrivals rule flags only 2.4% of rows; email domain and
party are at or below the base rate. At the same 68% recall the model delivers ~87% precision — the
frontier dominates every rule point. (The originally pre-registered email+deadline heuristic was
miscoded and directionally backwards; replaced post-review.)
4. **Forward transfer holds on the data we have.** Train-2025 → test-completed-2026: 0.896 ROC-AUC /
0.913 PR-AUC.
Caveats: completed-2026 races are off-cycle, not the Nov-2026 wave, and at in-cycle scoring time the
BR-arrivals feature is right-censored (only 19% of the in-cycle set has any BR arrivals row vs most of
the retrospective pool), so early-cycle deployment behaves closer to the no-arrivals ablation (0.859)
than the full model (0.875).
5. **The score predicts corroboration, not legitimacy — and coverage varies enormously.** State base
rates run 26% (NY) to 81% (IA); state-level offices sit at 16%. Used as a hard binary filter it would
systematically drop real candidates in BR-under-covered geographies and office classes, and every
downstream analysis inherits that composition shift. Recommendation: ship as a **calibrated score with
threshold bands**, publish per-state base rates alongside, and name the eventual mart column for what it
predicts (`corroboration_score` / `p_corroborated`, not `legitimacy_score`).
6. **Calibration is honest pooled and roughly honest per-cohort.** Pooled deciles track within ~0.01;
across OH/IL/TX/NY and the missing-level bucket, mean quintile gaps are ≤0.022 and the largest single
gap is 0.045 (IL). Full per-cohort verification is a build-out item.
7. **Weak spots.** State-level offices (n=180, AUC 0.686) and NY (base rate 26%) — both read as label
coverage artifacts rather than model failures. `election_level = missing` (n=7,150, best AUC 0.928) is
likely BR-unclassified local offices, where structural features separate cleanly.
8. **Product behavior adds modest lift and is not the filter (pass A, added 2026-08-05).** The v1
pass-A headline — all behavior signals except `act_verified` (excluded: no verification timestamp, not
knowable up front) — lifts 0.875 → **0.888** ROC-AUC (79% precision at 95% recall). Including verified
gives 0.894 (diagnostic only); behavior alone reaches just 0.657. Top v1 behavioral features: dashboard views and active
campaign count. These aggregates include post-deadline behavior and are multi-causal
with the outcome, so per the leakage design pass B remains the analysis filter; pass A is the upper-bound
diagnostic.
9. **Fold construction verified (2026-08-05).** Race-grouped CV (no race straddles train/validation)
moves ROC-AUC only 0.8750 → 0.8737 — the estimate is not inflated by cross-fold race sharing.
10. **2024 stays out of training (tested 2026-08-05).** Adding 2024 (8.9k users, 45.0% positive)
degrades out-of-time performance: completed-2026 AUC 0.899 → 0.892 and PR-AUC 0.918 → 0.911 vs
training on 2025 alone, with the 2025 holdout flat. The lower 2024 positive rate reads as a
label-coverage era effect, and the model imports it as noise. Decision: train on 2025+; keep 2024
only as a robustness reference.
11. **Interpretable standalone: elastic-net LR at 0.857/0.872 OOF, also honestly calibrated.** Its
coefficients are legible (dominant noise marker: declared position never matched a BR office type,
log-odds -4.2; Statewide/Governor runs and registered minor-party labels push noise; Independent and
"Other" party push real). It is a separate candidate model: if coefficient-level auditability is a
requirement (e.g. sales/marketing), the decision is to promote LR itself at a ~2-point AUC cost —
never quote LR coefficients as explanations of HGB scores, they drift the moment the models diverge.
12. **Geo-consistency features added (2026-08-05; missingness diagnosis corrected 2026-08-06).**
Stated-zip-vs-campaign-state consistency (pure product data, 99% coverage, 98.9% match — the rare
mismatch is the signal) plus near-signup login-state matches from Amplitude IP-geo. Retrospective lift
is marginal (+0.001 AUC) because coverage is only 19.8%. The dominant cause is NOT the 2025-04-24
geo-era boundary (70% of the pool signed up after it): geo is IP-derived and server-emitted events
carry none, and **~45% of post-era office-declared signups have ONLY server-side events in their first
30 days** — no browser presence near signup (99.8% server-side event share among the geo-missing vs
4.9% among the geo-covered; consistent with CRM/sales-created accounts). That is a product insight in
its own right, and it caps this feature's forward coverage well below "near-complete" for this
population. Distance-in-km remains unbuildable: `location_lat/lng` are never populated.

### Caveats
- Precision is a **lower bound**: some "noise"-labeled signups are real candidates BR never listed.
- Retrospective only (completed elections); the Nov-2026 wave is out of sample.
- The score is calibrated to the *corroboration label*, not ground-truth legitimacy.
- Feature-list deviations from the brief, documented post-review: `campaign_party` (user-declared but
  mutable, ~25% missing in-cycle, era-dependent missingness), `br_normalized_position_type` and
  `voter_count` (deterministic lookups off the signup-declared BR position id, but current-state reads
  with no as-of-signup snapshot). None is label-side; all are flagged for PIT hardening in v2.
- `n_br_by_deadline` counts the user's own BR record when it landed by the deadline (self-inclusion);
  v2 should count opponents' arrivals only. Ablation bounds the effect (drop costs 0.017 AUC).

### Next (deferred per brief)
Feature deep-dive + interactions; 2024 label audit + backward holdout; Amplitude geo-mismatch
(product DB has no IP; Amplitude does); L2 district demographics; as-of-signup product answers via
Airbyte insert-only history; threshold bands; own-record exclusion in BR arrivals;
filing-period selection refinement (independent deadlines); candidate-grain CV grouping; per-cohort
calibration verification; forward scoring + mart promotion (dev schema first); incumbency when
DATA-2213 lands; Samuel predictor-list comparison."""

cells = [
    md(
        "# DATA-2225 MVP — legitimate candidate predictor (pass B)\n\n"
        "**Executed 2026-08-04, rev 2 (post two-reviewer pass).** Brief: "
        "`2026-08-04_legit-candidate-predictor-mvp_brief.yaml` (this directory). Ticket: "
        "[DATA-2225](https://goodparty.clickup.com/t/90132012119/DATA-2225).\n\n"
        "Can signup-time features (information cutoff = the race's general filing deadline) predict "
        "whether a Win signup is a real candidate (BR/TS/DDHQ-corroborated)? Pass B = **no product-activity "
        "features** (the ticket's leakage constraint); this is the candidate analysis filter.\n\n"
        "- Population: `users_win_candidacy` latest/non-demo, office-declared, completed generals 2025-01-01+.\n"
        "- Label: DATA-2202 corroborated cut at `gp_candidacy_id` grain. Negative labels are noisy "
        "(BR under-covers local races), so **precision numbers are lower bounds**.\n"
        "- Point-in-time allowlist enforced (brief step 3b): no results, stages, runoff, uncontested, "
        "opponent, vote, viability, or product-activity fields.\n\n"
        "**Model naming:** Model A = HistGradientBoosting (headline filter candidate); Model B = elastic-net logistic regression (interpretable candidate). Pass B = signup-time features only (the filter); pass A = pass B + product behavior (diagnostic).\n\nRev 2 adds, per the product-data-scientist and product-manager reviews: population accounting + "
        "join reconciliation, fair rule baselines (replacing a miscoded heuristic), a train-2025 → "
        "test-2026 forward split, ICP/email-domain slices, Wilson CIs on state claims, per-cohort "
        "reliability, and in-cycle feature-availability counts.\n\n"
        "*Executed locally against the cached working set (`build_working_set.py` → "
        "`working_set.parquet`); the Databricks workspace copy is for reading.*"
    ),
    code(
        "import sys\n"
        "from pathlib import Path\n"
        "import numpy as np\n"
        "import pandas as pd\n"
        "from sklearn.ensemble import HistGradientBoostingClassifier\n"
        "from sklearn.inspection import permutation_importance\n"
        "from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score\n"
        "from sklearn.model_selection import StratifiedGroupKFold\n\n"
        "PROJ = Path.cwd().parent if Path.cwd().name == 'notebooks' else Path.cwd()\n"
        "sys.path.insert(0, str(PROJ))\n"
        "from mvp_model import CAT_COLS, NUM_COLS, engineer, make_lr, make_hgb_frame, frontier, reliability\n"
        "from ablation import ABLATIONS\n\n"
        "df = engineer(pd.read_parquet(PROJ / 'datasets' / 'working_set.parquet'))\n"
        "y = df['corroborated'].astype(int)\n"
        "groups = df['user_id']\n"
        "print(f'n={len(df):,} rows ({df.user_id.nunique():,} users)   positive rate={y.mean():.3f}   '\n"
        "      f'deadline present={df.has_deadline.mean():.1%}')"
    ),
    md(
        "## Population accounting (brief step 1 + review fix)\n\n"
        "Where every latest-version Win signup lands, and how the framing pool reconciles to the modeled "
        "pool. Queries: `population_accounting.py` (run 2026-08-04)."
    ),
    code(
        "census = pd.read_csv(PROJ / 'datasets' / 'population_accounting.csv')\n"
        "display(census)\n"
        "rec = pd.read_csv(PROJ / 'datasets' / 'join_reconciliation.csv')\n"
        "display(rec)\n"
        "print('Reconciliation: the framing census (loose campaign join) counts',\n"
        '      f"{rec.n_loose_join_framing[0]:,} users; the working set requires the version-aware join",\n'
        "      '(campaign election_date = candidacy general_election_date), dropping',\n"
        '      f"{rec.n_dropped_by_election_date_match[0]:,} mismatched users -> {rec.n_version_aware_join[0]:,}.")\n'
        "inc = pd.read_csv(PROJ / 'datasets' / 'incycle_feature_availability.csv')\n"
        "display(inc)\n"
        "print('In-cycle (forward scoring set) feature availability: position id/type are complete;',\n"
        "      'the filing deadline exists for ~51%; BR-arrivals rows exist for only ~19% (right-censored',\n"
        "      'in-cycle) — early-cycle scores lean on the no-arrivals feature set (see ablation).')"
    ),
    md(VARDICT),
    md("## Datasets and feature matrices per model variant"),
    code(
        "from pass_a import ACT_NUM as _ACT\n"
        "_ACT_V1 = [c for c in _ACT if c != 'act_verified']\n"
        "_miss = df.br_normalized_position_type == 'missing'\n"
        "PASS_B = CAT_COLS + NUM_COLS\n"
        "variants = pd.DataFrame([\n"
        "    {'variant': 'pass B — Models A + B (headline)', 'rows': len(df),\n"
        "     'data': 'datasets/working_set.parquet + datasets/geo_features.parquet',\n"
        "     'n_features': len(PASS_B), 'features': ', '.join(PASS_B)},\n"
        "    {'variant': 'pass A v1 — Model A + behavior minus verified', 'rows': len(df),\n"
        "     'data': 'pass B + datasets/activity.parquet',\n"
        "     'n_features': len(PASS_B) + len(_ACT_V1), 'features': 'pass B + ' + ', '.join(_ACT_V1)},\n"
        "    {'variant': 'pass A full — diagnostic only', 'rows': len(df),\n"
        "     'data': 'pass B + datasets/activity.parquet',\n"
        "     'n_features': len(PASS_B) + len(_ACT), 'features': 'pass A v1 + act_verified'},\n"
        "    {'variant': '2024 experiment — Model A', 'rows': 8906,\n"
        "     'data': 'datasets/working_set_2024.parquet (+ geo), same features as pass B',\n"
        "     'n_features': len(PASS_B), 'features': 'same as pass B'},\n"
        "    {'variant': 'decidable population — Models A + B', 'rows': int((~_miss).sum()),\n"
        "     'data': 'working_set minus the position-type-missing bucket, same features',\n"
        "     'n_features': len(PASS_B), 'features': 'same as pass B'},\n"
        "])\n"
        "with pd.option_context('display.max_colwidth', None):\n"
        "    display(variants)\n"
        "print('pass-B feature matrix preview (first 3 rows):')\n"
        "display(df[PASS_B].head(3))"
    ),
    md(
        "## Data profile — every column, fill rate, distribution, category frequencies\n\n"
        "Eyeball surface for the working set and for each Area 2 side table. The function is "
        "defined inline here on purpose so it stays editable in the notebook; `profile.py` holds "
        "the twin that build scripts import, and `write_profile()` there also drops "
        "`profiles/<name>.md` for datasets you have not loaded yet. Keep the two in step.\n\n"
        "Side tables are profiled raw: `engineer()` expects working-set columns and will raise on "
        "a user-grain side table."
    ),
    code(
        "def eda_summary(df: pd.DataFrame, cat_top_n: int = 5) -> pd.DataFrame:\n"
        '    """One row per column: dtype, fill, numeric distribution or category frequencies.\n'
        "\n"
        "    - dtype, count, missing_%                                    all columns\n"
        "    - mean, std, min, 25%, 50%, 75%, max, skew                   numeric only\n"
        "    - n_unique, top_N_values                                     categorical/object/bool\n"
        "\n"
        "    Cells that do not apply to a column's type are left as NaN.\n"
        '    """\n'
        '    numeric_cols = df.select_dtypes(include="number").columns\n'
        '    cat_cols = df.select_dtypes(include=["object", "category", "bool"]).columns\n'
        '    top_col = f"top_{cat_top_n}_values"\n'
        "\n"
        "    rows = {}\n"
        "\n"
        "    for col in numeric_cols:\n"
        "        s = df[col]\n"
        "        rows[col] = {\n"
        '            "dtype": str(s.dtype),\n'
        '            "count": s.notna().sum(),\n'
        '            "missing_%": round(s.isna().mean() * 100, 2),\n'
        '            "mean": round(s.mean(), 3),\n'
        '            "std": round(s.std(), 3),\n'
        '            "min": round(s.min(), 3),\n'
        '            "25%": round(s.quantile(0.25), 3),\n'
        '            "50%": round(s.quantile(0.5), 3),\n'
        '            "75%": round(s.quantile(0.75), 3),\n'
        '            "max": round(s.max(), 3),\n'
        '            "skew": round(s.skew(), 3),\n'
        "        }\n"
        "\n"
        "    for col in cat_cols:\n"
        "        s = df[col]\n"
        "        top = s.value_counts(dropna=True).head(cat_top_n)\n"
        '        top_str = ", ".join(f"{idx} ({cnt})" for idx, cnt in top.items())\n'
        "        rows[col] = {\n"
        '            "dtype": str(s.dtype),\n'
        '            "count": s.notna().sum(),\n'
        '            "missing_%": round(s.isna().mean() * 100, 2),\n'
        '            "n_unique": s.nunique(dropna=True),\n'
        "            top_col: top_str,\n"
        "        }\n"
        "\n"
        "    column_order = [\n"
        '        "dtype", "count", "missing_%", "mean", "std", "min", "25%", "50%", "75%", "max",\n'
        '        "skew", "n_unique", top_col,\n'
        "    ]\n"
        "    summary = pd.DataFrame(rows).T.reindex(columns=column_order)\n"
        '    summary.index.name = "column"\n'
        "    return summary"
    ),
    code(
        "# Working set goes through engineer() (that is what the model sees); side tables do not.\n"
        "print('=== working_set (engineered, model input) ===')\n"
        "display(eda_summary(df))\n"
        "\n"
        "for _name in ('geo_features', 'name_features'):\n"
        "    _p = PROJ / 'datasets' / f'{_name}.parquet'\n"
        "    if _p.exists():\n"
        "        print(f'=== {_name} (side table, raw) ===')\n"
        "        display(eda_summary(pd.read_parquet(_p)))\n"
        "    else:\n"
        "        print(f'({_name}.parquet not built yet)')"
    ),
    md(
        "## Model A (gradient boosting / HGB) vs Model B (elastic-net logistic regression) — training + stratified group 5-fold CV"
    ),
    code(
        "cv = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=7)\n"
        "x_lr = df[CAT_COLS + NUM_COLS]\n"
        "x_hgb = make_hgb_frame(df)\n"
        "splits = list(cv.split(x_lr, y, groups))\n"
        "oof = {'lr': np.zeros(len(df)), 'hgb': np.zeros(len(df))}\n"
        "rows, last_fold = [], None\n"
        "for i, (tr, va) in enumerate(splits):\n"
        "    lr = make_lr().fit(x_lr.iloc[tr], y.iloc[tr])\n"
        "    oof['lr'][va] = lr.predict_proba(x_lr.iloc[va])[:, 1]\n"
        "    hgb = HistGradientBoostingClassifier(categorical_features='from_dtype', random_state=7,\n"
        "                                         max_iter=400, early_stopping=True,\n"
        "                                         validation_fraction=0.15).fit(x_hgb.iloc[tr], y.iloc[tr])\n"
        "    oof['hgb'][va] = hgb.predict_proba(x_hgb.iloc[va])[:, 1]\n"
        "    rows.append({'fold': i, 'lr_auc': roc_auc_score(y.iloc[va], oof['lr'][va]),\n"
        "                 'hgb_auc': roc_auc_score(y.iloc[va], oof['hgb'][va])})\n"
        "    last_fold = (tr, va, hgb)\n"
        "display(pd.DataFrame(rows).round(4))\n"
        "pooled = pd.DataFrame({m: {'roc_auc': roc_auc_score(y, oof[m]),\n"
        "                           'pr_auc': average_precision_score(y, oof[m])}\n"
        "                       for m in oof}).T.round(4)\n"
        "display(pooled)\n"
        "winner = pooled.roc_auc.idxmax()\n"
        "p = oof[winner]\n"
        "print(f'winner: {winner}')"
    ),
    md("## Precision-recall frontier — Model A (HGB), out-of-fold"),
    code(
        "fr = frontier(y, p)\n"
        "display(fr[fr.precision.notna()][['at', 'precision']].reset_index(drop=True))\n"
        "display(fr[fr.recall.notna()][['at', 'recall']].reset_index(drop=True))"
    ),
    md(
        "## Fair rule baselines (review fix)\n\n"
        "The brief's pre-registered heuristic (non-GP email AND not-after-deadline) was miscoded — "
        "`fillna(False)` swept the 41% deadline-missing rows into 'before deadline', and 'signed up "
        "before deadline ⇒ real' is directionally backwards (most corroborated users sign up after "
        "their deadline). Replaced with single-marker rules evaluated fairly."
    ),
    code(
        "def rule_eval(name, mask):\n"
        "    mask = mask.astype(bool)\n"
        "    tp = int((y[mask] == 1).sum())\n"
        "    return {'rule': name, 'flagged_pct': round(float(mask.mean()), 3),\n"
        "            'precision': round(tp / max(int(mask.sum()), 1), 3),\n"
        "            'recall': round(tp / int(y.sum()), 3)}\n\n"
        "rules = [\n"
        "    ('majority: call everyone real', pd.Series(True, index=df.index)),\n"
        "    ('has recoverable filing deadline', df.has_deadline == 1),\n"
        "    ('a BR candidacy landed for the race by deadline', df.any_br_by_deadline == 1),\n"
        "    ('non-goodparty email domain', df.email_domain_class != 'goodparty'),\n"
        "    ('party declared', df.campaign_party != 'missing'),\n"
        "]\n"
        "display(pd.DataFrame([rule_eval(n, m) for n, m in rules]))"
    ),
    md("## Calibration — Model A (HGB): raw scores are already honest, no remap applied (verify-then-fix)"),
    code("display(reliability(y, p))"),
    md("## Figures — Model A (HGB)"),
    code(
        "import matplotlib.pyplot as plt\n\n"
        "INK, MUTED, GRID = '#3a3f47', '#6b7280', '#e5e7eb'\n"
        "BLUE = '#4269d0'\n"
        "plt.rcParams.update({'text.color': INK, 'axes.labelcolor': INK,\n"
        "                     'xtick.color': MUTED, 'ytick.color': MUTED, 'font.size': 11})\n\n"
        "prec, rec, _ = precision_recall_curve(y, p)\n"
        "fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))\n\n"
        "ax = axes[0]\n"
        "ax.plot(rec, prec, color=BLUE, lw=2)\n"
        "ax.axhline(y.mean(), color=MUTED, lw=1.2, ls='--')\n"
        "ax.text(0.02, y.mean() + 0.012, f'base rate {y.mean():.2f}', color=MUTED, fontsize=9)\n"
        "for r_t in (0.95, 0.8, 0.5):\n"
        "    p_at = prec[rec >= r_t].max()\n"
        "    ax.plot([r_t], [p_at], 'o', color=BLUE, ms=8)\n"
        "    ax.annotate(f'{p_at:.2f} @ recall {r_t}', (r_t, p_at), textcoords='offset points',\n"
        "                xytext=(-8, -14), ha='right', fontsize=9, color=INK)\n"
        "ax.set_xlabel('recall (share of real candidates kept)')\n"
        "ax.set_ylabel('precision (lower bound)')\n"
        "ax.set_title('Precision-recall frontier (OOF)', loc='left', fontsize=12)\n"
        "ax.set_ylim(0.4, 1.02)\n\n"
        "ax = axes[1]\n"
        "rel = reliability(y, p)\n"
        "ax.plot([0, 1], [0, 1], color=MUTED, lw=1.2, ls='--')\n"
        "ax.plot(rel.pred_mean, rel.obs_rate, 'o-', color=BLUE, lw=2, ms=8)\n"
        "ax.set_xlabel('mean predicted score (decile)')\n"
        "ax.set_ylabel('observed corroboration rate')\n"
        "ax.set_title('Reliability — raw scores, no remap', loc='left', fontsize=12)\n\n"
        "for ax in axes:\n"
        "    ax.grid(color=GRID, lw=0.6)\n"
        "    ax.set_axisbelow(True)\n"
        "    for s in ('top', 'right'):\n"
        "        ax.spines[s].set_visible(False)\n"
        "    for s in ('left', 'bottom'):\n"
        "        ax.spines[s].set_color(GRID)\n"
        "fig.tight_layout()\n"
        "fig.savefig(PROJ / 'figures_mvp.png', dpi=150, bbox_inches='tight')\n"
        "plt.show()"
    ),
    md(
        "## Forward split — Model A (HGB): train 2025 → test completed 2026\n\n"
        "The deployment-shaped read. Caveat: completed-2026 races are off-cycle, not the Nov-2026 wave."
    ),
    code(
        "tr_m = (df.cycle_year == 2025).values\n"
        "te_m = (df.cycle_year == 2026).values\n"
        "m_fwd = HistGradientBoostingClassifier(categorical_features='from_dtype', random_state=7,\n"
        "                                       max_iter=400, early_stopping=True,\n"
        "                                       validation_fraction=0.15).fit(x_hgb[tr_m], y[tr_m])\n"
        "p_fwd = m_fwd.predict_proba(x_hgb[te_m])[:, 1]\n"
        "print(f'n_train={tr_m.sum():,} (2025)  n_test={te_m.sum():,} (completed 2026)')\n"
        "print(f'forward ROC-AUC={roc_auc_score(y[te_m], p_fwd):.4f}  '\n"
        "      f'PR-AUC={average_precision_score(y[te_m], p_fwd):.4f}')\n"
        "display(frontier(y[te_m], pd.Series(p_fwd)).dropna(subset=['precision'])[['at', 'precision']]\n"
        "        .reset_index(drop=True))"
    ),
    md(
        "## Does 2024 data help or hurt? — Model A (HGB) experiment (added 2026-08-05)\n\n"
        "2024 has 8,906 labeled users at a suspiciously lower 45.0% positive rate (2025: 59.8%), which "
        "could be corroboration coverage rather than composition. The test: train with and without 2024, "
        "evaluate both on the same out-of-time sets (a 20% grouped 2025 holdout, and completed 2026). "
        "If adding 2024 degrades out-of-time performance, the suspicion is confirmed."
    ),
    code(
        "from sklearn.model_selection import GroupShuffleSplit\n\n"
        "df24 = engineer(pd.read_parquet(PROJ / 'datasets' / 'working_set_2024.parquet'))\n"
        "y24 = df24['corroborated'].astype(int)\n"
        "x24 = make_hgb_frame(df24)\n"
        "print(f'2024 pool: {len(df24):,} users, positive rate {y24.mean():.3f}, '\n"
        "      f'deadline present {df24.has_deadline.mean():.1%}')\n\n"
        "idx25 = np.where((df.cycle_year == 2025).values)[0]\n"
        "te26 = (df.cycle_year == 2026).values\n"
        "gss = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=7)\n"
        "tr_i, ho_i = next(gss.split(idx25, y.iloc[idx25], df['user_id'].iloc[idx25]))\n"
        "tr_pos, ho_pos = idx25[tr_i], idx25[ho_i]\n\n"
        "def cat_fix(xx):\n"
        "    xx = xx.copy()\n"
        "    for c in CAT_COLS:\n"
        "        xx[c] = xx[c].astype('category')\n"
        "    return xx\n\n"
        "train_sets = {\n"
        "    'train 2025-only': (x_hgb.iloc[tr_pos], y.iloc[tr_pos]),\n"
        "    'train 2024+2025': (cat_fix(pd.concat([x_hgb.iloc[tr_pos], x24], ignore_index=True)),\n"
        "                        pd.concat([y.iloc[tr_pos], y24], ignore_index=True)),\n"
        "}\n"
        "rows24 = []\n"
        "for name, (xt, yt) in train_sets.items():\n"
        "    m = HistGradientBoostingClassifier(categorical_features='from_dtype', random_state=7,\n"
        "                                       max_iter=400, early_stopping=True,\n"
        "                                       validation_fraction=0.15).fit(xt, yt)\n"
        "    p_ho = m.predict_proba(x_hgb.iloc[ho_pos])[:, 1]\n"
        "    p_26 = m.predict_proba(x_hgb[te26])[:, 1]\n"
        "    rows24.append({'train set': name,\n"
        "                   'auc_2025_holdout': round(roc_auc_score(y.iloc[ho_pos], p_ho), 4),\n"
        "                   'pr_2025_holdout': round(average_precision_score(y.iloc[ho_pos], p_ho), 4),\n"
        "                   'auc_2026': round(roc_auc_score(y[te26], p_26), 4),\n"
        "                   'pr_2026': round(average_precision_score(y[te26], p_26), 4)})\n"
        "display(pd.DataFrame(rows24))"
    ),
    md(
        "## Model B — standalone interpretable model: elastic-net logistic regression (added 2026-08-05)\n\n"
        "A **separate candidate model**, not an explanation layer for the HGB score. Its coefficients "
        "describe THIS model's scoring only. If HGB is what gets promoted, per-lead explanations must come "
        "from the HGB model's own attributions — quoting LR coefficients against HGB scores would drift "
        "the moment the models diverge. If coefficient-level auditability (e.g. for sales/marketing) is a "
        "requirement, the decision is to promote LR itself and accept its ~2-point AUC cost."
    ),
    code(
        "print(f\"LR pooled OOF (from the CV above): ROC-AUC={roc_auc_score(y, oof['lr']):.4f}  \"\n"
        "      f\"PR-AUC={average_precision_score(y, oof['lr']):.4f}   (HGB: {roc_auc_score(y, oof['hgb']):.4f} / \"\n"
        "      f\"{average_precision_score(y, oof['hgb']):.4f})\")\n"
        "print('\\nLR reliability (OOF deciles):')\n"
        "display(reliability(y, pd.Series(oof['lr'])))\n\n"
        "lr_full = make_lr().fit(x_lr, y)\n"
        "feat_names = lr_full.named_steps['pre'].get_feature_names_out()\n"
        "coefs = pd.Series(lr_full.named_steps['lr'].coef_[0], index=feat_names)\n"
        "coef_tbl = pd.DataFrame({'log_odds': coefs.round(3), 'odds_ratio': np.exp(coefs).round(3)})\n"
        "print('\\nmost NEGATIVE (pushes toward noise):')\n"
        "display(coef_tbl.sort_values('log_odds').head(12))\n"
        "print('most POSITIVE (pushes toward real):')\n"
        "display(coef_tbl.sort_values('log_odds').tail(12).iloc[::-1])"
    ),
    md(
        "## Slices — Model A (HGB) at a reference threshold (max-F1 on OOF; reference only, no blessed operating point)"
    ),
    code(
        "prec_c, rec_c, thr_c = precision_recall_curve(y, p)\n"
        "f1 = 2 * prec_c * rec_c / np.clip(prec_c + rec_c, 1e-9, None)\n"
        "t_star = float(thr_c[np.argmax(f1[:-1])])\n"
        "print(f'reference threshold (max-F1): {t_star:.3f}')\n"
        "df['_pred'], df['_p'] = (p >= t_star).astype(int), p\n\n"
        "def wilson(k, n, z=1.96):\n"
        "    if n == 0:\n"
        "        return (np.nan, np.nan)\n"
        "    ph = k / n\n"
        "    d = 1 + z * z / n\n"
        "    c = (ph + z * z / (2 * n)) / d\n"
        "    h = z * np.sqrt(ph * (1 - ph) / n + z * z / (4 * n * n)) / d\n"
        "    return (c - h, c + h)\n\n"
        "def slice_table(dim, top=None, ci=False):\n"
        "    def _one(s):\n"
        "        flag = s['_pred'] == 1\n"
        "        row = {'n': len(s), 'pos_rate': s['corroborated'].mean(),\n"
        "               'precision': s['corroborated'][flag].mean() if flag.any() else np.nan,\n"
        "               'recall': (s['_pred'][s['corroborated'] == 1]).mean()\n"
        "                         if (s['corroborated'] == 1).any() else np.nan,\n"
        "               'auc': roc_auc_score(s['corroborated'], s['_p'])\n"
        "                      if s['corroborated'].nunique() == 2 and len(s) >= 30 else np.nan}\n"
        "        if ci:\n"
        "            lo, hi = wilson(int(s['corroborated'][flag].sum()), int(flag.sum()))\n"
        "            row['prec_ci'] = f'[{lo:.2f}, {hi:.2f}]' if not np.isnan(lo) else ''\n"
        "        return pd.Series(row)\n"
        "    g = df.groupby(dim, observed=True).apply(_one, include_groups=False).sort_values('n', ascending=False)\n"
        "    return (g.head(top) if top else g).round(3)\n\n"
        "display(slice_table('cycle_year'))\n"
        "display(slice_table('election_level'))\n"
        "display(slice_table('campaign_state', top=10, ci=True))\n"
        "display(slice_table('icp_office_win'))\n"
        "display(slice_table('email_domain_class'))"
    ),
    md(
        "## Per-cohort reliability — Model A (HGB)\n\n"
        "Pooled calibration can hide within-cohort dishonesty when base rates run 26% (NY) to 81% (IA). "
        "Quintile reliability for the largest states, NY, and the missing-level bucket; `max_gap` is the "
        "largest |predicted − observed| across quintiles."
    ),
    code(
        "cohorts = {'state=OH': df.campaign_state == 'OH', 'state=IL': df.campaign_state == 'IL',\n"
        "           'state=TX': df.campaign_state == 'TX', 'state=NY': df.campaign_state == 'NY',\n"
        "           'level=missing': df.election_level == 'missing'}\n"
        "summ = []\n"
        "for name, m in cohorts.items():\n"
        "    r = reliability(y[m.values], pd.Series(p[m.values]), bins=5)\n"
        "    summ.append({'cohort': name, 'n': int(m.sum()),\n"
        "                 'mean_gap': round(float((r.pred_mean - r.obs_rate).abs().mean()), 3),\n"
        "                 'max_gap': round(float((r.pred_mean - r.obs_rate).abs().max()), 3)})\n"
        "display(pd.DataFrame(summ))\n"
        "print('NY quintile detail:')\n"
        "display(reliability(y[(df.campaign_state == 'NY').values],\n"
        "                    pd.Series(p[(df.campaign_state == 'NY').values]), bins=5))"
    ),
    md(
        "## Robustness — Model A (HGB): coverage-proxy ablations\n\n"
        "The label is BR/TS/DDHQ *presence*, so features correlated with BR **coverage** (position type, "
        "state, BR arrivals) could let the model re-learn 'which races BallotReady lists' instead of "
        "candidate legitimacy. Ablating them answers how much AUC is at stake.\n\n"
        "Fold-construction check (2026-08-05, `race_grouped_cv.py`): regrouping CV folds by race "
        "(state × BR position × election date) instead of user, so no race straddles train/validation, "
        "moves ROC-AUC 0.8750 → 0.8737 — cross-fold race sharing is not inflating the estimate."
    ),
    code(
        "ab_rows = []\n"
        "for name, drop in ABLATIONS.items():\n"
        "    cols = [c for c in CAT_COLS + NUM_COLS if c not in drop]\n"
        "    o = np.zeros(len(df))\n"
        "    for tr, va in splits:\n"
        "        m = HistGradientBoostingClassifier(categorical_features='from_dtype', random_state=7,\n"
        "                                           max_iter=400, early_stopping=True,\n"
        "                                           validation_fraction=0.15).fit(x_hgb[cols].iloc[tr], y.iloc[tr])\n"
        "        o[va] = m.predict_proba(x_hgb[cols].iloc[va])[:, 1]\n"
        "    ab_rows.append({'ablation': name, 'roc_auc': round(roc_auc_score(y, o), 4),\n"
        "                    'pr_auc': round(average_precision_score(y, o), 4)})\n"
        "display(pd.DataFrame(ab_rows))"
    ),
    md(
        "## Model A + product behavior — pass A, diagnostic only (added 2026-08-05)\n\n"
        "Per the brief's two-pass leakage design. These are **lifetime** aggregates (post-deadline "
        "behavior included) and behavior is multi-causal with the outcome, so pass A measures how much "
        "signal behavior carries — an upper bound and a diagnostic. **Pass B remains the analysis "
        "filter.** Activity fields: outreach sent/recipients, dashboard views, verified/pledged "
        "campaign, Pro, pro-upgrade milestone, campaign counts (`users_win_base` + "
        "`int__amplitude_user_milestones`)."
    ),
    code(
        "from pass_a import ACT_NUM, build_activity, run_cv\n"
        "dfa = build_activity(df)\n"
        "ACT_V1 = [c for c in ACT_NUM if c != 'act_verified']  # verified excluded per 2026-08-05 decision\n"
        "x_a_full = pd.concat([x_hgb, dfa[ACT_NUM]], axis=1)\n"
        "x_a_v1 = pd.concat([x_hgb, dfa[ACT_V1]], axis=1)\n"
        "res, fr_a, pass_a_fold = [], None, None\n"
        "for name, xx in (('pass B (no activity)', x_hgb),\n"
        "                 ('pass A v1: + activity, minus verified (headline)', x_a_v1),\n"
        "                 ('pass A full: + activity incl. verified (diagnostic)', x_a_full),\n"
        "                 ('activity only', dfa[ACT_NUM])):\n"
        "    o, last = run_cv(xx, y, groups)\n"
        "    res.append({'features': name, 'roc_auc': round(roc_auc_score(y, o), 4),\n"
        "                'pr_auc': round(average_precision_score(y, o), 4)})\n"
        "    if name.startswith('pass A v1'):\n"
        "        pass_a_fold = (xx, last)\n"
        "        pa, ra, _ = precision_recall_curve(y, o)\n"
        "        fr_a = pd.DataFrame({'at': [f'recall>={r}' for r in (0.95, 0.8, 0.5)],\n"
        "                             'precision': [round(float(pa[ra >= r].max()), 3) for r in (0.95, 0.8, 0.5)]})\n"
        "display(pd.DataFrame(res))\n"
        "display(fr_a)\n"
        "xx_a, (tr_a, va_a, m_a) = pass_a_fold\n"
        "pi_a = permutation_importance(m_a, xx_a.iloc[va_a], y.iloc[va_a], scoring='roc_auc',\n"
        "                              n_repeats=5, random_state=7)\n"
        "imp_a = pd.Series(pi_a.importances_mean, index=xx_a.columns).sort_values(ascending=False)\n"
        "print('top behavioral features (pass A v1 permutation, verified excluded):')\n"
        "display(imp_a[imp_a.index.isin(ACT_V1)].head(6).round(4).to_frame('auc_drop'))"
    ),
    md(
        "## The near-tautological bucket and the decidable population — Models A + B (independent validation, 2026-08-05)\n\n"
        "External validation surfaced this: when `br_normalized_position_type` is missing, BallotReady has "
        "no position record for the declared office, so it near-mechanically cannot have a candidacy "
        "record either — the label is ~0 by construction, and the bucket is 100% nested inside "
        "`br_arrivals_missing`. Pooled metrics therefore get a free ride on 12% of rows a rule decides. "
        "The number that describes the judgment calls the score actually supports is the "
        "**decidable-population** metric below (BR knows the office exists)."
    ),
    code(
        "miss = df.br_normalized_position_type == 'missing'\n"
        "display(pd.DataFrame([{'rows': int(miss.sum()), 'share_of_pool': round(float(miss.mean()), 3),\n"
        "                       'corroboration_rate': round(float(y[miss].mean()), 3),\n"
        "                       'nested_in_br_arrivals_missing': f\"{(df.loc[miss, 'br_arrivals_missing'] == 1).mean():.0%}\",\n"
        "                       'rule_accuracy_on_bucket': round(float(1 - y[miss].mean()), 3)}]))\n\n"
        "sub = df[~miss].reset_index(drop=True)\n"
        "ys, gs = sub['corroborated'].astype(int), sub['user_id']\n"
        "xs_hgb, xs_lr = make_hgb_frame(sub), sub[CAT_COLS + NUM_COLS]\n"
        "cv_r = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=7)\n"
        "oof_rh, oof_rl = np.zeros(len(sub)), np.zeros(len(sub))\n"
        "for tr, va in cv_r.split(xs_lr, ys, gs):\n"
        "    oof_rh[va] = HistGradientBoostingClassifier(categorical_features='from_dtype', random_state=7,\n"
        "        max_iter=400, early_stopping=True, validation_fraction=0.15\n"
        "        ).fit(xs_hgb.iloc[tr], ys.iloc[tr]).predict_proba(xs_hgb.iloc[va])[:, 1]\n"
        "    oof_rl[va] = make_lr().fit(xs_lr.iloc[tr], ys.iloc[tr]).predict_proba(xs_lr.iloc[va])[:, 1]\n"
        "print(f'decidable population: n={len(sub):,}, base rate {ys.mean():.3f}')\n"
        "display(pd.DataFrame({\n"
        "    'hgb': {'roc_auc': roc_auc_score(ys, oof_rh), 'pr_auc': average_precision_score(ys, oof_rh)},\n"
        "    'lr': {'roc_auc': roc_auc_score(ys, oof_rl), 'pr_auc': average_precision_score(ys, oof_rl)},\n"
        "}).T.round(4))\n"
        "pr_r, rc_r, _ = precision_recall_curve(ys, oof_rh)\n"
        "display(pd.DataFrame({'at': [f'recall>={r}' for r in (0.95, 0.8, 0.5)],\n"
        "                      'precision_decidable': [round(float(pr_r[rc_r >= r].max()), 3)\n"
        "                                              for r in (0.95, 0.8, 0.5)]}))\n"
        "print('Two-stage recommendation: route position-type-missing by RULE (report as its own segment,\\n'\n"
        "      'like the no-office segment); the model scores only the decidable 88%. Operating points for\\n'\n"
        "      'consumers should be quoted from the decidable-population frontier above.')"
    ),
    md("## What drives Model A (HGB permutation importance, last fold's validation split)"),
    code(
        "tr, va, hgb = last_fold\n"
        "pi = permutation_importance(hgb, x_hgb.iloc[va], y.iloc[va], scoring='roc_auc',\n"
        "                            n_repeats=5, random_state=7)\n"
        "display(pd.Series(pi.importances_mean, index=x_hgb.columns)\n"
        "        .sort_values(ascending=False).round(4).to_frame('auc_drop'))"
    ),
    md(
        "## Serialized artifacts — Models A + B (reproducibility fix, 2026-08-05)\n\n"
        "Final pass-B models fit on the full pool and saved, plus fresh OOF predictions and a results "
        "snapshot pinning the sklearn version — so any specific score is reproducible without a refit."
    ),
    code(
        "import json\n"
        "import joblib\n"
        "import sklearn\n\n"
        "final_hgb = HistGradientBoostingClassifier(categorical_features='from_dtype', random_state=7,\n"
        "                                           max_iter=400, early_stopping=True,\n"
        "                                           validation_fraction=0.15).fit(x_hgb, y)\n"
        "final_lr = make_lr().fit(x_lr, y)\n"
        "joblib.dump(final_hgb, PROJ / 'model_hgb_passb.joblib')\n"
        "joblib.dump(final_lr, PROJ / 'model_lr_passb.joblib')\n\n"
        "oof_df = df[['user_id', 'campaign_id', 'corroborated', 'cycle_year']].copy()\n"
        "oof_df['oof_lr'], oof_df['oof_hgb'] = oof['lr'], oof['hgb']\n"
        "oof_df.to_parquet(PROJ / 'datasets' / 'oof_predictions.parquet', index=False)\n\n"
        "snapshot = {\n"
        "    'executed': '2026-08-05', 'sklearn': sklearn.__version__, 'n': int(len(df)),\n"
        "    'positive_rate': round(float(y.mean()), 4),\n"
        "    'pooled': {m: {'roc_auc': round(roc_auc_score(y, oof[m]), 4),\n"
        "                   'pr_auc': round(average_precision_score(y, oof[m]), 4)} for m in oof},\n"
        "    'decidable_population': {'n': int(len(sub)), 'base_rate': round(float(ys.mean()), 4),\n"
        "                             'hgb_roc_auc': round(roc_auc_score(ys, oof_rh), 4),\n"
        "                             'lr_roc_auc': round(roc_auc_score(ys, oof_rl), 4)},\n"
        "}\n"
        "(PROJ / 'results.json').write_text(json.dumps(snapshot, indent=2))\n"
        'print(f"saved model_hgb_passb.joblib, model_lr_passb.joblib, oof_predictions.parquet, "\n'
        '      f"results.json (sklearn {sklearn.__version__})")'
    ),
    md(FINDINGS),
]

nb = nbf.v4.new_notebook(
    cells=cells,
    metadata={
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python"},
    },
)

out = NB_DIR / "01_mvp.ipynb"
client = NotebookClient(
    nb, timeout=1800, kernel_name="python3", resources={"metadata": {"path": str(NB_DIR)}}
)
client.execute()
nbf.write(nb, out)
print(f"executed + saved -> {out}")
