---
name: ddhq-miss-audit
description: Audit and quantify gp_api product campaigns that have no matching DDHQ election result, for a given election-date window. Classifies every unmatched candidacy into a definitive reason (never ran / race not present in DDHQ data / never made the ballot / product wrong-office or wrong-date / DDHQ omission / genuine matcher false-negative) using a deterministic SQL pre-pass against DDHQ staging plus a web-verification subagent fan-out, then writes an external aggregate summary CSV, an internal per-record detail CSV, and a confirmed-winners list. Use when asked to understand or quantify why product users tied to a 2026 (or later) campaign are not matched to election results, or to refresh the prior DDHQ miss audit.
---

# DDHQ miss audit

Quantify why gp_api product campaigns in an election-date window have no DDHQ
election result, and split the unmatched population into definitive, web-verified
reasons. Worked window: 2026-01-01..2026-05-31 (2026 H1).

## The entity being matched (read this first, it drives every interpretation)

The matching entity is a **candidacy_stage = person + office + election_date**. A
person who shows up in DDHQ under a **different office** or a **different date** is a
**different entity**, so not matching it is **correct by design** — not a matcher bug.
This single fact reframes most "we should have matched" intuitions:

- Person in DDHQ at a different office -> usually our product stored the **wrong
  office** (data-quality), or they genuinely ran for two different things. Correct non-match.
- Person in DDHQ at a different date -> our product stored the **wrong / single
  date** (gp_api hard-codes one general-election date per campaign). Correct non-match.
- The ONLY genuine matcher false-negative is **same person + same office + same
  date present in DDHQ that still did not cluster** (deterministic `C3b`).

**Do not re-implement office matching to grade the matcher.** Office strings are not
comparable across gp_api and DDHQ (different word order, locality embedding, and
office_type vocab — e.g. "wood county school board" vs "Wood County Board of
Education"); resolving office identity is exactly what the probabilistic matcher is
for. So the deterministic pre-pass matches PEOPLE on **name + state + date only** and
treats office identity as a question for **web verification**, not SQL. A deterministic
office gate biases the audit toward *hiding* true FNs (a same-person, same-date pair
whose office is merely reworded gets dismissed as a "correct non-match"), which is the
one thing the audit must not do.

When you report, attribute different-office/different-date misses to **product data
quality**, not to the matcher. The bulk of unmatched records are correctly unmatched.
Because C3b is matched by name+state+date (not office), it surfaces a handful of
candidate FNs (~15 in the worked H1 run); web verification filters the namesakes, and
the confirmed genuine matcher-FN count is what you report.

## Setup

Databricks via CLI OAuth (`~/.databrickscfg`). Run SQL through the warehouse
statement API on Serverless Starter (`18583d8b081c6486`),
`format=JSON_ARRAY`, `disposition=INLINE`. The miss set is a few hundred rows.

Pick a scratch work dir (the session scratchpad). All intermediate JSON and the
output CSVs land there.

## Step 0: freshness gate

Confirm `er_source.clustered_candidacy_stages` reflects the latest matcha run and
`dbt.stg_airbyte_source__ddhq_gdrive_election_results` has the latest DDHQ load
(`max(election_date)` should cover your window). State both in the report.

## Step 1: deterministic pre-pass (classify.sql)

Edit the window in `classify.sql` (`miss0` WHERE, and `dd0` lookback ~6 months
earlier), run it, and save the rows as `records.json` (array of objects keyed by
column name). It excludes test users (`@goodparty.org` email) and emits one row per
miss with a `category`:

| category | meaning |
|---|---|
| `C1_absent_for_state` | DDHQ has no data for the state at all |
| `C1_race_not_tracked` | state covered, but no DDHQ race in this locality (coverage gap) |
| `C2_race_tracked_person_absent` | DDHQ holds a race in this locality; this person is absent (split by web) |
| `C3_singleton_lost_or_unk` | same name+state at a DIFFERENT date in DDHQ (upper bound: may be a namesake) |
| `C3_singleton_WON` | same, and it was a win we appear to already hold |
| `C3b_person_sameday_nearFN` | same name+state+date in DDHQ that did not cluster (candidate matcher FN) |

Non-negotiable design rules (undo any and a known bug returns):
1. Resolve race existence against DDHQ **staging** (not the clustered table, not
   `official_office_name_tokens` — both are lossy / post-filter).
2. Match people on **name + state + date only**. Never gate person/FN matching on an
   office-token comparison: office strings are not comparable across the two sources, so
   an office gate hides true FNs (see "the entity being matched" above).
3. Coverage (C1 vs C2) is a **coarse locality-name presence** signal only — does DDHQ
   hold any race whose place tokens overlap ours. It pre-sorts; web verification is the
   arbiter. Never infer coverage from office level.

## Step 2: fixture gate (run before trusting counts)

These have known, web-confirmed answers. The classifier MUST reproduce them; if any
fails, the logic regressed.

| name | state | gp office | expected |
|---|---|---|---|
| Mary Adams | DE | colonial school board - district f | `C2_race_tracked_person_absent` |
| Deirdre Goins | AK | anchorage municipal assembly - seat g, district 4 | `C2_race_tracked_person_absent` |
| Marisol Ortega | FL | lake hamilton town council - seat 1 | `C3_singleton_WON` |
| Debra Barnes | AR | crossett school board - zone 2 | `C3_singleton_WON` |

## Step 3: web-verification fan-out (USE A SMALL MODEL)

Verify every C1 + C2 + C3b record. Skip the C3 set: it is matched by name+state at a
different date, so it is an upper bound on "we already hold this" (some are namesakes),
but a different-date C3 can never hide a same-date FN, so skipping it is safe for the
headline. This is the cost driver, so the subagents MUST run on a small model.

1. Filter `records.json` to `category` matching `^(C1_|C2_|C3b_)`. Write to
   `to_verify.json`.
2. Shard into batches of ~20. Write each to `batches/batch_NN.json`.
3. Launch one `general-purpose` subagent per batch, **with `model: "haiku"`**
   (cheapest; the task is structured web lookups). Each agent: reads
   `verify_contract.md` (same dir) and its batch file, does the research INCLUDING the
   required broad state-wide name search, writes its JSON array to
   `results/result_NN.json`, and returns the same array. Launch ~10 per message.

   Cost rule: do not use the orchestrator's model (usually Opus) for the fan-out.
   Default `model: "haiku"`. If a spot-check of a batch shows several wrong or
   low-confidence calls, re-run only those batches with `model: "sonnet"`. Never use
   `opus` for verification.

4. The contract forbids guessing wins and requires confirming locality/office before
   trusting a name hit (common-name collisions are the main error mode).

Confirm scope with the user before launching — it is a large, outward-facing run on
hundreds of real people's names (~30 subagents for a full H1 window).

## Step 3b: AI office-judgment pass (the wrong-office bucket)

Subagents are told to leave `actual_office` null when the office they find matches
ours, but they routinely rephrase it instead ("Town Commission" -> "Town
Commissioner", "District 1" -> "Seat 1", "School Board" -> "School District board").
If you bucket on `actual_office` being non-empty, the product-wrong-office count is
inflated 2-3x by these rephrasings (Jane Reiser / Hillsboro Beach was the canonical
example: same office, mislabeled wrong-office).

So decide same-vs-different with a model, not a string rule. The candidate set is
small (~60 for an H1 window):

1. Collect every record where `category` is `C2_*`/`C3b`, the subagent said the
   person ran (`final_category` not NON_CANDIDATE / NOT_ON_BALLOT), and
   `actual_office` is non-empty. Write `{unique_id, state, product_office,
   reported_office}` rows to `office_judge_input.json`.
2. Launch ONE small-model subagent (`model: "haiku"`, no web research — pure semantic
   text judgment) that reads that file and writes `office_judgments.json`:
   `{unique_id, judgment: same|different|unsure, reason}`. Rule: same locality + same
   governing body + same/unspecified seat = `same`; different jurisdiction, different
   body, or a different seat/district NUMBER = `different` (office granularity includes
   the seat, so a wrong district is a real product error).
3. `aggregate.py` reads `office_judgments.json` automatically: `same` -> the office is
   correct, so the record falls to "race in DDHQ, candidate absent" (or matcher FN);
   `different`/`unsure` -> product wrong-office. The token heuristic in `same_office`
   is only a fallback for records with no judgment.

## Step 4: aggregate (aggregate.py)

Merge `results/result_*.json` into `all_verified.json`
(`jq -s 'add' results/result_*.json > all_verified.json`), then run
`python aggregate.py <work_dir>`. It re-buckets all records under the
candidacy_stage definition. **Coverage is taken from the deterministic SQL category,
never predicted from office size** — `C1_*` means no DDHQ race matched our locality;
`C2_*`/`C3b` means DDHQ holds a race in the locality. Final buckets: the subagent's
candidacy status (NON_CANDIDATE / NOT_ON_BALLOT / ran) combines with the SQL category —
a ran record with `category=C1_*` is "race not in DDHQ data"; a ran record with `C2_*`
and a different `actual_office` is product wrong-office; `C3_*` is product wrong-date; a
`C3b` (same name+state+date) is the only candidate matcher FN, confirmed by verification.

## Outputs

- `ddhq_unmatched_summary.csv` — **external/shareable**. Aggregate counts by outcome
  with plain-language descriptions and an "expected to be unmatched" column. No
  per-person rows. This is the stakeholder artifact.
- `ddhq_unmatched_by_office_level.csv` — office-level breakdown of real candidates
  whose race is not in DDHQ data. Descriptive audit aid, not a coverage prediction.
- `ddhq_unmatched_confirmed_wins.csv` — confirmed election winners (public record),
  the positive headline, tagged with office level and whether the race is in DDHQ.
  Shareable, but it identifies people as product users; confirm the sharing use case.
- `ddhq_unmatched_detail.csv` — **internal only**. One row per record with the
  candidacy-status label, office level, source URL, confidence. Do NOT publish
  externally: it labels named individuals as non-candidates / junk signups.

## Reporting

Lead with: share of the unmatched population that is **correctly unmatched** (no
matchable candidacy_stage exists), then the two real levers —
**product data quality** (wrong office/date for real candidacies whose race is in
DDHQ) and the **races not present in DDHQ data** (real candidates, including winners,
whose race never appears in DDHQ). Report the latter with the office-level breakdown;
do NOT assert a race "should have been covered" from its size — we contract DDHQ for
small races, so coverage is whatever the staging data shows, full stop. State the
genuine matcher-FN count explicitly — report the **verification-confirmed** C3b count,
not the raw candidate count (the pre-pass surfaces ~15 candidates by name+state+date;
most are namesakes the web pass clears). Note that "no evidence found" records are an
upper bound on true non-candidates, not confirmed non-candidates.

## Gotchas

- DDHQ `is_winner` is in staging, typed BOOLEAN in some partitions and STRING in
  others; cast to string and compare to `'true'`.
- Do person/name matching in SQL (a join), not an in-memory dict.
- gp_api `election_date` is unreliable (hard-coded single general date); never gate
  C1/C2 on date equality — judge race presence date-agnostically.
- `is_verified` is a bad fake-signup filter (drops ~2/3 of real wins). Don't use it.
- Coverage is empirical, not predicted. We contract DDHQ to cover small races, so never
  infer coverage from office level; read it from whether the race is in DDHQ staging.
