---
name: gold-match-run-audit
description: Audit one run key of the gold-match pipeline (llm_l2_br_match_results) before publication or as a post-hoc review — run shape and confidence, the geography rule-class mirror with prior-answer transitions, the override suite, the ratified holdout gate via score_holdout.py, a web spot-check, and a sign-off checklist. Use after a supervised or automated gold-match run lands rows under a new attempted_at, before the nightly swap publishes them, or when reviewing a completed run's abstain/withdrawal counts.
---

# Gold-match run audit

`goodparty_data_catalog.model_predictions.llm_l2_br_match_results` is append-only;
a run is identified by its single `attempted_at` timestamp (the run key). The
baseline run key is `2026-01-26` (the January seed). The staging model
`dbt.stg_model_predictions__llm_l2_br_match` serves the newest row per office.
This skill audits ONE run key at a time: the supervised cutover run first, the
automated daily runs later.

## Setup

Pick a scratch directory (the session scratchpad works). Each step's SQL below
carries a `__RUN_KEY__` placeholder standing for the run's `attempted_at`, written
as a bare timestamp body (`2026-08-27 14:00:00` — no quotes, no `timestamp` cast;
the SQL already wraps it as `timestamp'__RUN_KEY__'`). Copy a step's SQL to a
scratch file, substitute, then run it through the databricks-query helper
(`.claude/skills/databricks-query/SKILL.md`), which takes one statement per call
— every fenced block below is exactly one statement for that reason:

```bash
RUN_KEY='2026-08-27 14:00:00'
sed "s/__RUN_KEY__/$RUN_KEY/g" step1.sql > "$SCRATCH/step1.sql"
python .claude/skills/databricks-query/scripts/dbsql.py -f "$SCRATCH/step1.sql"
```

Run each step in order, read the printed rows, interpret them against the lines
under that step, then move on.

## Step 0 — Identify the run

```sql
select distinct attempted_at
from goodparty_data_catalog.model_predictions.llm_l2_br_match_results
order by 1 desc
```

Confirm the candidate `attempted_at` with the operator, alongside the batch size
their own run summary reports. The writer contract counts what it wrote; this
audit re-counts independently, starting in Step 1. Set `__RUN_KEY__` to the
confirmed value for every step below, copied VERBATIM from this output —
fractional seconds included, if present — because every step filters on exact
timestamp equality.

## Step 1 — Run shape

One statement, `union all` branches over the results table, the staging model,
the district universe, and the two election-api marts.

```sql
with
    run_rows as (
        select br_database_id, l2_state, l2_district_type, l2_district_name, confidence
        from goodparty_data_catalog.model_predictions.llm_l2_br_match_results
        where attempted_at = timestamp'__RUN_KEY__'
    ),

    br_feed_check as (
        select run_rows.br_database_id
        from run_rows
        left join
            goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_position as br
            on br.database_id = run_rows.br_database_id
        where br.database_id is null
    ),

    -- Mirrors the warn-severity l2_district_tuple_exists test's row_condition on
    -- stg_model_predictions__llm_l2_br_match (dbt/project/models/staging/
    -- model_predictions_source/stg_model_predictions.yaml). "Matched" is the
    -- generic test's own `district_name is not null` filter
    -- (dbt/project/tests/generic/test_l2_district_tuple_exists.sql), not a
    -- separate predicate restated here.
    label_check_tuples as (
        select distinct l2_state, l2_district_type, l2_district_name
        from goodparty_data_catalog.dbt.stg_model_predictions__llm_l2_br_match
        where l2_district_name is not null and attempted_at <> timestamp'2026-01-26'
    ),

    label_check_missing as (
        select label_check_tuples.*
        from label_check_tuples
        left join
            goodparty_data_catalog.dbt.int__l2_district_universe as universe
            on universe.state_postal_code = label_check_tuples.l2_state
            and universe.district_type = label_check_tuples.l2_district_type
            and universe.district_name = label_check_tuples.l2_district_name
        where universe.state_postal_code is null
    ),

    -- The run-scoped variant: THIS run's own matched tuples against the
    -- current universe. The staging-wide count above describes the serving
    -- state; only this one attributes a dead label to the audited run.
    run_label_missing as (
        select distinct run_rows.l2_state, run_rows.l2_district_type, run_rows.l2_district_name
        from run_rows
        left join
            goodparty_data_catalog.dbt.int__l2_district_universe as universe
            on universe.state_postal_code = run_rows.l2_state
            and universe.district_type = run_rows.l2_district_type
            and universe.district_name = run_rows.l2_district_name
        where run_rows.l2_district_name is not null and universe.state_postal_code is null
    ),

    -- Same join as dbt/project/tests/assert_position_district_voter_coverage_floor.sql;
    -- the floor itself lives there and is not reproduced here.
    coverage as (
        select
            count(*) as positions_with_district,
            count(district.registered_voters) as positions_on_populated_district
        from goodparty_data_catalog.dbt.m_election_api__position as position
        join
            goodparty_data_catalog.dbt.m_election_api__district as district
            on position.district_id = district.id
    )

select 'rows_under_key' as metric, cast(count(*) as double) as value, '' as detail
from run_rows

union all

select 'rows_matched', cast(count(*) as double), ''
from run_rows
where l2_district_name is not null

union all

select 'rows_abstained', cast(count(*) as double), ''
from run_rows
where l2_district_name is null

union all

select 'matched_confidence_min', cast(min(confidence) as double), ''
from run_rows
where l2_district_name is not null

union all

select 'matched_confidence_median', percentile(confidence, 0.5), ''
from run_rows
where l2_district_name is not null

union all

select 'matched_confidence_max', cast(max(confidence) as double), ''
from run_rows
where l2_district_name is not null

union all

select
    'rows_absent_from_br_feed',
    cast(count(*) as double),
    'feed churn between batch generation and audit, informational'
from br_feed_check

union all

select
    'label_check_warn_count',
    cast(count(*) as double),
    'distinct dead tuples in the CURRENT serving state since the baseline'
from label_check_missing

union all

select
    'run_label_check_missing',
    cast(count(*) as double),
    'distinct dead tuples matched by THIS run'
from run_label_missing

union all

select
    'coverage_ratio_positions_on_populated_over_with_district',
    positions_on_populated_district * 1.0 / nullif(positions_with_district, 0),
    concat(positions_on_populated_district, ' / ', positions_with_district)
from coverage
```

Read the printed rows, then interpret against these lines:

- Both label metrics and the coverage ratio are meaningful only AFTER the
  post-append dbt rebuild (the cutover's rebuild-then-gate ordering). Before
  it, they read a MIXED snapshot — the staging model is a view over live rows,
  so it already includes the appended run, while the universe and the
  election-api marts are still the last build — which is neither the previous
  publication nor this one. Never gate on the pre-rebuild reading.
- A nonzero `run_label_check_missing` is a HARD STOP before publication: THIS
  run shipped labels the current universe does not carry — delete the run's
  rows per SPEC 3.5, rebuild, stop.
- `label_check_warn_count` nonzero while `run_label_check_missing` is zero
  means the dead tuple belongs to a DIFFERENT run — an earlier run's answer, or
  a later relabel wave when auditing post hoc. Deleting this run's rows cannot
  clear it; repair it at its source before publication. For a post-hoc audit of
  a superseded run, only the run-scoped metric speaks for the audited run.
  The January baseline stratum is deliberately OUT OF SCOPE for both metrics'
  staging-wide reading, mirroring the warn test's own scoping: a January-origin
  dead label is the pre-existing backlog (the pending list's dead-label rule
  has already reopened those offices), and a dead label joins nothing — the
  office shows no number, not a wrong one — so counting them would red every
  audit until the whole backlog is re-matched.
- `rows_under_key` must equal the operator's reported batch count. ZERO rows
  means a mistranscribed run key far more often than a missing run — re-copy it
  verbatim from Step 0 before concluding anything. A genuine short write is
  repaired by deleting the run's rows and re-running.
- `coverage_ratio_positions_on_populated_over_with_district` must clear the
  floor `dbt/project/tests/assert_position_district_voter_coverage_floor.sql`
  states, with this run's labels in place (see the as-built line above).

## Step 2 — Rule classes, outcomes, and prior-answer transitions

The classification below is recomputed at audit time from BallotReady's own
fields — there is no persisted rule column anywhere, by design, so this is the
only way to know which path an office's match took.

### The classification mirror

Mirror of `_classify_office_geography` in
`omni:packages/gp-ai/stitch_golden_data/prod_gold_data/l2_br_matcher.py`, which
itself enumerates its family vocabulary from `get_l2_district_types(scope="all")`
in `dbt/project/macros/l2_district_columns.sql`. A type added to that macro
needs this block and the matcher's own constants re-checked together. Parent
(whole-jurisdiction) type sets are the matcher's menu-DENIAL concern (which
candidates it hides), never the classification label itself, so they are
intentionally absent from this classification-only mirror.

```sql
with
    run_rows as (
        select br_database_id, l2_state, l2_district_type, l2_district_name, confidence
        from goodparty_data_catalog.model_predictions.llm_l2_br_match_results
        where attempted_at = timestamp'__RUN_KEY__'
    ),

    -- Same partition/order as stg_model_predictions__llm_l2_br_match's
    -- `latest_attempt`, capped before the run key so it reads the office's
    -- answer walking INTO this run.
    prior_answer as (
        select br_database_id, l2_state, l2_district_type, l2_district_name
        from goodparty_data_catalog.model_predictions.llm_l2_br_match_results
        where attempted_at < timestamp'__RUN_KEY__'
        qualify
            row_number() over (
                partition by br_database_id order by attempted_at desc, l2_district_name nulls first
            )
            = 1
    ),

    -- CURRENT BallotReady fields for every office under the key. Feed absence
    -- is witnessed by the JOIN KEY (the same churn Step 1's
    -- rows_absent_from_br_feed counts), never by a null field, because the
    -- matcher classifies null-field offices normally. Normalization mirrors the
    -- matcher's load boundary exactly: empty and any-case "null" sentinels
    -- become absent, and kept values stay UNMODIFIED — no trimming, so a padded
    -- geo_id is malformed to the format check on both sides.
    office_geo as (
        select
            run_rows.br_database_id,
            br.database_id is null as absent_from_br_feed,
            upper(trim(br.state)) as office_state,
            coalesce(case when lower(trim(br.mtfcc)) in ('', 'null') then null else br.mtfcc end, '')
                as mtfcc,
            coalesce(br.is_judicial, false) as is_judicial,
            coalesce(br.has_unknown_boundaries, false) as has_unknown_boundaries,
            case when lower(trim(br.geo_id)) in ('', 'null') then null else br.geo_id end as geo_id,
            case
                when lower(trim(br.sub_area_name)) in ('', 'null') then null else br.sub_area_name
            end as sub_area_name,
            case
                when lower(trim(br.sub_area_value)) in ('', 'null') then null else br.sub_area_value
            end as sub_area_value
        from run_rows
        left join
            goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_position as br
            on br.database_id = run_rows.br_database_id
    ),

    family_geo as (
        select
            office_geo.*,
            case office_geo.mtfcc
                when 'G4020' then 'county'
                when 'X0005' then 'county'
                when 'G4110' then 'place'
                when 'G4210' then 'place'
                when 'X0001' then 'place'
                when 'G5420' then 'school'
                when 'G5400' then 'school'
                when 'G5410' then 'school'
                when 'X0102' then 'school'
                when 'G4040' then 'county_subdivision'
            end as family,
            (office_geo.sub_area_name is not null or office_geo.sub_area_value is not null) as has_sub_area
        from office_geo
    ),

    leveled as (
        select
            family_geo.*,
            case family_geo.family
                when 'county' then 5
                when 'place' then 7
                when 'school' then 7
                when 'county_subdivision' then 10
            end as family_parent_geoid_length
        from family_geo
    ),

    -- geo_id classified against the family's parent (whole-jurisdiction) Census
    -- id length: only the first family_parent_geoid_length characters must be
    -- digits, since a real slice id's own suffix need not be.
    geo_level as (
        select
            leveled.*,
            case
                when has_unknown_boundaries then 'slice'
                when geo_id is null or length(geo_id) < family_parent_geoid_length then 'malformed'
                when not substring(geo_id, 1, family_parent_geoid_length) rlike '^[0-9]+$' then 'malformed'
                when length(geo_id) = family_parent_geoid_length then 'whole'
                else 'slice'
            end as level
        from leveled
    ),

    -- Per-state vocabulary the classifier reads. Only SUB types are needed
    -- (the slice zero-subtype abstain check) -- PARENT types shape menu denial
    -- only, per the note above.
    state_vocab as (
        select
            upper(trim(state_postal_code)) as state_postal_code,
            max(case when district_type rlike '^Judicial_' then 1 else 0 end) = 1 as has_judicial_vocab,
            sum(
                case
                    when district_type rlike '^Judicial_'
                        and district_type <> 'Judicial_Supreme_Court_District'
                        then 1
                    else 0
                end
            )
            = 0 as only_judicial_type_is_sole_supreme,
            max(
                case
                    when
                        district_type in (
                            'County_Commissioner_District',
                            'County_Supervisorial_District',
                            'County_Legislative_District'
                        )
                        then 1
                    else 0
                end
            )
            = 1 as has_county_subtype,
            max(
                case
                    when
                        district_type
                        in ('City_Ward', 'City_Council_Commissioner_District', 'Village_Ward', 'Borough_Ward')
                        then 1
                    else 0
                end
            )
            = 1 as has_place_subtype,
            max(
                case
                    when
                        district_type in (
                            'School_Subdistrict',
                            'Unified_School_SubDistrict',
                            'Elementary_School_SubDistrict',
                            'High_School_SubDistrict',
                            'Board_of_Education_SubDistrict',
                            'County_Board_of_Education_SubDistrict',
                            'School_Board_District'
                        )
                        then 1
                    else 0
                end
            )
            = 1 as has_school_subtype,
            max(case when district_type in ('Township_Ward', 'Town_Ward') then 1 else 0 end) = 1
                as has_county_subdivision_subtype
        from goodparty_data_catalog.dbt.int__l2_district_universe
        group by upper(trim(state_postal_code))
    ),

    labeled as (
        select
            geo_level.br_database_id,
            geo_level.office_state,
            run_rows.l2_state,
            run_rows.l2_district_type,
            run_rows.l2_district_name,
            case when run_rows.l2_district_name is not null then 'matched' else 'abstained' end as outcome,
            case
                when geo_level.absent_from_br_feed then null  -- office gone from BR staging; see Step 1
                when geo_level.mtfcc = 'X0024' then 'R0_party_committee'
                when
                    geo_level.is_judicial
                    and geo_level.mtfcc <> 'G4000'
                    and (
                        not coalesce(sv.has_judicial_vocab, false)
                        or coalesce(sv.only_judicial_type_is_sole_supreme, false)
                    )
                    then 'R1_judicial_abstain'
                when geo_level.is_judicial then 'R1_judicial_menu'
                when not geo_level.has_sub_area or geo_level.family is null then 'pass_through'
                when geo_level.level = 'malformed' then 'pass_through'
                when
                    geo_level.level = 'slice'
                    and not (
                        (geo_level.family = 'county' and coalesce(sv.has_county_subtype, false))
                        or (geo_level.family = 'place' and coalesce(sv.has_place_subtype, false))
                        or (geo_level.family = 'school' and coalesce(sv.has_school_subtype, false))
                        or (
                            geo_level.family = 'county_subdivision'
                            and coalesce(sv.has_county_subdivision_subtype, false)
                        )
                    )
                    then 'R2_slice_zero_subtype_abstain'
                when geo_level.level = 'slice' then 'R2_slice_asserted'
                when geo_level.level = 'whole' and geo_level.family = 'school' then 'R2_whole_school_gated'
                when geo_level.level = 'whole' then 'R2_whole_asserted'
            end as rule_class,
            case
                when
                    run_rows.l2_district_name is not null
                    and (prior_answer.br_database_id is null or prior_answer.l2_district_name is null)
                    then 'new_match'
                when
                    run_rows.l2_district_name is not null
                    and prior_answer.l2_district_name is not null
                    and run_rows.l2_state = prior_answer.l2_state
                    and run_rows.l2_district_type = prior_answer.l2_district_type
                    and run_rows.l2_district_name = prior_answer.l2_district_name
                    then 'same_tuple'
                when run_rows.l2_district_name is not null and prior_answer.l2_district_name is not null
                    then 'moved'
                when run_rows.l2_district_name is null and prior_answer.l2_district_name is not null
                    then 'withdrawal'
                when run_rows.l2_district_name is null and prior_answer.br_database_id is not null
                    then 'still_abstained'
                else 'first_abstain'
            end as transition
        from run_rows
        left join geo_level on geo_level.br_database_id = run_rows.br_database_id
        left join state_vocab as sv on sv.state_postal_code = geo_level.office_state
        left join prior_answer on prior_answer.br_database_id = run_rows.br_database_id
    )

select rule_class, outcome, transition, count(*) as n
from labeled
group by rule_class, outcome, transition
order by rule_class, outcome, transition
```

Row-level drill-down: the identical statement above, with the final `select`
replaced by a filter to one `(rule_class, transition)` pair:

```sql
-- ... same `with` block as above ...
select br_database_id, office_state, l2_state, l2_district_type, l2_district_name, outcome
from labeled
where rule_class = '__RULE_CLASS__' and transition = '__TRANSITION__'
```

Read the printed rows, then interpret against these lines:

- The three abstain classes (`R0_party_committee`, `R1_judicial_abstain`,
  `R2_slice_zero_subtype_abstain`) must show ZERO matched rows: the code
  abstains before the LLM on those paths, so a match there means the run was
  made with different code than reviewed. Hard stop — but rule out input drift
  first: the classes are recomputed from TODAY's BR fields and universe, and a
  rebuild between the run and the audit (the cutover's own rebuild step) can
  legitimately reclassify an office. Check the drill-down rows' fields and
  their state's vocabulary against the run window before deleting anything; a
  post-hoc rule class is evidence of which branch ran, never a replay of it.
- `R2_whole_school_gated` offices matched to a school SUB-level type indicate
  the run had `--enable-school-whole-assertion` OFF (allowed only if that is
  what the operator intended; the flag is run config, not persisted, so this is
  how the audit infers the arm).
- Withdrawals concentrated in `R1_judicial_abstain` and
  `R2_slice_zero_subtype_abstain` are the filter design working as intended.
  Withdrawals in `pass_through` or a matched `R2_*` class are the ones to read
  row-by-row with the drill-down query above. The supervised cutover's review
  step counts served matches flipping to abstain — this is that count.

## Step 3 — Override suite (outside the gate)

```sql
with
    run_rows as (
        select br_database_id, l2_state, l2_district_type, l2_district_name
        from goodparty_data_catalog.model_predictions.llm_l2_br_match_results
        where attempted_at = timestamp'__RUN_KEY__'
    ),

    -- Minted 2026-map rows exist to compensate for a class the universe
    -- structurally cannot express, not to record a matcher disagreement;
    -- excluded before joining.
    active_overrides as (
        select br_database_id, state, l2_district_type, l2_district_name
        from goodparty_data_catalog.dbt.l2_br_match_overrides
        where l2_district_type not like '%\\_2026'
    ),

    joined as (
        select
            run_rows.br_database_id,
            run_rows.l2_state,
            run_rows.l2_district_type,
            run_rows.l2_district_name,
            active_overrides.state as override_state,
            active_overrides.l2_district_type as override_district_type,
            active_overrides.l2_district_name as override_district_name,
            case
                when run_rows.l2_district_name is null then 'abstained'
                when
                    lower(run_rows.l2_state) = lower(active_overrides.state)
                    and lower(run_rows.l2_district_type) = lower(active_overrides.l2_district_type)
                    and lower(run_rows.l2_district_name) = lower(active_overrides.l2_district_name)
                    then 'agree'
                else 'disagree'
            end as verdict
        from active_overrides
        inner join run_rows on run_rows.br_database_id = active_overrides.br_database_id
    )

select verdict, count(*) as n
from joined
group by verdict
order by verdict
```

Disagreement drill-down (identical statement, filtered):

```sql
-- ... same `with` block as above ...
select *
from joined
where verdict = 'disagree'
```

The suite is known-hard offices scored automatically and reported alongside the
holdout but OUTSIDE the gate: the serving path bypasses the matcher for every
one of them, so a disagreement is signal for review, never a stop.

## Step 4 — The holdout gate

The holdout is scored from DEDICATED arm artifacts, never from the audited
run's own rows: a production run reads the pending list, which structurally
excludes the holdout's served stratum, so exporting its rows would score every
served office as an abstention and produce a spurious served-gate FAIL. The
holdout owner's operator-local arms driver calls the matcher's `match_office`
directly over all 120 frozen offices and writes one COMPLETE answers JSON per
arm (`{br_database_id, l2_state, l2_district_type, l2_district_name,
confidence}`, nulls meaning abstain); the scorer refuses an answers file that
does not cover every scorable office. This step only RECORDS the arms'
verdicts against the run being audited — it does not produce them. Run once
per arm:

```bash
python .claude/skills/gold-match-run-audit/score_holdout.py \
  --truth <adjudicated-holdout-packet.csv> \
  --answers <this-run's-answers.json> \
  --label "<run key>, <arm>"
```

The packet CSV lives in the holdout owner's working directory (it is operator-local, not
committed); it carries the frozen draw's `stratum`/`cell` columns, the `jan_*` baseline
columns, and the owner-ruled `truth_*` columns the scorer reads.

Omit `--answers` to print January's own report straight from the packet's
`jan_*` columns (the baseline).

The ratified rule, quoted verbatim so nobody re-derives it: **strict
superiority on the backlog 72 and at most 2 net regressions on the served 48, a
wrong match scoring below an abstain.** `score_holdout.py` encodes that once;
read its printed verdict rather than re-deriving PASS/FAIL from the table by
eye.

## Step 5 — Web spot-check

No code. Sample ~15 matched rows under the key from Step 2, stratified by rule
class, with at least 3 from `R2_whole_school_gated` when the run produced any.
Verify each the way the holdout was adjudicated — establish the office's real
electorate first (statute, charter, municipal code, the jurisdiction's own site;
a residency requirement is not an electorate), then check the matched tuple is
that electorate's row (the full method contract lives in the holdout owner's
working directory) — using the ddhq-miss-audit fan-out pattern and its cost rule
(`.claude/skills/ddhq-miss-audit/SKILL.md` Step 3): small model by default for
the fan-out, escalate only a batch a spot-check shows wrong, never the
orchestrator's own model. A confirmed wrong match is evidence for the owner's
review, not an automatic stop.

## Step 6 — Sign-off checklist

Restate only the hard conditions, each naming where it was measured:

- [ ] Batch count reconciles (Step 0's operator count vs Step 1's `rows_under_key`).
- [ ] `run_label_check_missing` is zero (Step 1) — a nonzero here is THIS run's
  hard stop.
- [ ] `label_check_warn_count` is zero before release (Step 1) — zero POST-baseline
  dead tuples, the warn test's own scope. January-origin dead labels are the
  pending backlog, deliberately out of scope, and join nothing while they wait.
  A nonzero with a zero run-scoped count is repaired at its SOURCE run, never
  by deleting this one.
- [ ] Zero CONFIRMED matched-row violations in `R0_party_committee`,
  `R1_judicial_abstain`, and `R2_slice_zero_subtype_abstain` after Step 2's
  input-drift review (a reclassification caused by BR/universe drift between
  run and audit is not a violation).
- [ ] Coverage ratio clears `assert_position_district_voter_coverage_floor.sql`'s
  floor (Step 1).
- [ ] Withdrawal count in `pass_through` and matched `R2_*` classes reviewed by
  the owner (Step 2).
- [ ] Holdout gate verdict is PASS **for the arm this run actually used**
  (operator-confirmed `--enable-school-whole-assertion` state; the flag is not
  persisted) — the other arm's PASS does not transfer, and a FAIL stops the
  cutover rather than being satisfied by recording it; supervised cutover only
  (Step 4).
