# Analysis brief schema

Part of the **analytics-process** skill. The framing→execution handoff contract.

When the framing routine (see [framing.md](framing.md)) finishes shaping a question, it produces a
structured analysis brief. This brief is the handoff artifact to the executor (Claude Code, or
whoever runs the analysis). The format below is the contract. See [pipeline.md](pipeline.md)
for where the brief sits in the overall flow.

Every brief must use this template. Don't omit sections; if a section doesn't apply, write
"N/A" with a one-line explanation.

```yaml
brief_id: short-kebab-case-identifier  # e.g., messaging-tool-win-rate-2026q2
created: YYYY-MM-DD
author: framing routine (refined with <username>)

decision:
  what_action: |
    What will be done differently based on the result.
  who_acts: |
    Which team or role takes that action.

question:
  one_sentence: |
    The sharp version of the question, stated in one sentence.
  type: causal | correlational | descriptive
  underlying_hypothesis: |
    What the user actually believes and wants to test.

population:
  included: |
    Precise definition of who's in scope.
  excluded: |
    Cohorts explicitly removed (demo accounts, internal users, out-of-scope geo, etc.).
  source_model: |
    The dbt model or table this population is drawn from.

data_provenance:
  schema_status: prod | dev | pending_merge
  coverage_start: |
    Actual MIN of the time column in the source table, verified by query
    (not the brief's assumed floor). E.g., MIN(week_start_date) from
    int__amplitude_win_activity_weekly.
  coverage_end: |
    Actual MAX of the time column.
  post_merge_swap: |
    If schema_status = dev or pending_merge: what to change after merge
    (e.g., "swap WIN_ACTIVITY_WEEKLY_SCHEMA from 'private_tristan' to 'dbt'").

eligibility:
  tenure_requirement: |
    Minimum observability window before the reference event.
  reference_event: |
    The anchor date (election date, signup date, feature launch, etc.).
  rationale: |
    Why this eligibility rule exists.

target:
  definition: |
    Precise outcome variable, including units.
  censoring: |
    How incomplete observations are handled.
  absorbing_states: |
    How terminal states (won, lost, dropped, deleted) are handled.

comparison:
  type: treatment_vs_control | correlation | descriptive_cut | other
  comparison_group: |
    What the treated group is being compared to.
  notes: |
    Any caveats about comparability (selection, confounding).

observation_window:
  start: |
    Date or rule for window start.
  end: |
    Date or rule for window end.
  anchored_to: |
    The reference event the window is relative to.

cohorts:
  - dimension: e.g., position_type
    values: |
      Which values to break out (or "all observed").
  - dimension: ...

sample_size:
  expected_n_total: |
    Order of magnitude.
  expected_n_per_cohort: |
    Rough breakdown.
  power_concern: |
    Note if any cohort is likely underpowered for the effect of interest.

falsification:
  what_would_change_belief: |
    The result that would make the user update.

known_concerns:
  - |
    Concerns raised during framing that the user chose to proceed past.
  - |
    Limitations of the data or design.

execution_notes:
  preferred_format: notebook  # default
  output_location: |
    Where the executed analysis should be saved.
  reruns: |
    Should this be a one-off or set up for repeated runs?
```

## Notes on using briefs

- The executor should treat the brief as a spec. If something in the brief is ambiguous or unworkable on inspection of the actual data, return to the framing step (re-run [framing.md](framing.md)) rather than improvising.
- After execution, `product-data-scientist` reviews the notebook against the brief — both for methodological soundness and to interpret what the results mean.
- Briefs are durable: save them alongside the executed notebook so the framing is retrievable later.

## Amending a brief vs re-framing

When the owner wants to modify the question after a brief exists, decide which path applies:

- **Amend (no new framer round).** The change is an additional stratification or slice on the *same* population, metric, and comparison — e.g. "also cut by ICP vs not." Append the new dimension to the brief's `cohorts` section and re-slice. If the working set was built via `analytics/lib` carrying the standard dimensions from the **win-analytics-knowledge** skill's [segmentation.md](../../win-analytics-knowledge/references/segmentation.md), this is a zero-query pandas `groupby` (see [methodology.md](methodology.md)). Note the amendment in the brief so it stays the source of truth.
- **Re-frame (new framing round + brief revision).** The change touches **population, eligibility, target metric, or comparison** — e.g. "filter to ICP only" (not slice), switch the outcome variable, or change the cohort window. These are exactly what framing owns, so re-run [framing.md](framing.md) for a revised brief.

Decision rule: if population/eligibility/target/comparison are unchanged and you're only adding a breakdown, it's an amend. Otherwise re-frame. (Example: ICP vs not = amend; ICP-only = re-frame.)

## Cross-references

- [pipeline.md](pipeline.md) — the stages this brief flows through.
- [methodology.md](methodology.md) — scoping and the build-once-slice-many pattern; the product's default cohorts are in the product knowledge skill's `methodology_defaults.md`.
