---
name: audit-er-results
description: Audit entity resolution match results for quality — analyze coverage, false positives, and false negatives
user_invocable: true
---

# Audit Entity Resolution Results

After an ER match run, audit the results for match quality. Work from the
`entity_resolution/` directory. Default paths assume `data/input.csv` for
input and `results/` for outputs, but the user may override these.

Run each step below sequentially. Read each output CSV after running the
script, then interpret the results before moving to the next step.

## Step 1: Summary Statistics

```bash
cd /Users/danball/projects/gp-data-parent/gp-data-platform/entity_resolution
uv run python scripts/audit_summary.py --input data/input.csv --results-dir results/
```

Read `results/audit_summary.csv` and interpret the terminal output. Flag:
- Any provider with a match rate below 50% (may indicate missing blocking rules)
- Any provider with a match rate above 95% (may indicate over-matching)
- Cluster sizes > 4 (likely false-positive chains)
- Within-source duplicate clusters (should not exist in link_only mode)

## Step 2: Low-Confidence Match Review

```bash
uv run python scripts/audit_low_confidence.py --results-dir results/ --sample 20
```

This finds the 20 pairs closest to 0.5 match probability — the model's most
ambiguous decisions. Read `results/audit_low_confidence.csv`. For each pair:
1. Look at the side-by-side display columns (name, office, state, etc.)
2. Look at the gamma values to understand which comparisons agreed/disagreed
3. Assess whether this looks like a true match or false positive
4. Note the pattern — what's driving the ambiguity?

Summarize your findings as:
- Common patterns in true matches that scored low (model is underweighting something)
- Common patterns in false positives that scored mid-range (model is overweighting something)
- Specific comparison columns that are noisy or unhelpful

## Step 3: False Negative Review

```bash
uv run python scripts/audit_false_negatives.py --input data/input.csv --results-dir results/ --sample 20
```

Read `results/audit_false_negatives.csv`. For each suspicious non-match:
1. Check the `was_in_pairwise_predictions` column:
   - If **True**: the blocking rules generated the pair but the model scored it too low
     or the person/race filter removed it. Investigate which gammas drove the low score.
   - If **False**: the blocking rules never generated this pair. Identify which blocking
     rule *should* have caught it and why it didn't.
2. Look for systematic patterns:
   - Are all misses from one specific provider?
   - Do they share a common data quality issue (missing field, different formatting)?
   - Is there a name variant pattern the model isn't handling?

## Step 4: Recommendations

Based on all three steps, compile actionable recommendations. Organize them as:

**Blocking rule changes** (pairs never generated):
- New blocking rules needed
- Existing rules that are too restrictive

**Comparison/threshold changes** (pairs generated but misscored):
- Comparison columns to add, remove, or re-threshold
- Term frequency adjustments that might help

**Data quality issues** (fix upstream in the prematch dbt model):
- Missing fields for specific providers
- Formatting inconsistencies across providers
- Schema mapping issues

**Post-prediction filter changes**:
- Person identity filter adjustments
- Race-level filter adjustments

Present these recommendations to the user and ask before making any changes.
If the user wants to proceed with changes, edit the relevant files
(`scripts/initial_match.py` for model config, or the dbt prematch model for
data quality fixes).
