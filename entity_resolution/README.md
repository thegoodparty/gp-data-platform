# Entity Resolution: BallotReady x TechSpeed Candidacies

Splink-based probabilistic record linkage to match candidacy records across
BallotReady (BR) and TechSpeed (TS). TechSpeed receives BallotReady race data,
enhances some records (adding phone/email), and also discovers "net new"
candidates BallotReady doesn't have yet. The overlap means we need entity
resolution to avoid duplicates before combining both sources in the civics mart.

## Quick start

```bash
cd entity_resolution
uv run python scripts/cli.py match --input data/input.csv
```

Or with a custom input file and output directory:

```bash
uv run python scripts/cli.py match --input /path/to/prematch.csv --output-dir /path/to/output/
```

**Input:** CSV file exported from `dbt_dball.int__er_prematch_candidacy_stages`
**Output:**
- `results/pairwise_predictions.csv` — all scored candidate pairs
- `results/clustered_candidacies.csv` — all records with cluster assignments
- `results/match_weights_chart.html` — Splink match weight visualization
- `results/m_u_parameters_chart.html` — learned m/u probability visualization

## Design: candidate-level vs race-level attributes

Attributes are divided into two categories based on how they contribute to
scoring:

**Candidate-level attributes** (strongest Splink comparisons — drive the match
score):
- `last_name`, `first_name`, `party`, `email`, `phone`

**Race/election-level attributes** (used in both blocking rules and as Splink
comparisons, but guarded by post-prediction filters to prevent false positives):
- `state`, `official_office_name`, `election_date`, `district_identifier`
- `br_race_id` — used in blocking only

**Additional retained columns** (carried through for filtering and output but
not used as comparisons):
- `office_type`, `candidate_office`, `office_level`,
  `district_raw`, `seat_name`, `election_stage`, `br_candidacy_id`

### Why race-level attributes need post-prediction guards

Multiple candidates run in the same race. Race-level attributes like
`official_office_name`, `election_date`, and `state` produce positive Bayes
factors for *any* pair of candidates in the same race — which can overwhelm
name-mismatch penalties. The post-prediction filter (described below) catches
these cases by requiring name agreement and office/race consistency.

## How it works

The script uses [Splink 4](https://moj-analytical-services.github.io/splink/)
in `link_only` mode (cross-source matching, no within-source dedup) with
DuckDB as the backend.

### Preprocessing

Most data cleaning is handled upstream in the dbt prematch model
(`int__er_prematch_candidacy_stages`). The Python script performs only:

- **First name nicknames:** the dbt model maps each first name to an alias
  array via the `nicknames` seed (e.g. robert -> [robert, bob, bobby, rob,
  bert, ...]). The array always includes the original first name. The script
  parses these JSON arrays so Splink's `ArrayIntersectLevel` can check for
  overlap, recognizing "robert" and "bob" as potential matches without
  requiring exact string similarity.
- **Nulls:** literal `"null"` strings, empty strings, and `NaN` are all
  converted to `None` so Splink treats them as missing data

The following are handled in dbt (not in the Python script):
- **Names:** lowercased and trimmed
- **`official_office_name`:** lowercased and trimmed
- **`district_identifier`:** cast to int (normalizes leading zeros)
- **`br_race_id`:** cast to int (non-integer values like
  `ts_found_race_net_new` become null so the blocking rule only fires for
  records with a shared race ID)

### Blocking rules (which pairs to compare)

Blocking rules determine which record pairs are generated for scoring. Splink
unions the pairs from each rule, deduplicating. All rules enforce race-level
constraints so that only candidates plausibly in the same race are compared.

| Order | Rule | Purpose |
|-------|------|---------|
| 1 | `br_race_id` (exact) | High-cardinality first pass. Pairs TS records with BR records in the same race. Covers the majority of matches. |
| 2 | `state + election_date + office_name (JW >= 0.88) + last_name` (exact) | Catches cross-source office formatting differences (e.g. "republic city council - ward 3" vs "republic city ward 3") for records without a shared race ID. |
| 3 | `state + last_name + election_date` (exact) | Broad catch-all for net-new TS records and cases not covered by race ID or office name. |
| 4 | `state + election_date + office_name (JW >= 0.88) + last_name (JW >= 0.88)` | Catches last name typos/variants across sources with different office formatting. |
| 5 | `phone` (exact) | Contact-info matches where names may differ. |
| 6 | `email` (exact) | Contact-info matches where names may differ. |

Rules 2 and 4 use DuckDB's `jaro_winkler_similarity` function via Splink's
`CustomRule` for fuzzy blocking.

### Comparisons (how pairs are scored)

All comparisons contribute Bayes factors to the match score:

| Column | Type | Levels | Notes |
|--------|------|--------|-------|
| `last_name` | Jaro-Winkler | exact, >= 0.95, >= 0.88, else | Term frequency adjusted (down-weights common surnames) |
| `first_name` | Custom | exact -> nickname -> JW >= 0.92 -> else | Nickname match via alias array intersection; TF adjusted on exact |
| `party` | Exact | match, else | |
| `email` | Exact | match, else | |
| `phone` | Exact | match, else | |
| `state` | Exact | match, else | |
| `election_date` | Exact | match, else | |
| `official_office_name` | Jaro-Winkler | exact, >= 0.95, >= 0.88, >= 0.75, else | 0.75 tier catches cross-source formatting (e.g. "durham school board" vs "durham county board of education") |
| `district_identifier` | Exact | match, else | Numeric district; provides positive/negative Bayesian evidence |

### Training

Four EM passes with different blocking ensure all comparison columns get
trained. Each pass blocks on one or more columns (fixing them) and estimates
m probabilities for the rest:

1. Block on `last_name + state + election_date` -> trains first_name, party, email, phone, official_office_name, district_identifier. Blocking on all three prevents EM contamination from same-race different-person pairs that would inflate the first_name non-agreement m probability.
2. Block on `first_name` -> trains last_name, party, email, phone, state, election_date, official_office_name, district_identifier
3. Block on `email` -> trains last_name, first_name, party, phone, state, election_date, official_office_name, district_identifier
4. Block on `state + election_date` -> trains last_name, first_name, party, email, phone, official_office_name, district_identifier

u probabilities are estimated via random sampling (5M pairs) before EM.

### Post-prediction filters

After Splink scores all blocked pairs, three filters ensure we only cluster
true candidacy matches (same person + same office + same election):

1. **Person identity filter** — requires last name agreement (gamma > 0) AND
   first name agreement OR email/phone match. Removes same-race,
   different-candidate pairs.

2. **Race-level filter** — requires `official_office_name` JW >= 0.75
   (gamma > 0). The 0.75 threshold is high enough to exclude completely
   different offices (JW 0.60-0.72) while catching cross-source formatting
   differences for the same office (JW 0.76+).

3. **Race ID filter** — excludes pairs where both sides have a known integer
   `br_race_id` and they differ, **unless** the office names match well
   (JW >= 0.88). BR and TS sometimes assign different race IDs to the same
   race, so a strong office name match overrides the race ID disagreement.

### Thresholds

- **Prediction threshold: 0.01** — low threshold to capture all plausible pairs
  for the post-prediction filters to evaluate
- **Clustering threshold: 0.95** — high confidence required to cluster, since
  the unit of matching is a *candidacy* (person + office + election date), not
  just a person

## Edge cases this handles

### Last name typos across sources

The fuzzy last name blocking rule (JW >= 0.88) ensures these pairs are
generated even when names don't match exactly:

| BR record | TS record | Match prob |
|-----------|-----------|------------|
| phillip **whitaker** (fort smith school board - zone 1) | phillip **whiteaker** (fort smith public school district zone 1) | 0.92 |
| joe **montelone** (green park city mayor) | joe **monteleone** (green park city mayor) | 0.72 |
| bob **feidler** (st. croix county board - dist 9) | bob **fiedler** (chenequa village board) | 0.83 |
| amanda **fuerst** (wauwatosa city council - dist 10) | amanda **fuers** (wauwatosa city council - dist 10) | 0.84 |
| emily **bassham** (mountainburg school board - zone 2) | emily **basham** (mountainburg school district, zone 2) | 0.88 |

### Cross-source office name formatting

BallotReady and TechSpeed often format the same office differently. The fuzzy
office blocking rule (JW >= 0.88) handles most cases. The 0.75 JW tier in the
comparison catches reformatted office names that fall below 0.88:

| BR format | TS format | JW | Mechanism |
|-----------|-----------|-----|-----------|
| `fort smith school board - zone 1` | `fort smith public school district zone 1` | 0.89 | Office JW >= 0.88 |
| `mountainburg school board - zone 2` | `mountainburg school district, zone 2` | 0.89 | Office JW >= 0.88 |
| `durham school board - district 4` | `durham county board of education district 04` | 0.87 | Office JW >= 0.75 |
| `university of nebraska board of regents - district 1` | `nebraska board of regents - district 01` | 0.76 | Office JW >= 0.75 |
| `lake mills city council - district 1` | `city of lake mills council member- district 1` | 0.79 | Office JW >= 0.75 |

### First name nicknames

The alias array intersection catches nickname matches that string similarity
would miss:

| BR name | TS name | Mechanism |
|---------|---------|-----------|
| robert smith | bob smith | alias arrays both contain "bob" and "robert" |
| william jones | bill jones | alias intersection |
| james wilson | jim wilson | alias intersection |

### Same person, different office (correctly separated)

The race-level filter prevents matching a person who runs for two different
offices (e.g. mayor and city council) in the same city:

| Candidate A | Candidate B | Matched? |
|-------------|-------------|----------|
| dean isgrigg, gerald city council - ward 2 | dean isgrigg, gerald city mayor | No (City Council != Mayor) |

Note: candidates running for multiple offices in the same election can still
end up in the same cluster if intermediate pairs chain them together. For
example, John Muraski's howard village board and howard village president
records are clustered together via transitive links through cross-source pairs.

### Same race, different candidates (correctly separated)

Two different candidates running in the same race share office, state, date,
and district — but the person identity filter separates them:

| Candidate A | Candidate B | Matched? |
|-------------|-------------|----------|
| joel straub, marathon county board dist 15 | timothy sondelski, marathon county board dist 25 | No (different names) |
| clark rinehart, raleigh city council | sana siddiqui, raleigh city council | No (different names) |

### Known false negatives

A small number of true matches are systematically missed:

1. **Office name JW < 0.75 (~5 pairs):** Extreme cross-source formatting
   differences push JW below the 0.75 threshold. The main example is
   TechSpeed's "city of norman councilmember councilmember ward N (unexpired)"
   vs BR's "norman city council - ward N" (JW = 0.65), caused by a duplicated
   "councilmember" in the TS data.

2. **Uncommon nicknames not in the nicknames seed (~60 pairs sharing
   `br_race_id` + `last_name`):** The nickname alias table doesn't cover
   informal or uncommon variants. Examples:
   - `barb` / `barbara`, `samara` / `sammie`, `keisha` / `lakeisha`
   - `a.j.` / `a.` (initial/period handling)
   - `fee fee` / `iphenia`, `clutch` / `claude` (exotic nicknames)

## Current results (42,249 input records)

| Metric | Value |
|--------|-------|
| Input records | 22,925 BR + 19,324 TS |
| Pairwise pairs above 0.01 | 17,112 |
| Removed by post-prediction filters | 12,327 |
| Cross-source matched clusters | 4,685 |
| Within-source duplicate clusters | 0 |

## Diagnostic charts

### Match weights

Shows how much each comparison column contributes to the overall match score.
Bars to the right indicate evidence *for* a match when columns agree; bars to
the left indicate evidence *against* when they disagree.

![Match weights chart](results/match_weights_chart.png)

<sub>[Interactive version](results/match_weights_chart.html)</sub>

### M/U parameters

Shows the learned probability distributions for each comparison level. The **m
probability** is the chance two records agree on a column *given they are a true
match*; the **u probability** is the chance they agree *given they are not a
match*. Columns where m is high and u is low are the most discriminating.

![M/U parameters chart](results/m_u_parameters_chart.png)

<sub>[Interactive version](results/m_u_parameters_chart.html)</sub>

## Next steps

### Productionization
- Convert Splink logic into a **dbt Python model** on Databricks. Reference
  pattern: `int__techspeed_candidates_fuzzy_deduped.py`
- Output table (`int__er_match_candidacies`) should contain `er_cluster_id`,
  source IDs, `match_probability`, and all prematch columns
- Update `marts/civics/candidacy.sql` to union BR + TS candidacies, join ER
  clusters, and deduplicate (BR wins most fields, TS wins phone/email)
