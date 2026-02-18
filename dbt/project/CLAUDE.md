## Helpful Commands

```
# dbt (must be run from in dbt/project/ directory
poetry install && eval $(poetry env activate)
dbt run                               # Run transformations
dbt test                              # Data quality tests
dbt build                             # Run + test

# Inpect models/sources:
dbt run-operation inspect_data --args '{"model": "model_name"}'
```

**IMPORTANT** - When working on dbt models, it may be helpful to inspect existing sources/models in Databricks, as well as models that you have added and modified after creating them. Do so by running the `inspect_data` utility macro:

Output includes:
  - Relation name and type
  - Total row count
  - Column details table (name, data type, non-null count, % populated)
  - Sample data rows

```
# For models
dbt run-operation inspect_data --args '{"model": "my_model_name"}'

# For sources
dbt run-operation inspect_data --args '{"source_name": "airbyte_source", "table_name": "gp_api_db_campaign"}'

# With custom sample size
dbt run-operation inspect_data --args '{"source_name": "airbyte_source", "table_name": "gp_api_db_user", "sample_size": 10}'
```
## Conventions

- Most dbt tests do *not* need a `config.where: some_column_is_not_null`. For
  instance, accepted_values tests will work fine with nulls without this
  config.
- For boolean columns, use the boolean value explicitly. Don't do a comparison.
    - Good: `where is_valid and not is_expired`
    - Bad: `where is_valid = 'true' and is_expired = 'false'
- Databricks column references are case insensitive, so you don't need to
  include backticks for mixed-case column names

## Workflow

Use the `gh` cli to make pull-requests.

### Building and Testing Models

When creating or modifying dbt models, always follow this workflow:

1. **Build the model** — Run `dbt run --select model_name` to materialize it.
2. **Inspect the data** — Run `dbt run-operation inspect_data --args '{"model": "model_name"}'` to review row counts, column types, null rates, and sample rows.
3. **Write tests informed by the data** — Use the inspect output to decide which tests are appropriate:
   - Add `not_null` tests to columns that are 100% populated and should stay that way (primary keys, foreign keys, core identifiers, boolean flags).
   - Add `unique` tests to primary key columns.
   - Add `accepted_values` tests to columns with a known set of valid values (e.g. status, result, stage columns). Note: `accepted_values` already skips nulls by default — do not add a redundant `config.where: "column is not null"`.
   - Add `dbt_utils.accepted_range` tests for numeric columns that have a logical bound (e.g. `voter_count >= 0`).
   - Add `dbt_utils.expression_is_true` for row-level invariants (e.g. `updated_at >= created_at`).
   - Do not add tests that are likely to fail on real data (e.g. cross-column ordering assumptions that may not hold for all rows).
4. **Run all tests** — Run `dbt test --select model_name` and confirm everything passes before committing.

## Naming Conventions

### Mart Models
Mart model filenames should use **domain-friendly names** that end users will understand. Do **not** prefix mart models with `m_`. Use plain, descriptive names like `candidacies.sql`, `candidates.sql`, `user_signups_by_month.sql`, etc.

### Other Layers
- **Staging models**: `stg_<source>__<table>.sql` (e.g., `stg_airbyte_source__gp_api_db_user.sql`)
- **Intermediate models**: `int__<domain>_<object>_<year>.sql` (e.g., `int__civics_candidacy_2025.sql`)

## Terminology Definitions

### Key Terms
**Candidate**
*   An individual person who runs or has run for office
*   Example: "Jon Smith"
**Candidacy**
*   A specific instance of a candidate running for a particular position in a particular year
*   Example: "John Smith for Seattle Mayor 2026"
*   A Candidacy comprises a Candidate and an Election
**Election**
*   The full election cycle for a specific position in a specific year
*   Encompasses all stages (primary, general, runoff)
*   Example: "Seattle Mayor 2026" (the entire election)
*   An Election comprises multiple Stages
**Election-Stage**
*   A single phase within an election (primary, general, or runoff)
*   Example: "Seattle Mayor 2026 Primary" or "Seattle Mayor 2026 General"
*   Values: Primary, General, Runoff
**Candidacy-Stage**
*   The intersection of a candidacy and a specific stage
*   Contains vendor-specific IDs and stage-specific results
*   Example: "John Smith for Seattle Mayor 2026, Primary Results"
*   A Candidate Stage comprises a Candidate and a Stage

## Table Structure in Civics Mart
### Core Tables
#### 1\. **_Candidate_** **Table**
*   **Granularity:** One row per unique person
*   **Purpose:** Store individual candidate information
#### 2\. **_Candidacy_** **Table**
*   **Granularity:** One row per unique candidacy (person + election + year)
*   **Purpose:** Track when a candidate runs for a specific position/office in a specific year
*   **Example:** John Smith running for Seattle Mayor in 2026 = 1 row
*   **Note:** This allows easy counting of total candidacies without needing DISTINCT operations
#### 3\. **_Candidacy-Stage_** **Table**
*   **Granularity:** One row per candidacy stage
*   **Purpose:** Store detailed results and vendor IDs for each election stage
    *   **Includes:**Ballot Ready stage ID: `br_candidacy_id`
    *   DDHQ stage ID
    *   Stage-specific results and data
*   **Example:** John Smith's 2026 mayoral candidacy would have separate rows for:
    *   Primary
    *   General
    *   Runoff (if applicable)
#### 4\. **_Election_** **Table**
*   **Granularity:** One row per election (full election cycle)
*   **Purpose:** Represent the complete election (all stages combined)
*   **Example:** "Seattle Mayor 2026" = 1 row encompassing primary, general, and any runoffs
#### 5\. _Election-_**_Stage_** **Table**
*   **Granularity:** One row per election stage
*   **Purpose:** Break out individual stages of an election
*   **Example:** "Seattle Mayor 2026 Primary", "Seattle Mayor 2026 General" = separate rows
#### 5\. **_Voter_** **Table**
*   **Granularity:** ..
*   **Purpose:** To provide the historical voting records on individual voter basis
*   **Example**


## ID Mappings

| Defintion/<br>Level | GoodParty Mart Term (int) | BallotReady Term (ext) | DDHQ Term (ext) | HubSpot Term<br>(int) | Product DB Term (int) |
| ---| ---| ---| ---| ---| --- |
| Candidate | gp\_candidate\_id | [person\_id](https://developers.civicengine.com/docs/api/graphql/reference/objects/person) | candidate\_id | N/A | db\_user\_id |
| Candidacy | gp\_candidacy\_id |  | ? | hs\_contact\_id | campaign\_id |
| Election | gp\_election\_id | No direct equivalent but [position\_id](https://developers.civicengine.com/docs/api/graphql/reference/objects/position) closest | ? | N/A | N/A |
| Stage | gp\_election\_stage\_id | [race\_id](https://developers.civicengine.com/docs/api/graphql/reference/objects/race) | ddhq\_race\_id | N/A | N/A |
| Candidacy-Stage | gp\_candidacy\_stage\_id | [candidacy\_id](https://developers.civicengine.com/docs/api/graphql/reference/objects/candidacy) | candidacy\_id<br> | N/A | N/A |

### Internal IDs Mapping
*   **Product**
    *   `candidate.product_user_id` | Provides an candidate-level link to a `user` in the _Win_ product
    *   `candidacy.product_campaign_id` | Provides a candidacy-level link to a `campaign` in the _Win_ product
*   **HubSpot**
    *   `candidate.hs_contact_id` | Provides a candidate-level link to a `contact` in HubSpot
    *   `candidacy.hs_company_id` | Provides a candidacy-level link to a `company` in HubSpot
### Vendor IDs Mapping
*   **Ballot Ready:**
    *   `candidacy_stage.br_candidacy_id` | Provides candidacy-stage-level IDs for BallotReady Data
    *   `election_stage.br_race_id` | Provides election-stage ID for BallotReady Data
*   **DDHQ:**
    *   `candidacy_stage.ddhq_candidacy_id` | Provides candidacy-stage-level IDs for DDHQ Data
    *   `election_stage.ddhq_race_id` | Provides election-stage ID for DDHQ Data


## Useful Links
[BallotReady Documentation](https://developers.civicengine.com/docs/api/graphql)

## Example schema for each of the 5 above tables

These may be expanded and/or denormalized per the "one big table" strategy as outlined above:

| Field Name | Display Name | Description | Data Type | tests |
|------------|--------------|-------------|-----------|-------|
| gp_candidacy_stage_id | GP Candidacy Stage ID | Primary key - unique identifier for candidacy stage | uuid (pk) | not-null |
| br_candidacy_id | Ballot Ready Candidacy ID | Ballot Ready candidacy ID | varchar | |
| ddhq_candidacy_id | DDHQ Candidacy ID | DDHQ candidacy ID | varchar | |
| gp_candidacy_id | GP Candidacy ID | Foreign key to candidacy table | uuid (fk) | not-null |
| gp_election_stage_id | GP Election Stage ID | Foreign key to election_stage table | uuid (fk) | not-null |
| candidacy_stage_result | Candidacy Stage Result | Result for this candidacy in this stage | varchar | |
| hubspot_contact_id | HubSpot Contact ID | The Contact ID number as it appears in HubSpot | int | |
| ddhq_candidate | DDHQ Candidate | DDHQ candidate name | varchar | |
| ddhq_candidate_id | DDHQ Candidate ID | DDHQ candidate ID | int | not-null |
| ddhq_race_id | DDHQ Race ID | DDHQ race ID | int | not-null |
| ddhq_candidate_party | DDHQ Candidate Party | DDHQ candidate party | varchar | |
| ddhq_is_winner | DDHQ Is Winner | DDHQ winner flag | boolean | |
| ddhq_llm_confidence | DDHQ LLM Confidence | LLM matching confidence score | double | |
| ddhq_llm_reasoning | DDHQ LLM Reasoning | LLM matching reasoning | varchar | |
| ddhq_top_10_candidates | DDHQ Top 10 Candidates | Top 10 candidates from DDHQ | varchar | |
| ddhq_has_match | DDHQ Has Match | Whether DDHQ match was found | boolean | |
| votes_received | Votes Received | Number of votes received in this stage | int | |
| vote_percentage | Vote Percentage | Percentage of votes received in this stage | double | |
| created_at | Created At | Record creation timestamp | timestamp | |
| updated_at | Updated At | Record last update timestamp | timestamp | |
