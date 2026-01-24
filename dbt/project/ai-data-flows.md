# AI-Powered Data Matching Workflows

This document provides a comprehensive overview of the AI/LLM-powered data matching systems used at GoodParty to connect candidate, election, and voter data across multiple external sources.

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [System Architecture Overview](#system-architecture-overview)
3. [Match Type 1: HubSpot to DDHQ (Election Results)](#match-type-1-hubspot-to-ddhq-election-results)
4. [Match Type 2: L2 Districts to BallotReady Positions](#match-type-2-l2-districts-to-ballotready-positions)
5. [Match Type 3: Zip Code to L2 District (dbt-native)](#match-type-3-zip-code-to-l2-district-dbt-native)
6. [Data Flow Diagrams](#data-flow-diagrams)
7. [Current Performance Characteristics](#current-performance-characteristics)
8. [Opportunities for Optimization](#opportunities-for-optimization)

---

## Executive Summary

GoodParty uses three distinct data matching pipelines to connect information across disparate data sources:

| Match Type | Purpose | Current Approach | Location |
|------------|---------|------------------|----------|
| **HubSpot → DDHQ** | Link GoodParty candidates to official election results | Gemini Embeddings + FAISS + LLM Validation | `gp-ai-projects/hubspot_ddhq_match/` |
| **L2 → BallotReady** | Connect L2 voter districts to BallotReady office positions | Gemini Embeddings + Cosine Similarity + LLM Selection | `gp-ai-projects/stitch_golden_data/` |
| **Zip → L2 District** | Map zip codes to electoral districts | Direct join on L2 voter file data | `dbt/project/models/intermediate/l2/` |

All AI-powered matching currently runs **external to Databricks** in Python processes (local or AWS ECS), with results manually or programmatically loaded into the `model_predictions` schema for consumption by dbt models.

---

## System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           EXTERNAL COMPUTE (gp-ai-projects)                          │
│                                                                                      │
│   ┌─────────────────────────┐         ┌─────────────────────────────────────────┐   │
│   │  hubspot_ddhq_match/    │         │  stitch_golden_data/prod_gold_data/     │   │
│   │                         │         │                                         │   │
│   │  Python + FAISS + LLM   │         │  Python + Vector Store + LLM            │   │
│   │  (AWS ECS or Local)     │         │  (Local execution)                      │   │
│   └───────────┬─────────────┘         └──────────────┬──────────────────────────┘   │
│               │                                      │                               │
│               ▼                                      ▼                               │
│   ┌─────────────────────────┐         ┌─────────────────────────────────────────┐   │
│   │  S3: parquet output     │         │  Local: parquet files                   │   │
│   │  s3://ddhq-matcher-...  │         │  stitch_golden_data/output/             │   │
│   └───────────┬─────────────┘         └──────────────┬──────────────────────────┘   │
│               │                                      │                               │
└───────────────┼──────────────────────────────────────┼───────────────────────────────┘
                │         MANUAL/SCHEDULED UPLOAD      │
                ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              DATABRICKS (model_predictions schema)                   │
│                                                                                      │
│   candidacy_ddhq_matches_YYYYMMDD          llm_l2_br_match_YYYYMMDD                 │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
                │                                      │
                ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              DBT PROJECT (transforms & marts)                        │
│                                                                                      │
│   Staging:     stg_model_predictions__candidacy_ddhq_matches_YYYYMMDD               │
│                stg_model_predictions__llm_l2_br_match_YYYYMMDD                       │
│                                                                                      │
│   Intermediate: int__gp_ai_election_match (consumes DDHQ matches)                   │
│                 int__zip_code_to_br_office (consumes L2-BR matches)                 │
│                                                                                      │
│   Marts:        m_general__candidacy, m_election_api__district, civics tables       │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Match Type 1: HubSpot to DDHQ (Election Results)

### Purpose

Match GoodParty candidate records (stored in HubSpot) to official election results from Decision Desk HQ (DDHQ) to determine win/loss outcomes for tracked candidates.

### Data Sources

| Source | Table/Location | Description |
|--------|----------------|-------------|
| **Input: HubSpot Candidates** | `dbt.m_general__candidacy` | GoodParty's candidate records with name, office, election date |
| **Input: DDHQ Results** | `dbt.stg_airbyte_source__ddhq_gdrive_election_results` | Official election results with candidate names, race info, winner flags |
| **Output: Match Results** | `model_predictions.candidacy_ddhq_matches_YYYYMMDD` | Matched pairs with confidence scores |

### Pipeline Steps

#### Step 1: Data Extraction (`data_extraction.py`)

**Location:** `gp-ai-projects/hubspot_ddhq_match/data_extraction.py`

**Process:**
- Connects to Databricks via SQL connector
- Pulls HubSpot candidacy data from `dbt.m_general__candidacy`
- Pulls DDHQ election results from `dbt.stg_airbyte_source__ddhq_gdrive_election_results`
- Saves both datasets as parquet and TSV files locally

**Output:** `offline_data/hubspot_candidacy_latest.parquet`, `offline_data/ddhq_election_results_latest.parquet`

#### Step 2: Data Cleaning (`data_cleaning.py`)

**Location:** `gp-ai-projects/hubspot_ddhq_match/data_cleaning.py`

**Process:**
- Standardizes name formatting (proper case, removes corruption)
- Normalizes geographic information (state codes)
- Cleans office/race names using custom macros
- **Critical:** Expands candidates into separate primary/general election records
  - Each candidate with both primary and general elections becomes 2 separate rows
  - Adds `election_type` field ('primary', 'general', or 'runoff')

**Output:** `offline_data/hubspot_candidacy_cleaned_latest.parquet`, `offline_data/ddhq_election_results_cleaned_latest.parquet`

#### Step 3: Temporal Filtering (`temporal_filtering.py`)

**Location:** `gp-ai-projects/hubspot_ddhq_match/temporal_filtering.py`

**Process:**
- Filters HubSpot candidates to only those with election dates **exactly matching** DDHQ dates
- Prevents matching 2020 candidates with 2024 results
- Special handling for runoffs: checks both runoff_date and general_election_date

**Output:** `offline_data/hubspot_filtered_to_match_ddhq_dates_latest.parquet`

**Retention Rate:** ~63% (filters out ~37% of records without matching DDHQ dates)

#### Step 4: Embedding Generation (`generate_cleaned_embeddings.py`)

**Location:** `gp-ai-projects/hubspot_ddhq_match/generate_cleaned_embeddings.py`

**Process:**
- Uses **Google Gemini `text-embedding-004`** to create semantic vector representations
- Creates labeled text format: `"name: John Smith | race: CA Governor"`
- Generates embeddings for both HubSpot and DDHQ datasets
- Configuration: `BATCH_SIZE=150`, `MAX_WORKERS=400`

**Output:** `offline_data/hubspot_filtered_with_embeddings_latest.parquet`, `offline_data/ddhq_with_embeddings_cleaned_latest.parquet`

**Cost:** ~$0.02 per 1M tokens (embedding API)

#### Step 5: Production Matching (`parallel_production_matcher.py`)

**Location:** `gp-ai-projects/hubspot_ddhq_match/parallel_production_matcher.py`

This is the core matching engine with multiple stages:

##### Stage 5a: Date-Partitioned FAISS Index Building

**Process:**
- Partitions DDHQ data by: `{date}_{state}_{election_type}`
- Builds FAISS indices using L2 distance on normalized embeddings
- Uses lazy-loading with LRU cache (max 64 partitions in memory)
- **Key optimization:** Only searches candidates from the same election date + state + type

**Data Structure:**
```python
partition_key = f"{election_date}_{state}_{election_type}"
# e.g., "2024-11-05_CA_general"
```

##### Stage 5b: FAISS Semantic Similarity Search

**Process:**
- For each HubSpot candidate, finds the top 5 most similar DDHQ candidates
- Uses `IndexFlatIP` (Inner Product) with L2 normalization
- Search is constrained to the relevant partition only

**Output per candidate:** Top 5 DDHQ candidates with similarity scores

##### Stage 5c: LLM Validation (Google Gemini Flash)

**Process:**
- Validates each similarity match using strict rules
- Uses **Gemini Flash 3** with temperature=0.0 (deterministic)
- Checks: name matching, geographic alignment, gender consistency
- Returns JSON with: `best_match`, `confidence` (0-100), `reasoning`

**Confidence Calibration:**
- 70% minimum threshold for acceptance
- Gender mismatch: -25 points
- No exact name component: -15 points
- Partial name match: -8 points

**LLM Prompt Examples:**
```
VALID: "John Smith" → "John Smith, Berkeley City Council" (exact match)
VALID: "Bob Johnson" → "Robert Johnson, County Commissioner" (nickname)
INVALID: "Stanley Pokras" → "Susan Kopras" (gender mismatch)
INVALID: "Marco Huerta" → "Erick Huerta" (different first name)
```

##### Stage 5d: Fallback Mechanisms

- **Stateless fallback:** For records missing state info, searches all states for that date
- **Date-range fallback:** For runoffs, searches dates >= base election date

**Concurrency:** ThreadPoolExecutor with up to 2000 concurrent workers, targeting 10,000 records/minute

**Output:** `output/parallel_hubspot_ddhq_matches_latest.parquet`

#### Step 6: Runoff Enrichment (`enrich_runoffs.py`)

**Location:** `gp-ai-projects/hubspot_ddhq_match/enrich_runoffs.py`

**Process:**
- Discovers runoffs from DDHQ for all matched candidates
- Creates synthetic HubSpot runoff records
- Generates embeddings and matches synthetic records to DDHQ
- Merges runoff results with original matches

**Output:** `output/discovered_runoffs_latest.parquet`

### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `gp_candidacy_id` | string | GoodParty candidacy identifier |
| `hubspot_first_name` | string | Candidate first name from HubSpot |
| `hubspot_last_name` | string | Candidate last name from HubSpot |
| `hubspot_election_date` | date | Election date from HubSpot |
| `hubspot_election_type` | string | 'primary', 'general', or 'runoff' |
| `llm_best_match` | string | Best DDHQ candidate name selected by LLM |
| `llm_confidence` | int | Confidence score 0-100 |
| `llm_reasoning` | string | LLM explanation for match decision |
| `has_match` | boolean | Whether a match was found above threshold |
| `ddhq_candidate` | string | Matched DDHQ candidate name |
| `ddhq_race_name` | string | DDHQ race name |
| `ddhq_race_id` | int | DDHQ race identifier |
| `ddhq_candidate_id` | int | DDHQ candidate identifier |
| `ddhq_is_winner` | boolean | Whether candidate won the race |
| `match_similarity` | float | FAISS embedding similarity score |
| `embedding_text` | string | Text used to generate embeddings |

### Integration with dbt

**Two integration paths:**

1. **Manual/Batch Path:**
   - Upload parquet to `model_predictions.candidacy_ddhq_matches_YYYYMMDD`
   - Create staging model `stg_model_predictions__candidacy_ddhq_matches_YYYYMMDD.sql`
   - Reference in downstream models

2. **Live API Path:**
   - `int__gp_ai_start_election_match.py` triggers ECS task via HTTP POST to `/match/hubspot-ddhq`
   - `int__gp_ai_election_match.py` reads results from S3 once complete
   - Falls back to latest manual load if S3 file not ready

**Downstream Consumers:**
- `m_general__candidacy.sql` - Joins match results to candidacy mart
- `m_general__contest.sql` - Uses DDHQ race_id for contest data
- Civics mart tables - Include `ddhq_candidate_id`, `ddhq_race_id`, confidence scores

---

## Match Type 2: L2 Districts to BallotReady Positions

### Purpose

Match L2 voter file district names (e.g., "State House District 15") to BallotReady office positions (e.g., "California State Assembly District 15") to enable voter turnout predictions for specific races.

### Data Sources

| Source | Table/Location | Description |
|--------|----------------|-------------|
| **Input: L2 Districts** | `dbt.m_election_api__district` | Unique L2 districts from voter file |
| **Input: BR Positions** | `dbt.int__enhanced_position` | BallotReady office positions |
| **Output: Match Results** | `model_predictions.llm_l2_br_match_YYYYMMDD` | Matched pairs with confidence |

### Pipeline Steps

#### Step 1: Vector Store Generation (`vector_store_generator.py`)

**Location:** `gp-ai-projects/stitch_golden_data/prod_gold_data/vector_store_generator.py`

**Process:**
- Reads L2 districts from `dbt.m_election_api__district`
- Creates Gemini embeddings for each district
- Embedding text format: `"state: {state}, district type: {district_type}, district name: {district_name}"`
- Generates separate vector stores per state

**Output:** Per-state pickle files in `prod_gold_data/vector_store/`
```python
# Example: l2_embeddings_ca.pkl
{
    'state': 'CA',
    'embeddings': np.ndarray,  # shape: [num_districts, 768]
    'texts': List[str],        # Original embedding texts
    'metadata': List[Dict],    # {district_name, district_type, state}
    'created_at': timestamp,
    'total_districts': int
}
```

#### Step 2: Production Matching (`production_matcher.py`)

**Location:** `gp-ai-projects/stitch_golden_data/prod_gold_data/production_matcher.py`

##### Stage 2a: Load BallotReady Positions

**Process:**
- Queries `dbt.int__enhanced_position` for BR positions by state
- Caches as parquet for re-use

##### Stage 2b: Embedding Search (per BR position)

**Process:**
- Query: `"race name: {br_position_name}"`
- Generate Gemini embedding for query
- Calculate cosine similarity against all L2 embeddings in state
- Return top 10 matches + 1 generic state result + 2 buffer = 13 candidates

**Output:** `List[EmbeddingDistrict]` with `(name, type, similarity_score, state)`

##### Stage 2c: LLM Selection (Google Gemini Flash)

**Process:**
- Present 13 candidates to LLM with decision rules
- LLM selects best match (1-13) or rejects all (0)
- Uses structured JSON output for reliable parsing

**LLM Decision Rules:**
- Geographic alignment (city/county matching)
- Office type and district type compatibility
- Specific identifiers or numbers in names
- Functional role alignment (e.g., School Board → School Board districts)
- Ignores seats and positions
- Rejects matches for offices greater than state level
- Returns 0 (no match) if unsure

**Output Schema:**
```json
{
    "selected_candidate_number": 1-13 or 0,
    "selection_confidence": 0-100,
    "reasoning": "explanation string",
    "close_alternatives": ["alt1", "alt2"]
}
```

##### Stage 2d: Result Formatting

**Process:**
- Add all matching columns to original BR position row
- Preserve all original BR data

**Concurrency:** ThreadPoolExecutor with 1500 workers, 1200 max HTTP connections

**Output:** Per-state parquet files, then merged to `full_state_matching.parquet`

#### Step 3: Result Aggregation (`merge_all_states.py`)

**Location:** `gp-ai-projects/stitch_golden_data/merge_all_states.py`

**Process:**
- Merges individual state parquet files
- Sorts by state and br_database_id
- Generates statistics on match rates and confidence

### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `br_database_id` | int | BallotReady position unique identifier |
| `name` | string | BallotReady position name |
| `state` | string | State postal code |
| `l2_district_name` | string | Matched L2 district name |
| `l2_district_type` | string | Matched L2 district type |
| `is_matched` | boolean | Whether a match was found |
| `llm_reason` | string | LLM reasoning for selection |
| `confidence` | int | Confidence score 0-100 |
| `top_embedding_score` | float | Best embedding similarity score |
| `embeddings` | string | JSON of all 13 embedding candidates with scores |
| `alternative_matches` | string | JSON list of close alternatives |
| `embedding_queried_term` | string | Query used for embedding search |

### Integration with dbt

**Manual Load Process:**
1. Upload to `model_predictions.llm_l2_br_match_YYYYMMDD`
2. Create `stg_model_predictions__llm_l2_br_match_YYYYMMDD.sql`
3. Update reference in `int__zip_code_to_br_office.sql`

**Downstream Consumer: `int__zip_code_to_br_office.sql`**

```sql
-- Lines 70-74: Join L2-BR match results
left join stg_model_predictions__llm_l2_br_match_20250811 as tbl_match
    on lower(tbl_zip.exploded_district_name) = lower(tbl_match.l2_district_name)
    and lower(tbl_zip.district_type) = lower(tbl_match.l2_district_type)
    and lower(tbl_zip.state_postal_code) = lower(tbl_match.state)
```

---

## Match Type 3: Zip Code to L2 District (dbt-native)

### Purpose

Map zip codes to L2 electoral districts using voter file data. This is a deterministic join (no AI/LLM) but has data quality challenges.

### Data Sources

| Source | Table/Location | Description |
|--------|----------------|-------------|
| **Input: L2 Voter File** | `dbt.int__l2_nationwide_uniform` | Nationwide L2 voter records |
| **Output: Zip Mapping** | `dbt.int__zip_code_to_l2_district` | Zip code to district mapping |

### Pipeline Steps

**Location:** `dbt/project/models/intermediate/l2/int__zip_code_to_l2_district.py`

**Process:**
1. Read L2 uniform data with voter addresses
2. For each district type column, extract unique `(zip_code, state, district_type, district_name)` tuples
3. Group by `(zip_code, state, district_type)` and collect district names into arrays
4. Handle 4-character zip codes by padding with leading zero

### Known Data Quality Issues

Per team discussion:
- **Borderlands problem:** Missouri addresses appearing in Kansas zip codes
- **Egregious errors:** CA districts with FL zip codes (voter registration address mismatches)

**Mitigation in `int__zip_code_to_br_office.sql`:**
```sql
-- Lines 33-49: Filter zip codes to valid state ranges
zip_code_within_state_range as (
    select ...
    from zip_code_to_l2_district as tbl_zip
    inner join int__general_states_zip_code_range as zip_range
        on tbl_zip.state_postal_code = zip_range.state_postal_code
        and tbl_zip.zip_code >= zip_range.zip_code_range[0]
        and tbl_zip.zip_code <= zip_range.zip_code_range[1]
)
```

---

## Data Flow Diagrams

### Complete End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              RAW DATA SOURCES                                        │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   HubSpot CRM          DDHQ Election       BallotReady API      L2 Voter File       │
│   (Candidates)         Results             (Positions/Races)    (Districts)         │
│        │                   │                     │                   │              │
│        ▼                   ▼                     ▼                   ▼              │
│   ┌─────────┐        ┌──────────┐          ┌──────────┐       ┌──────────┐         │
│   │ Airbyte │        │ G-Drive  │          │ Airbyte  │       │ S3 Load  │         │
│   │  Sync   │        │  Sync    │          │  Sync    │       │          │         │
│   └────┬────┘        └────┬─────┘          └────┬─────┘       └────┬─────┘         │
│        │                  │                     │                  │               │
└────────┼──────────────────┼─────────────────────┼──────────────────┼───────────────┘
         │                  │                     │                  │
         ▼                  ▼                     ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              DATABRICKS BRONZE/SILVER                                │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   hubspot_api_contacts    ddhq_gdrive_         ballotready_api_    l2_s3_{state}_   │
│                           election_results     position            uniform          │
│        │                       │                    │                  │            │
│        ▼                       ▼                    ▼                  ▼            │
│   stg_airbyte_source__    stg_airbyte_source__ stg_airbyte_source__ int__l2_        │
│   hubspot_api_contacts    ddhq_gdrive_...      ballotready_api_...  nationwide_...  │
│        │                       │                    │                  │            │
│        ▼                       ▼                    ▼                  ▼            │
│   m_general__candidacy    (used by matcher)    int__enhanced_       m_election_api_ │
│                                                position             _district       │
│                                                                                      │
└────────┬───────────────────────┬────────────────────┬──────────────────┬────────────┘
         │                       │                    │                  │
         │    MATCH TYPE 1       │    MATCH TYPE 2    │    MATCH TYPE 3  │
         │    (External)         │    (External)      │    (dbt-native)  │
         ▼                       ▼                    ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              GP-AI-PROJECTS (External Compute)                       │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   ┌─────────────────────────────┐    ┌─────────────────────────────────────────┐   │
│   │  hubspot_ddhq_match/        │    │  stitch_golden_data/prod_gold_data/     │   │
│   │                             │    │                                         │   │
│   │  1. Extract from Databricks │    │  1. Build vector stores from L2         │   │
│   │  2. Clean & normalize       │    │  2. Load BR positions                   │   │
│   │  3. Temporal filter         │    │  3. Embedding search (top 13)           │   │
│   │  4. Generate embeddings     │    │  4. LLM selection                       │   │
│   │  5. FAISS partitioned search│    │  5. Merge all states                    │   │
│   │  6. LLM validation          │    │                                         │   │
│   │  7. Runoff enrichment       │    │                                         │   │
│   │                             │    │                                         │   │
│   │  AI: Gemini Embeddings      │    │  AI: Gemini Embeddings                  │   │
│   │       FAISS Vector Search   │    │       Cosine Similarity                 │   │
│   │       Gemini Flash LLM      │    │       Gemini Flash LLM                  │   │
│   └──────────────┬──────────────┘    └───────────────────┬─────────────────────┘   │
│                  │                                       │                          │
│                  ▼                                       ▼                          │
│   ┌─────────────────────────────┐    ┌─────────────────────────────────────────┐   │
│   │  Output: S3 parquet         │    │  Output: Local parquet files            │   │
│   │  s3://ddhq-matcher-output/  │    │  stitch_golden_data/output/             │   │
│   └──────────────┬──────────────┘    └───────────────────┬─────────────────────┘   │
│                  │                                       │                          │
└──────────────────┼───────────────────────────────────────┼──────────────────────────┘
                   │      MANUAL / SCHEDULED UPLOAD        │
                   ▼                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              MODEL_PREDICTIONS SCHEMA                                │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   candidacy_ddhq_matches_20251202          llm_l2_br_match_20250811                 │
│   candidacy_ddhq_matches_20251112          llm_l2_br_match_20250806                 │
│   candidacy_ddhq_matches_20251016          ...                                      │
│   ...                                                                               │
│                                                                                      │
└────────────────────────────────────────────────────────────────────────────────────┘
                   │                                       │
                   ▼                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              DBT STAGING LAYER                                       │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   stg_model_predictions__candidacy_ddhq_matches_20251202.sql                        │
│   stg_model_predictions__llm_l2_br_match_20250811.sql                               │
│                                                                                      │
└────────────────────────────────────────────────────────────────────────────────────┘
                   │                                       │
                   ▼                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              DBT INTERMEDIATE LAYER                                  │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   int__gp_ai_election_match.py ◄────────── Consumes DDHQ match results             │
│                                                                                      │
│   int__zip_code_to_l2_district.py ◄─────── Direct L2 voter file mapping            │
│             │                                                                        │
│             ▼                                                                        │
│   int__zip_code_to_br_office.sql ◄──────── Joins L2-BR matches + zip mapping       │
│                                                                                      │
└────────────────────────────────────────────────────────────────────────────────────┘
                   │                                       │
                   ▼                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              DBT MARTS LAYER                                         │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   m_general__candidacy ◄────── DDHQ match results, win/loss, confidence            │
│   m_general__contest   ◄────── DDHQ race_id for contest data                       │
│   m_election_api__district ◄── L2 districts with turnout predictions               │
│                                                                                      │
│   Civics Mart Tables:                                                               │
│   - m_civics__candidate                                                             │
│   - m_civics__candidacy                                                             │
│   - m_civics__candidacy_stage ◄── ddhq_candidate_id, ddhq_race_id, confidence      │
│   - m_civics__election                                                              │
│   - m_civics__election_stage                                                        │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Current Performance Characteristics

### HubSpot-DDHQ Matcher

| Metric | Value |
|--------|-------|
| **Throughput** | ~10,000 records/minute |
| **Concurrency** | 2,000 parallel workers |
| **FAISS Partitions** | Up to 64 in LRU cache |
| **LLM Calls per Record** | 1 (validates top 5 candidates) |
| **Embedding Batch Size** | 150 |
| **Embedding Workers** | 400 |
| **Confidence Threshold** | 70% minimum |
| **Estimated Cost** | ~$0.0007 per 50 records (LLM) |

### L2-BallotReady Matcher

| Metric | Value |
|--------|-------|
| **Throughput** | 50-100+ records/second |
| **Concurrency** | 1,500 parallel workers |
| **Max HTTP Connections** | 1,200 |
| **Candidates per LLM Call** | 13 |
| **Embedding Delay** | 0.02 seconds |
| **Rate Limit** | 10,000 RPM (Gemini API) |

### Cost Breakdown (Estimated)

| Component | Cost per 1M Tokens | Typical Usage |
|-----------|-------------------|---------------|
| Gemini Embeddings | ~$0.02 | High volume |
| Gemini Flash LLM | ~$0.075 | 1 call per record |

---

## Opportunities for Optimization

### 1. Reduce Number of Match Candidates

**Current State:**
- HubSpot-DDHQ: Top 5 candidates from FAISS search
- L2-BR: Top 13 candidates (10 embedding + 1 generic + 2 buffer)

**Potential Improvements:**
- Use tighter date/state/district_type partitioning to reduce search space
- Pre-filter candidates using deterministic rules (exact name components, office type)
- Reduce L2-BR candidates from 13 to 5-7 with better embedding quality

### 2. Probabilistic Matching with Splink

**Current State:**
- Embedding similarity + LLM validation (expensive)
- No probabilistic scoring or explainable match weights

**Potential Improvements:**
- Implement Splink for probabilistic record linkage
- Use blocking rules to reduce comparison space:
  - Block on state + election_year
  - Block on soundex(last_name) + state
  - Block on office_type + state
- Train match weights on labeled data
- Use Splink scores to pre-filter before LLM validation
- Only send low-confidence Splink matches to LLM

**Splink Benefits:**
- Explainable match weights per field
- Faster than LLM for high-volume matching
- Can run natively in Spark/Databricks
- Produces match probabilities for downstream use

### 3. Move Compute into Databricks

**Current State:**
- External Python processes (local or AWS ECS)
- Manual upload of results to `model_predictions` schema
- Separate Gemini API calls from Databricks environment

**Potential Improvements:**
- Use Databricks ML Runtime for embedding generation
- Implement FAISS in Databricks using `faiss-cpu` in cluster libraries
- Use Databricks Model Serving for LLM calls (or direct Gemini API)
- Implement as dbt Python models with PySpark
- Trigger via Databricks Workflows instead of external ECS

**Migration Path:**
1. Move vector store generation to dbt Python model
2. Implement embedding search as UDF or Python model
3. Use Databricks Feature Store for embeddings cache
4. Implement Splink blocking + scoring in Spark SQL
5. Reserve LLM calls only for ambiguous matches

### Proposed Hybrid Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              DATABRICKS (All Compute)                                │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   Stage 1: Blocking (Splink)                                                        │
│   - Deterministic rules: state, year, office_type                                   │
│   - Phonetic blocking: soundex(name)                                                │
│   - Output: Candidate pairs for comparison                                          │
│                                                                                      │
│   Stage 2: Probabilistic Scoring (Splink)                                           │
│   - Field-level comparison: name, office, geography                                 │
│   - Match probability per pair                                                      │
│   - Output: High-confidence matches (>95%) → direct accept                          │
│            Low-confidence matches (<50%) → direct reject                            │
│            Medium-confidence (50-95%) → LLM validation                              │
│                                                                                      │
│   Stage 3: LLM Validation (Only ambiguous cases)                                    │
│   - ~10-20% of total pairs (vs. 100% currently)                                     │
│   - Gemini Flash via Databricks Model Serving                                       │
│   - Output: Final match decisions with reasoning                                    │
│                                                                                      │
│   Stage 4: dbt Transformation                                                       │
│   - All match results in Databricks tables                                          │
│   - No external data movement                                                       │
│   - Real-time incremental updates                                                   │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

**Expected Benefits:**
- **Speed:** 5-10x faster by reducing LLM calls to ambiguous cases only
- **Cost:** 80-90% reduction in LLM API costs
- **Precision:** Splink provides explainable match weights; LLM handles edge cases
- **Operability:** Single platform, no manual data movement, real-time updates

---

## File Reference

### gp-ai-projects

| File | Purpose |
|------|---------|
| `hubspot_ddhq_match/run_pipeline.py` | Main orchestrator for DDHQ matching |
| `hubspot_ddhq_match/data_extraction.py` | Databricks data extraction |
| `hubspot_ddhq_match/data_cleaning.py` | Data standardization |
| `hubspot_ddhq_match/temporal_filtering.py` | Date alignment filter |
| `hubspot_ddhq_match/generate_cleaned_embeddings.py` | Gemini embedding generation |
| `hubspot_ddhq_match/parallel_production_matcher.py` | Core FAISS + LLM matching |
| `hubspot_ddhq_match/enrich_runoffs.py` | Runoff discovery |
| `stitch_golden_data/prod_gold_data/vector_store_generator.py` | L2 vector store creation |
| `stitch_golden_data/prod_gold_data/production_matcher.py` | L2-BR matching engine |
| `stitch_golden_data/merge_all_states.py` | Result aggregation |

### dbt Project

| File | Purpose |
|------|---------|
| `models/staging/__sources.yaml` | Source definitions including model_predictions |
| `models/staging/model_predictions_source/stg_model_predictions__candidacy_ddhq_matches_*.sql` | DDHQ match staging |
| `models/staging/model_predictions_source/stg_model_predictions__llm_l2_br_match_*.sql` | L2-BR match staging |
| `models/intermediate/gp_ai/int__gp_ai_start_election_match.py` | Triggers live DDHQ matcher |
| `models/intermediate/gp_ai/int__gp_ai_election_match.py` | Fetches DDHQ match results |
| `models/intermediate/l2/int__zip_code_to_l2_district.py` | Zip to L2 district mapping |
| `models/intermediate/l2/int__zip_code_to_br_office.sql` | Joins L2-BR matches |
| `models/intermediate/int__enhanced_position.sql` | BallotReady positions |
| `models/marts/election_api/m_election_api__district.sql` | L2 districts mart |
| `models/marts/general/m_general__candidacy.sql` | Candidacy mart with DDHQ results |

---

## Appendix: Key ID Mappings

| Entity | GoodParty | BallotReady | DDHQ | HubSpot | Product DB |
|--------|-----------|-------------|------|---------|------------|
| Candidate | `gp_candidate_id` | `person_id` | `candidate_id` | N/A | `db_user_id` |
| Candidacy | `gp_candidacy_id` | N/A | N/A | `hs_contact_id` | `campaign_id` |
| Election | `gp_election_id` | `position_id` | N/A | N/A | N/A |
| Election Stage | `gp_election_stage_id` | `race_id` | `ddhq_race_id` | N/A | N/A |
| Candidacy-Stage | `gp_candidacy_stage_id` | `candidacy_id` | `candidacy_id` | N/A | N/A |

---

## Appendix: Cyclical Data Flow

The relationship between dbt and the AI matchers is **cyclical**, not linear. dbt models provide the source data that feeds the AI matchers, and the AI matchers produce results that flow back into dbt for downstream consumption.

### HubSpot-DDHQ Cycle

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                                                         │
                    ▼                                                         │
┌─────────────────────────────────────┐                                       │
│           dbt Project               │                                       │
│                                     │                                       │
│  m_general__candidacy ──────────────┼───► Source data for matching         │
│  stg_...__ddhq_gdrive_election_...  │     (candidates + election results)  │
│                                     │                                       │
│         ▲                           │                                       │
│         │                           │                                       │
│  stg_model_predictions__            │                                       │
│  candidacy_ddhq_matches_YYYYMMDD    │                                       │
│         │                           │                                       │
│         │  Match results with       │                                       │
│         │  DDHQ IDs, confidence,    │                                       │
│         │  win/loss outcomes        │                                       │
│         │                           │                                       │
└─────────┼───────────────────────────┘                                       │
          │                                                                   │
          │                                                                   │
┌─────────┴───────────────────────────┐                                       │
│      model_predictions schema       │                                       │
│                                     │                                       │
│  candidacy_ddhq_matches_YYYYMMDD ◄──┼───────────────────────────────────────┤
│                                     │                                       │
└─────────────────────────────────────┘                                       │
          ▲                                                                   │
          │  Upload parquet                                                   │
          │                                                                   │
┌─────────┴───────────────────────────┐                                       │
│        gp-ai-projects               │                                       │
│        hubspot_ddhq_match/          │                                       │
│                                     │                                       │
│  1. Extract from dbt models ◄───────┼───────────────────────────────────────┘
│  2. Clean & filter                  │
│  3. Generate embeddings             │
│  4. FAISS search + LLM validate     │
│  5. Output matches                  │
│                                     │
└─────────────────────────────────────┘
```

### L2-BallotReady Cycle

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                                                         │
                    ▼                                                         │
┌─────────────────────────────────────┐                                       │
│           dbt Project               │                                       │
│                                     │                                       │
│  int__enhanced_position ────────────┼───► BR positions for matching        │
│  m_election_api__district ──────────┼───► L2 districts for vector store    │
│                                     │                                       │
│         ▲                           │                                       │
│         │                           │                                       │
│  stg_model_predictions__            │                                       │
│  llm_l2_br_match_YYYYMMDD           │                                       │
│         │                           │                                       │
│         │  Match results with       │                                       │
│         │  l2_district_name,        │                                       │
│         │  confidence, reasoning    │                                       │
│         │                           │                                       │
│         ▼                           │                                       │
│  int__zip_code_to_br_office         │                                       │
│  (joins zip → L2 → BR)              │                                       │
│                                     │                                       │
└─────────┬───────────────────────────┘                                       │
          │                                                                   │
          │                                                                   │
┌─────────┴───────────────────────────┐                                       │
│      model_predictions schema       │                                       │
│                                     │                                       │
│  llm_l2_br_match_YYYYMMDD ◄─────────┼───────────────────────────────────────┤
│                                     │                                       │
└─────────────────────────────────────┘                                       │
          ▲                                                                   │
          │  Upload parquet                                                   │
          │                                                                   │
┌─────────┴───────────────────────────┐                                       │
│        gp-ai-projects               │                                       │
│        stitch_golden_data/          │                                       │
│                                     │                                       │
│  1. Build vector store from L2 ◄────┼───────────────────────────────────────┘
│  2. Load BR positions from dbt      │
│  3. Embedding search + LLM select   │
│  4. Output matches                  │
│                                     │
└─────────────────────────────────────┘
```

### Simplified Combined View

```
                              ┌───────────────────┐
                              │   Raw Sources     │
                              │  (HubSpot, DDHQ,  │
                              │   BallotReady,    │
                              │   L2 Voter File)  │
                              └─────────┬─────────┘
                                        │
                                        ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                                                                               │
│                              dbt Project                                      │
│                                                                               │
│    ┌──────────────┐      ┌──────────────┐      ┌──────────────────────┐      │
│    │   Staging    │ ───► │ Intermediate │ ───► │        Marts         │      │
│    │   Models     │      │    Models    │      │  (m_general__...,    │      │
│    └──────────────┘      └──────────────┘      │   m_election_api__..│      │
│                                                 └──────────────────────┘      │
│          ▲                                               │                    │
│          │                                               │                    │
│          │  Match results                                │  Source data       │
│          │  (model_predictions)                          │  for matching      │
│          │                                               │                    │
└──────────┼───────────────────────────────────────────────┼────────────────────┘
           │                                               │
           │         ┌─────────────────────────┐           │
           │         │                         │           │
           └─────────┤    gp-ai-projects       ◄───────────┘
                     │                         │
                     │  • Embeddings (Gemini)  │
                     │  • Vector Search (FAISS)│
                     │  • LLM Validation       │
                     │                         │
                     └─────────────────────────┘
```

**Key Insight:** The AI matchers are not standalone processes. They are tightly coupled to the dbt project:
- **Inputs** come from dbt mart/intermediate tables
- **Outputs** feed back into dbt via the `model_predictions` schema
- Changes to dbt source models may require re-running the matchers
- The cycle enables incremental enrichment of candidate/election data over time

---

## Appendix: Using LLM Results as Ground Truth for Splink Training

### The Opportunity

The current LLM-based matching produces high-quality match decisions with confidence scores and reasoning. These results can serve as **labeled training data** for a Splink probabilistic matching model, enabling a hybrid approach that dramatically reduces cost while maintaining precision.

### Current State vs. Proposed Hybrid Approach

**Current Flow (100% LLM):**
```
Candidates → Embeddings → FAISS → LLM Validation → Final Matches
                                    (per-match)
```

**Proposed Hybrid Flow:**
```
Phase 1: LLM produces ground truth labels (one-time or periodic)
Phase 2: Train Splink model on LLM-labeled data
Phase 3: Splink handles bulk matching (fast, cheap, in Databricks)
Phase 4: LLM validates only low-confidence Splink matches
```

### Why This Works for GoodParty's Use Case

1. **You Already Have Labeled Data**: The existing `stg_model_predictions__candidacy_ddhq_matches` table contains `llm_confidence` (0-100) and `has_match` fields—these are essentially ground truth labels. High-confidence matches (≥90) can directly become training data.

2. **Splink Needs Labeled Pairs**: Splink requires training data in the form of `(record_A, record_B, is_match)`. Your existing match results already have this structure.

3. **Cost Reduction**: LLM calls are expensive per-match. Training data needs to be created once, then Splink runs cheaply at scale on Databricks/Spark.

4. **Speed**: Splink is designed for probabilistic record linkage at scale and runs natively in Spark.

5. **Explainability**: Splink provides field-level match weights, showing which features contributed to each match decision.

### Implementation Path

```
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 1: Extract Ground Truth from Existing LLM Results       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  stg_model_predictions__candidacy_ddhq_matches                  │
│       │                                                         │
│       ▼                                                         │
│  Filter: llm_confidence >= 90 AND has_match = true  → POSITIVE  │
│  Filter: llm_confidence >= 90 AND has_match = false → NEGATIVE  │
│       │                                                         │
│       ▼                                                         │
│  int__splink_training_labels                                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  PHASE 2: Train Splink Model (in Databricks)                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Splink learns match weights for:                               │
│  - Name similarity (Jaro-Winkler, Levenshtein)                  │
│  - Office/position match                                        │
│  - State/city geographic alignment                              │
│  - Party affiliation                                            │
│  - Election date proximity                                      │
│                                                                 │
│  Output: Trained Splink model parameters                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  PHASE 3: Bulk Matching with Splink                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  m_general__candidacy  ──┐                                      │
│                          ├──► Splink ──► match_probability      │
│  stg_ddhq__election_*  ──┘                                      │
│                                                                 │
│  High probability (≥0.95): Auto-accept                          │
│  Low probability (≤0.10): Auto-reject                           │
│  Medium (0.10-0.95): Send to LLM for validation                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  PHASE 4: LLM Validation (Reduced Scope)                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Only ~10-20% of pairs (medium-confidence from Splink)          │
│  sent to Gemini Flash for final decision                        │
│                                                                 │
│  Output: Final matches with dual confidence signals             │
│  - splink_probability: Fast, cheap, explainable                 │
│  - llm_confidence: Handles edge cases                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### New Components Required

| Component | Location | Description |
|-----------|----------|-------------|
| `int__splink_training_labels` | dbt SQL/Python | Extracts high-confidence LLM results as labeled training pairs |
| `int__splink_model_training` | Databricks notebook or dbt Python | Trains Splink on labeled pairs, outputs model parameters |
| `int__splink_match_candidates` | dbt Python model | Runs Splink predictions at scale using trained model |
| `int__llm_validation_queue` | dbt SQL | Filters medium-confidence Splink matches for LLM review |
| Reduced LLM matcher | gp-ai-projects | Only validates uncertain matches (10-20% of volume) |

### Training Data Extraction (SQL Example)

```sql
-- int__splink_training_labels.sql
with high_confidence_matches as (
    select
        gp_candidacy_id,
        first_name,
        last_name,
        candidate_office,
        state,
        election_date,
        election_type,
        ddhq_candidate,
        ddhq_race_name,
        ddhq_candidate_id,
        ddhq_race_id,
        llm_confidence,
        has_match,
        -- Splink requires explicit match labels
        case
            when llm_confidence >= 90 and has_match = true then 1
            when llm_confidence >= 90 and has_match = false then 0
            else null  -- exclude uncertain cases from training
        end as is_match
    from {{ ref('stg_model_predictions__candidacy_ddhq_matches_20251202') }}
    where llm_confidence >= 90  -- only use high-confidence labels
)
select * from high_confidence_matches
where is_match is not null
```

### Splink Model Configuration (Python Example)

```python
from splink import Linker, DuckDBAPI, SettingsCreator, block_on
import splink.comparison_library as cl

# Define comparison rules based on your matching fields
settings = SettingsCreator(
    link_type="link_only",
    comparisons=[
        cl.JaroWinklerAtThresholds("first_name", [0.9, 0.7]),
        cl.JaroWinklerAtThresholds("last_name", [0.9, 0.7]),
        cl.ExactMatch("state"),
        cl.ExactMatch("election_date"),
        cl.LevenshteinAtThresholds("candidate_office", [1, 3, 5]),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("state", "election_date"),
        block_on("state", "last_name"),
    ],
)

linker = Linker([hubspot_df, ddhq_df], settings, db_api=DuckDBAPI())

# Train using labeled data from LLM results
linker.training.estimate_probability_two_random_records_match(
    training_labels_df,  # from int__splink_training_labels
    "is_match"
)
linker.training.estimate_parameters_using_expectation_maximisation(
    block_on("state", "election_date")
)

# Predict on full dataset
predictions = linker.inference.predict(threshold_match_probability=0.1)
```

### Expected Benefits

| Metric | Current (100% LLM) | Hybrid (Splink + LLM) | Improvement |
|--------|-------------------|----------------------|-------------|
| LLM API calls | 100% of pairs | 10-20% of pairs | 80-90% reduction |
| Cost per 10K records | ~$7.00 | ~$0.70-1.40 | 5-10x cheaper |
| Throughput | ~10K/min | ~50-100K/min | 5-10x faster |
| Explainability | LLM reasoning only | Field weights + LLM reasoning | Enhanced |
| Platform | External (ECS) | Native Databricks | Simplified ops |

### Dual Confidence Signals

The hybrid approach produces two complementary confidence measures:

1. **Splink Probability**:
   - Fast, deterministic, explainable
   - Shows which fields contributed (name: +0.8, state: +0.2, office: -0.1)
   - Good for automated decisions on clear cases

2. **LLM Confidence** (for ambiguous cases):
   - Handles edge cases, nicknames, typos
   - Provides human-readable reasoning
   - Resolves cases where Splink is uncertain

Matches where **both agree** (high Splink + high LLM) have very high reliability.

### Iterative Improvement Loop

The hybrid system enables continuous improvement:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Continuous Improvement Cycle                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   1. Run Splink on new data                                     │
│          │                                                      │
│          ▼                                                      │
│   2. LLM validates ambiguous cases                              │
│          │                                                      │
│          ▼                                                      │
│   3. New LLM decisions added to training set                    │
│          │                                                      │
│          ▼                                                      │
│   4. Periodically retrain Splink with expanded labels           │
│          │                                                      │
│          ▼                                                      │
│   5. Splink improves → fewer ambiguous cases → less LLM usage   │
│          │                                                      │
│          └──────────────────► (repeat)                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

Over time, the Splink model learns from LLM decisions, reducing the percentage of records that need LLM validation.

---

## Appendix: Stratified Sampling for Splink Training Data

### The Case for Random/Stratified Sampling

While using existing high-confidence LLM matches as training data is convenient, a **stratified random sample** with targeted LLM labeling may produce a better Splink model. Here's why:

### Selection Bias in Existing Data

The current LLM matches are biased toward "matchable" records—they've already passed through:
- FAISS filtering (top 5 candidates only)
- Temporal filtering (63% retention rate)
- State/date partitioning

This means the training data is missing:
- Records that get filtered out early
- True negatives (cases where no match exists in DDHQ)
- Edge cases the current pipeline might miss

A random sample would capture the full distribution of matching scenarios.

### Stratified Sampling Strategy

Rather than uniform random sampling, **stratify by ambiguity level** to maximize learning at the decision boundary:

```
Sample Distribution (example: 5,000 labeled pairs):
├── 1,500 random pairs (unbiased baseline)
├── 1,500 high-similarity pairs (FAISS score > 0.8)
├── 1,000 medium-similarity pairs (FAISS 0.5-0.8)  ← ambiguous zone
└── 1,000 low-similarity pairs (FAISS < 0.5)       ← true negatives
```

### Why Ambiguity Matters

Splink needs to learn the **decision boundary**. Obvious cases don't teach it much:

| Ambiguity Level | Example | Training Value |
|-----------------|---------|----------------|
| **Low (obvious match)** | "John Smith" → "John Smith" | Low |
| **High (ambiguous)** | "Bob Johnson" → "Robert Johnson Jr." | **High** |
| **High (ambiguous)** | "Maria Garcia" → "Maria Garcia-Lopez" | **High** |
| **Low (obvious non-match)** | "John Smith" → "Sarah Williams" | Low |

Oversampling ambiguous cases gives Splink better signal where it matters most.

### Implementation Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│  STEP 1: Generate Candidate Pairs (No LLM Yet)                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  m_general__candidacy  ──┐                                      │
│                          ├──► Blocking ──► All candidate pairs  │
│  stg_ddhq__election_*  ──┘    (state + date)                    │
│                                                                 │
│  Output: ~100K-1M candidate pairs with basic similarity scores  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 2: Stratified Random Sample                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Sample ~5,000-10,000 pairs:                                    │
│                                                                 │
│  • Random baseline (30%)                                        │
│  • High name similarity, same state (25%)                       │
│  • Partial name match, same office type (25%) ← ambiguous       │
│  • Same state, different name (20%) ← likely negatives          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 3: LLM Labeling (One-Time Cost)                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Send sampled pairs to Gemini Flash for labeling:               │
│                                                                 │
│  Input:  "Is John Smith (CA Governor 2024) the same as          │
│           Jonathan Smith (California Governor race)?"           │
│                                                                 │
│  Output: { is_match: true/false, confidence: 0-100, reason }    │
│                                                                 │
│  Cost: ~5,000 pairs × $0.00014/pair ≈ $0.70 total              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 4: Train Splink                                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Use 5,000 labeled pairs to learn:                              │
│  • P(match | exact last name) = 0.85                            │
│  • P(match | same state) = 0.60                                 │
│  • P(match | similar office) = 0.70                             │
│  • etc.                                                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 5: Apply to Full Dataset                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Splink scores all ~100K-1M pairs                               │
│  Only send uncertain pairs (10-20%) to LLM                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Identifying Ambiguous Pairs (Pre-LLM Heuristics)

Before sending pairs to the LLM for labeling, identify ambiguous cases using simple heuristics:

```sql
-- int__splink_sample_candidates.sql
with candidate_pairs as (
    select
        h.gp_candidacy_id,
        h.first_name as hubspot_first_name,
        h.last_name as hubspot_last_name,
        h.candidate_office as hubspot_office,
        h.state as hubspot_state,
        d.candidate as ddhq_candidate,
        d.race_name as ddhq_race_name,
        d.candidate_id as ddhq_candidate_id,

        -- Simple similarity metrics for stratification
        levenshtein(lower(h.last_name), lower(split_part(d.candidate, ' ', -1))) as last_name_distance,
        levenshtein(lower(h.first_name), lower(split_part(d.candidate, ' ', 1))) as first_name_distance,

        -- Ambiguity classification
        case
            when levenshtein(lower(h.last_name), lower(split_part(d.candidate, ' ', -1))) = 0
                 and levenshtein(lower(h.first_name), lower(split_part(d.candidate, ' ', 1))) = 0
            then 'exact_match'
            when levenshtein(lower(h.last_name), lower(split_part(d.candidate, ' ', -1))) <= 2
            then 'close_match'  -- ambiguous zone
            when levenshtein(lower(h.last_name), lower(split_part(d.candidate, ' ', -1))) <= 5
            then 'partial_match'  -- ambiguous zone
            else 'different'
        end as ambiguity_bucket

    from {{ ref('m_general__candidacy') }} h
    cross join {{ ref('stg_airbyte_source__ddhq_gdrive_election_results') }} d
    where h.state = d.state
      and h.election_date = d.election_date
),

stratified_sample as (
    -- Random baseline (30%)
    (select *, 'random' as sample_type from candidate_pairs order by random() limit 1500)
    union all
    -- Exact/close matches (25%)
    (select *, 'high_similarity' as sample_type from candidate_pairs
     where ambiguity_bucket in ('exact_match', 'close_match') order by random() limit 1250)
    union all
    -- Ambiguous zone (25%)
    (select *, 'ambiguous' as sample_type from candidate_pairs
     where ambiguity_bucket = 'partial_match' order by random() limit 1250)
    union all
    -- Likely negatives (20%)
    (select *, 'likely_negative' as sample_type from candidate_pairs
     where ambiguity_bucket = 'different' order by random() limit 1000)
)

select * from stratified_sample
```

### Hybrid Approach: Combine Existing + New Labels

The best approach may combine existing high-confidence LLM matches with targeted new sampling:

```
Training Data Composition:
├── 3,000 pairs from existing high-confidence LLM matches (≥90)
│   └── Advantage: Free, already available
├── 2,000 pairs from random sample (unbiased)
│   └── Advantage: Captures full distribution
└── 2,000 pairs from ambiguity-targeted sample (decision boundary)
    └── Advantage: Maximizes learning at edge cases
────────────────────────────────────────────────────────────────
Total: 7,000 labeled pairs
```

### Cost Comparison

| Approach | LLM Labeling Cost | Quality |
|----------|-------------------|---------|
| Existing high-confidence only | $0 (already done) | Good, but biased |
| Pure random sample (5K) | ~$0.70 | Unbiased, but may miss edge cases |
| Stratified sample (5K) | ~$0.70 | Best coverage of decision boundary |
| Hybrid (3K existing + 4K new) | ~$0.56 | Best of both worlds |

### Recommended Approach

1. **Start with existing data**: Use high-confidence LLM matches (≥90) as initial training set
2. **Train baseline Splink model**: Get a working model quickly
3. **Identify gaps**: Run Splink on full dataset, find cases where it's uncertain
4. **Targeted sampling**: Send a stratified sample of uncertain cases to LLM for labeling
5. **Retrain**: Incorporate new labels and retrain Splink
6. **Iterate**: Repeat steps 3-5 until model performance stabilizes

This iterative approach minimizes upfront LLM costs while progressively improving model quality.
