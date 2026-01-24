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
