# BallotReady Stance Model

## Overview

This model retrieves and processes stance data from the CivicEngine API for political candidates. It contains information about candidates' positions on various issues, which can be used for voter education and political analysis.

## Data Source

- Primary source: CivicEngine GraphQL API
- Requires authentication with a valid API token
- Depends on the stg_airbyte_source__ballotready_s3_candidacies_v3 model for candidacy IDs

## Technical Details

### API Integration

The model uses GraphQL to query the CivicEngine API with the following pattern:

1. Encode candidacy IDs in the format required by the API
2. Send GraphQL queries to retrieve stance data
3. Process and structure the response
4. Add metadata including timestamps

### Incremental Loading

This model uses dbt's incremental loading pattern:

- For full refreshes, it processes all candidacies
- For incremental runs, it:
  - Only processes candidacies updated in the last 30 days
  - Preserves created_at timestamps for existing records
  - Updates updated_at timestamps for all records

### Schema

The model produces data with the following structure:

| Column | Description | Type |
|--------|-------------|------|
| candidacy_id | Unique identifier for the candidacy | INTEGER |
| stances | Array of stance objects with issue positions | ARRAY<STRUCT> |
| created_at | Timestamp when the record was first created | TIMESTAMP |
| updated_at | Timestamp when the record was last updated | TIMESTAMP |

Each stance object in the stances array contains:

- databaseId: The stance's unique ID in the source system
- id: Encoded ID for the stance
- issue: Struct containing issue data (databaseId, id)
- locale: Language/locale for the stance
- referenceUrl: URL to source material for the stance (if available)
- statement: The candidate's position statement
- candidacy_id: ID of the associated candidacy
- encoded_candidacy_id: Base64-encoded ID used by the API

## Monitoring & Maintenance

### Tests

This model has the following tests:
- Schema validation (not_null, unique constraints)
- Data quality tests for issue IDs and statements
- Row count validation

### Macros

Utility macros are available for monitoring and maintenance:
- get_stance_freshness(): Checks if stance data is up-to-date with candidacy data
- validate_api_connectivity(): Validates recent API connectivity
- monitor_stance_counts(): Monitors for anomalies in stance counts

### Troubleshooting

Common issues and solutions:

1. API Authentication Failures
   - Check the CE_API_TOKEN environment variable
   - Verify API token validity with the CivicEngine team

2. Missing Stance Data
   - Check the API logs for errors
   - Verify candidacy IDs exist in the source system

3. Slow Performance
   - The model requests data for each candidacy individually
   - For large datasets, consider batching requests or implementing pagination

## Dependencies

- stg_airbyte_source__ballotready_s3_candidacies_v3
- Environment variable: DBT_CE (API token)

## Contact

For questions or issues, contact the Data Engineering team.
