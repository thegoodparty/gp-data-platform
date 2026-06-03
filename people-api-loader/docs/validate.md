# Voter-DB Refresh Validation — 20260420

**Status:** FAIL
**Started:** 2026-04-22T15:56:19.091881+00:00
**Finished:** 2026-04-22T16:00:39.045079+00:00

## Checks

### row_counts_match_databricks — PASS

- **states:** `51`
- **mismatch_count:** `0`
- **mismatches:** `{}`

### schema_diff_clean — PASS

- **sample_table:** `VoterTX`
- **prod_cols:** `348`
- **new_cols:** `383`
- **target_cols:** `383`
- **missing_from_new:** `[]`
- **unexpected_in_new:** `[]`
- **retired_typos:** `['Landscaping_And_Lighting_Assessment_Distric', 'Water_Control__Water_Conservation', 'Water_Control__Water_Conservation_SubDistrict']`

### index_constraint_diff_clean — FAIL

- **sample_table:** `VoterTX`
- **prod_count:** `253`
- **new_count:** `253`
- **missing_from_new:** `['VoterTX_Landscaping_And_Lighting_Assessment_Distric_idx', 'VoterTX_Water_Control__Water_Conservation_SubDistrict_idx', 'VoterTX_Water_Control__Water_Conservation_idx']`

### sample_queries_pass — PASS

- **pass:** `{'party_filter': 'ok', 'gender_filter': 'ok', 'age_cast_filter': 'ok', 'district_lookup': 'ok', 'family_dedupe': 'ok', 'justice_of_the_peace': 'ok'}`
- **fail:** `{}`

### l2Type_coverage — FAIL

- **error_reading_org_districts:** `relation "public.org_districts" does not exist
LINE 1: SELECT DISTINCT "l2Type" FROM public.org_districts WHERE "l2...
                                      ^`
