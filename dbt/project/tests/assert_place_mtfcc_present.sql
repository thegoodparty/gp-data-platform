-- dbt/project/tests/assert_place_mtfcc_present.sql
-- DATA-1950: int__place_fast_facts routes each place into the G4110 vs non-G4110
-- branch by mtfcc. mtfcc is the only branch signal and is non-null for every place
-- today. A null mtfcc would fall into the non-G4110 (substring) path and, if the
-- place is actually an incorporated city, inherit a coincidental wrong county that
-- the G4110 guard tests (which filter on mtfcc = 'G4110') cannot see. Fail loudly if
-- an ingestion gap ever introduces a null so its routing is decided consciously
-- rather than defaulting silently to the legacy path.
select database_id, name, state
from {{ ref("stg_airbyte_source__ballotready_api_place") }}
where mtfcc is null
