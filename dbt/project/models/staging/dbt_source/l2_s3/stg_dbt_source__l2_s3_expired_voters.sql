-- L2 publishes the omit list raw, so the blank rows and duplicates it carries are
-- dropped here.
select distinct trim(lalvoterid) as lalvoterid, loaded_at
from {{ source("dbt_source", "l2_s3_expired_voters") }}
where trim(coalesce(lalvoterid, '')) != ''
