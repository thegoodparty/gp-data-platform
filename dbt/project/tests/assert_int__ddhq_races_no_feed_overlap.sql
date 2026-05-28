{{ config(severity="error", error_if=">0") }}

-- A race must not appear with both is_reported=true and is_reported=false.
-- The LEFT ANTI JOIN in int__ddhq_races should prevent overlap; a hit
-- means upstream produced a duplicate ddhq_race_id within one feed
-- (which the per-feed unique tests should also catch) or the dedup
-- logic regressed.
select ddhq_race_id
from {{ ref("int__ddhq_races") }}
group by ddhq_race_id
having count(distinct is_reported) > 1
