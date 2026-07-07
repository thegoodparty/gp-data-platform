-- Long-format person <-> source-record map. One row per member record of a
-- person group (gp_person_id, source_name, source_id), including records whose
-- scalar identifier column in `people` was nulled by within-group ambiguity.
-- See canonical-person-plan.md decision 5.
select
    gp_person_id,
    source_name,
    substring_index(record_key, '|', -1) as source_id,
    record_key,
    first_seen_at
from {{ ref("int__civics_person_canonical_ids") }}
