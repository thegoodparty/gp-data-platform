-- Long-format person <-> source-record map. One row per member record of a
-- person group (gp_person_id, source_name, source_id), including records whose
-- scalar identifier column in `people` was nulled by within-group ambiguity.
select
    ci.gp_person_id,
    ci.source_name,
    substring_index(ci.record_key, '|', -1) as source_id,
    ci.record_key,
    ci.first_seen_at,
    -- Which identity this record belonged to before any similarity merge.
    -- Two distinct values under one gp_person_id is exactly what a merge looks
    -- like from here, which makes one traceable back to what it fused.
    ci.identity_key
from {{ ref("int__civics_person_canonical_ids") }} as ci
