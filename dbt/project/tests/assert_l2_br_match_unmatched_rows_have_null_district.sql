-- is_matched is the only predicate for match state, so the district columns must be
-- null exactly when is_matched is false. The raw snapshot spells "no match" as the
-- literal NOT_MATCHED in both columns; staging nulls it. Asserting both directions
-- means a future snapshot that reintroduces the sentinel, or that ships a matched row
-- with no district, fails here rather than downstream, where a `district_name is not
-- null` filter would quietly count unmatched positions as matched.
select id, is_matched, l2_district_type, l2_district_name
from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }}
where
    (
        not coalesce(is_matched, false)
        and (l2_district_type is not null or l2_district_name is not null)
    )
    or (is_matched and (l2_district_type is null or l2_district_name is null))
