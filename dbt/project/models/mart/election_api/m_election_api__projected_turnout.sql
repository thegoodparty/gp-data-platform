{{
    config(
        materialized="table",
        unique_key="id",
        auto_liquid_cluster=true,
        tags=["mart", "election_api", "projected_turnout"],
    )
}}


select
    id,
    state,
    l2_district_type,
    l2_district_name,
    projected_turnout,
    inference_at,
    election_year,
    election_code,
    model_version,
    br_position_id,
    geoid,
    created_at,
    updated_at
from {{ ref("int__projected_turnout") }}
{% if is_incremental() %}
    where updated_at = (select max(updated_at) from {{ this }})
{% endif %}
-- since model_version will change over time, we need to keep the latest version for
-- each br_position_id
qualify row_number() over (partition by br_position_id order by updated_at desc) = 1
