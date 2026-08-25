with
    source as (
        select * from {{ source("model_predictions", "llm_l2_br_match_results") }}
    ),

    -- Ordering must stay character-identical to `latest_attempt` in
    -- int__l2_br_match_pending_offices.sql: never by confidence or match
    -- state, and `nulls first` so an abstention wins a same-timestamp tie
    -- over a match. A future edit to one should find the other.
    latest_attempt as (
        select *
        from source
        qualify
            row_number() over (
                partition by br_database_id
                order by attempted_at desc, l2_district_name nulls first
            )
            = 1
    ),

    -- No ltrim/nullif here: the results table is written from canonical
    -- universe labels already, unlike the raw snapshot the dated sibling
    -- normalizes.
    renamed as (
        select
            br_database_id,
            -- The district's state, null on an abstain. NOT the office's
            -- state: consumers take that from the position side.
            l2_state,
            l2_district_type,
            l2_district_name,
            confidence,
            attempted_at,
            -- A populated district is what "matched" means now; there is no
            -- status column and none should be reintroduced.
            l2_district_name is not null as is_matched
        from latest_attempt
    )
select *
from renamed
