/*
Districts on a proposed map that the adoption seed records as the one legally in
force, aggregated to the same shape and id as int__l2_district_aggregations so
the dimension can union the two.

Deliberately its own model, and a full rebuild rather than incremental. A seed
edit does not move a voter's loaded_at, so an incremental branch would not pick
up a state flipped to verified until that state's voter data happened to reload
-- which could be weeks, and the election is in November. Rebuilding is the only
way to make a seed edit take effect promptly.

That is also why this does not live inside int__l2_district_aggregations. That
model is incremental-merge and never deletes, so it holds districts L2 has since
retired, and --full-refresh on it drops them: verified 2026-08-21, where a full
refresh dropped a Florida hospital authority that no longer appears in the voter
file but that six l2_br_match_overrides rows still point at, turning the override
guard red. Keeping the proposed districts separate means a seed flip rebuilds
only this model and cannot take that with it.
*/
with
    adopted_proposed_districts as (
        select
            l2.state_postal_code,
            {{ proposed_district_minted_type("l2.proposed_district") }}
            as district_type,
            {{ proposed_district_type("l2.proposed_district") }}
            as shadowed_district_type,
            {{ proposed_district_number("l2.proposed_district") }} as district_name,
            count(distinct l2.lalvoterid) as voter_count,
            count(
                distinct case
                    when
                        l2.votertelephones_cellphoneformatted is not null
                        and trim(l2.votertelephones_cellphoneformatted) != ''
                    then l2.votertelephones_cellphoneformatted
                end
            ) as unique_cellphones,
            count(
                distinct case
                    when
                        l2.votertelephones_landlineformatted is not null
                        and trim(l2.votertelephones_landlineformatted) != ''
                    then l2.votertelephones_landlineformatted
                end
            ) as unique_landlines,
            max(l2.loaded_at) as loaded_at
        from {{ ref("int__l2_nationwide_uniform") }} as l2
        -- The gate. Type comes from parsing the value, never from the column
        -- name, because one vendor column carries congressional and state senate
        -- together plus local annexation records that are not seats at all.
        -- Ungated this would mint Virginia's struck-down map and every one of
        -- those annexation values.
        inner join
            {{ ref("district_map_adoption") }} as adoption
            on adoption.state = l2.state_postal_code
            and adoption.district_type
            = {{ proposed_district_type("l2.proposed_district") }}
        where
            {{ is_proposed_handled_district("l2.proposed_district") }}
            and adoption.adopted_source = 'proposed'
            and adoption.is_verified
            -- Sparse key: an empty district_number covers the whole state, a
            -- value narrows the decision to that one district.
            and (
                cast(nullif(trim(adoption.district_number), '') as int) is null
                or cast(nullif(trim(adoption.district_number), '') as int)
                = cast({{ proposed_district_number("l2.proposed_district") }} as int)
            )
        group by 1, 2, 3, 4
    )

select
    {{
        generate_salted_uuid(
            fields=[
                "adopted_proposed_districts.state_postal_code",
                "adopted_proposed_districts.district_type",
                "adopted_proposed_districts.district_name",
            ]
        )
    }} as id,
    state_postal_code,
    district_type,
    -- The current type this district replaces. Carried as a column so the one
    -- place that knows the mapping is the one that mints it: consumers needing
    -- the same-numbered current district (the turnout carry-over) join on this
    -- rather than restating the pairing.
    shadowed_district_type,
    district_name,
    voter_count,
    unique_cellphones,
    unique_landlines,
    loaded_at
from adopted_proposed_districts
