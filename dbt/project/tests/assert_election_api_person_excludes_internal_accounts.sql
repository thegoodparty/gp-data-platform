-- No internal or test account may reach the public /people profile feed. Staff
-- file real (non-demo) campaigns while testing, so the is_demo filters upstream
-- do not catch them and every such person would otherwise get a live public
-- page. Keyed off every gp_api member of the group rather than the scalar
-- gp_api_user_id, which is null whenever a person holds more than one account.
select p.id, p.slug
from {{ ref("m_election_api__person") }} as p
where
    p.id in (
        select ids.gp_person_id
        from {{ ref("person_identifiers") }} as ids
        inner join
            {{ ref("stg_airbyte_source__gp_api_db_user") }} as u
            on cast(u.id as string) = ids.source_id
        where
            ids.source_name = 'gp_api'
            and u.email ilike '%goodparty%'
            and ids.gp_person_id is not null
    )
