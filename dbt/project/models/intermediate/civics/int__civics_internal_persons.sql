-- Persons that must never reach a public-facing feed: GoodParty staff and test
-- accounts. One row per person, so a public mart can anti-join it directly and
-- a test can assert the same set is absent.
--
-- Matched on the email *domain*, never the whole address. A substring match on
-- the address hard-excludes real candidates who put "goodparty" in the local
-- part (goodparty.jane@gmail.com and ~20 others today) from a public feed, and
-- silently. Matching the domain also keeps every GoodParty spelling without
-- enumerating them: goodparty.org, goodparty.com, the legacy thegoodparty.org
-- and .ca, and typos like goodparty.og.
--
-- mailinator is a disposable-address service used for test signups, so those
-- are test accounts by construction.
--
-- The admin and sales roles additionally catch internal users who signed up
-- with a personal address. campaignManager is a product role held by a
-- candidate's own staff, so it is deliberately not treated as internal.
--
-- Resolved through every gp_api member of the person group rather than the
-- scalar people.gp_api_user_id, which is null whenever a group holds more than
-- one account: a staff member with two logins is still covered.
--
-- Demo campaigns are handled at their own grain instead of here. A demo-only
-- account is not a candidate, so the is_demo filters on the gp_api candidacy
-- feeds keep it out of the role flags, and out of any mart scoped to them.
with
    internal_users as (
        select cast(id as string) as gp_api_user_id
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
        where
            regexp_extract(lower(email), '@(.+)$', 1)
            ilike any ('%goodparty%', '%mailinator%')
            or arrays_overlap(
                from_json(roles, 'array<string>'), array('admin', 'sales')
            )
    )

select distinct ids.gp_person_id
from {{ ref("int__civics_person_canonical_ids") }} as ids
inner join
    internal_users as u on u.gp_api_user_id = substring_index(ids.record_key, '|', -1)
where ids.source_name = 'gp_api' and ids.gp_person_id is not null
