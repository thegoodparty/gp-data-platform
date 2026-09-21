-- Everything about a Win signup that is knowable at the moment they sign up,
-- at user grain. Four blocks: product-side shapes, name and email shapes,
-- geographic consistency, and race timing.
--
-- The organising rule for the whole model is that a column must be knowable
-- at signup and must not move afterwards. Where a value is genuinely unknown
-- it stays null and carries a companion indicator; it is never filled with a
-- zero, because a zero that means "unknown" is indistinguishable from a real
-- zero and the model learns the coverage of our pipeline instead of the
-- behavior of the candidate.
--
-- The word lists (role mailboxes, office vocabulary, free email domains) and
-- the area-code seed are a-priori assets: public knowledge written down
-- before any outcome rate was computed, never grown by inspecting flagged
-- rows.
with
    -- One row per user. 0.26% of users have more than one campaign; the
    -- race-level columns describe the campaign chosen here, and the mart
    -- surfaces its campaign_id so a consumer can see which one.
    users as (
        select
            u.user_id,
            u.campaign_id,
            cast(u.user_created_at as date) as signup_date,
            u.campaign_state,
            u.user_zip,
            lower(trim(u.user_email)) as email,
            u.user_first_name,
            u.user_last_name,
            -- Product election dates are user-entered and a couple of dozen are
            -- impossible (years 0024 and 4444, a JavaScript max-date overflow at
            -- +275760). Unchecked they become absurd day counts that would wreck
            -- any scaler reading this table. A date more than two years before or
            -- twenty years after the signup is not a race this person is entering,
            -- so it reads as an unknown election date, which this column already
            -- has a meaning for. Dates merely a season or two in the past are left
            -- alone: those are real elections a user mis-selected, not corruption.
            case
                when
                    coalesce(
                        u.general_election_date,
                        u.election_date
                    ) between add_months(
                        cast(u.user_created_at as date), -24
                    ) and add_months(cast(u.user_created_at as date), 240)
                then coalesce(u.general_election_date, u.election_date)
            end as election_day,
            u.general_election_date,
            cast(u.ballotready_position_id as bigint) as br_position_id
        from {{ ref("users_win_candidacy") }} as u
        where u.is_latest_version and not coalesce(u.is_demo, false)
        qualify
            row_number() over (
                partition by u.user_id
                order by u.election_date desc nulls last, u.campaign_id desc
            )
            = 1
    ),

    -- ---------------------------------------------------------------- --
    -- Block 1: product-side shapes                                      --
    -- ---------------------------------------------------------------- --
    -- The campaign `details` map is written at campaign creation. Only keys
    -- the user types themselves are read below; race scaffolding the product
    -- copies in from the selected race encodes instrumentation era rather
    -- than candidate behavior.
    campaign_details as (
        select u.user_id, from_json(c.details, 'map<string,string>') as det
        from users as u
        left join
            {{ ref("stg_airbyte_source__gp_api_db_campaign") }} as c
            on cast(c.id as string) = cast(u.campaign_id as string)
    ),

    phone_digits as (
        select
            u.user_id,
            u.campaign_state,
            regexp_replace(coalesce(gu.phone, ''), '[^0-9]', '') as digits
        from users as u
        left join
            {{ ref("stg_airbyte_source__gp_api_db_user") }} as gu
            on cast(gu.id as string) = cast(u.user_id as string)
    ),

    phone_area_code as (
        select
            user_id,
            campaign_state,
            case
                when length(national) = 10 then substring(national, 1, 3)
            end as area_code
        from
            (
                select
                    user_id,
                    campaign_state,
                    case
                        when length(digits) = 11 and digits like '1%'
                        then substring(digits, 2)
                        else digits
                    end as national
                from phone_digits
            )
    ),

    phone_features as (
        select
            p.user_id,
            cast(p.area_code is not null as int) as usr_phone_present,
            -- Null when there is no parseable phone or the code is not in the
            -- seed; a real 0 means the code belongs to a different state than
            -- the declared one, which is a relocation or a virtual number.
            case
                when a.state_postal_code is not null and p.campaign_state is not null
                then cast(a.state_postal_code = p.campaign_state as double)
            end as usr_phone_area_code_matches_state
        from phone_area_code as p
        left join {{ ref("nanp_area_code_states") }} as a on a.area_code = p.area_code
    ),

    -- ---------------------------------------------------------------- --
    -- Block 3: geographic consistency                                   --
    -- ---------------------------------------------------------------- --
    -- Precise zip -> state, taken from where L2 voters in that zip actually
    -- live. A zip straddling a state line resolves to its larger side.
    zip_state as (
        select zip_code, state_postal_code
        from {{ ref("int__zip_code_to_l2_district") }}
        qualify
            row_number() over (
                partition by zip_code order by voters_in_zip_district desc
            )
            = 1
    ),

    -- Amplitude geo is IP-derived, so server-emitted events carry none and
    -- the columns only exist from 2025-04-24 on. Take the earliest
    -- geo-bearing event in a window around signup rather than the user's
    -- first-ever event, so pre-signup anonymous browsing does not disqualify
    -- them.
    signup_geo as (
        select u.user_id, e.region, e.country
        from users as u
        inner join
            {{ ref("stg_airbyte_source__amplitude_api_events") }} as e
            on try_cast(e.user_id as bigint) = u.user_id
            and cast(e.event_time as date)
            between date_add(u.signup_date, -1) and date_add(u.signup_date, 30)
        where e.region is not null
        qualify row_number() over (partition by u.user_id order by e.event_time) = 1
    ),

    -- ---------------------------------------------------------------- --
    -- Block 4: race timing and opponent arrivals                        --
    -- ---------------------------------------------------------------- --
    race_general as (
        select
            r.database_id as br_race_id,
            r.position.databaseid as br_position_id,
            e.election_day
        from {{ ref("stg_airbyte_source__ballotready_api_race") }} as r
        inner join
            {{ ref("stg_airbyte_source__ballotready_api_election") }} as e
            on r.election.databaseid = e.database_id
        where
            not coalesce(r.is_primary, false)
            and not coalesce(r.is_runoff, false)
            and not coalesce(r.is_recall, false)
    ),

    race_filing_period_ids as (
        select r.database_id as br_race_id, max(fp.databaseid) as filing_period_id
        from {{ ref("stg_airbyte_source__ballotready_api_race") }} as r
        lateral view explode(r.filing_periods) as fp
        group by r.database_id
    ),

    race_deadline as (
        select rg.br_position_id, rg.election_day, max(p.end_on) as filing_deadline
        from race_general as rg
        left join race_filing_period_ids as fi on fi.br_race_id = rg.br_race_id
        left join
            {{ ref("int__ballotready_filing_period") }} as p
            on p.database_id = fi.filing_period_id
        group by rg.br_position_id, rg.election_day
    ),

    -- How much of a race's eventual BR roster had landed by the filing
    -- deadline. Only the by-deadline count is knowable at signup.
    race_arrivals as (
        select
            rg.br_position_id,
            rg.election_day,
            sum(
                case
                    when
                        rd.filing_deadline is not null
                        and cast(cv.candidacy_created_at as date) <= rd.filing_deadline
                    then 1
                    else 0
                end
            ) as n_arrivals_by_deadline
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }} as cv
        inner join race_general as rg on cv.br_race_id = cast(rg.br_race_id as string)
        left join
            race_deadline as rd
            on rd.br_position_id = rg.br_position_id
            and rd.election_day = rg.election_day
        group by rg.br_position_id, rg.election_day
    ),

    -- The subject's OWN BR candidacy, reached by the same join path as the
    -- aggregate so that subtracting it can never go negative. It must be
    -- subtracted: br_candidacy_id is one of the ingredients of the
    -- corroboration label, so counting yourself feeds the label back in.
    own_candidacy as (
        select
            cs.gp_candidacy_id,
            rg.br_position_id,
            rg.election_day,
            min(cast(cv.candidacy_created_at as date)) as own_created_on
        from {{ ref("candidacy_stage") }} as cs
        inner join
            {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }} as cv
            on cast(cv.br_candidacy_id as string) = cast(cs.br_candidacy_id as string)
        inner join race_general as rg on cv.br_race_id = cast(rg.br_race_id as string)
        where cs.br_candidacy_id is not null
        group by cs.gp_candidacy_id, rg.br_position_id, rg.election_day
    ),

    user_candidacy as (
        select u.user_id, cand.gp_candidacy_id
        from users as u
        inner join
            {{ ref("candidacy") }} as cand
            on u.campaign_id = cand.product_campaign_id
            and u.general_election_date = cand.general_election_date
        qualify
            row_number() over (partition by u.user_id order by cand.gp_candidacy_id) = 1
    ),

    timing as (
        select
            u.user_id,
            u.signup_date,
            u.election_day,
            rd.filing_deadline,
            ra.n_arrivals_by_deadline,
            case
                when
                    oc.own_created_on is not null
                    and rd.filing_deadline is not null
                    and oc.own_created_on <= rd.filing_deadline
                then 1
                else 0
            end as own_counted_in_arrivals
        from users as u
        left join user_candidacy as uc on uc.user_id = u.user_id
        left join
            race_deadline as rd
            on rd.br_position_id = u.br_position_id
            and rd.election_day = u.election_day
        left join
            race_arrivals as ra
            on ra.br_position_id = u.br_position_id
            and ra.election_day = u.election_day
        left join
            own_candidacy as oc
            on oc.gp_candidacy_id = uc.gp_candidacy_id
            and oc.br_position_id = u.br_position_id
            and oc.election_day = u.election_day
    ),

    -- An arrival count of 0 where no deadline is known is a sentinel from the
    -- case expression above, which can never count, not an observed zero. A
    -- real zero needs a deadline to count against.
    arrivals as (
        select
            user_id,
            case
                when filing_deadline is null and coalesce(n_arrivals_by_deadline, 0) = 0
                then cast(null as double)
                else cast(n_arrivals_by_deadline - own_counted_in_arrivals as double)
            end as n_br_by_deadline
        from timing
    ),

    assembled as (
        select
            u.user_id,
            u.signup_date,
            u.campaign_id,
            u.br_position_id,

            -- Block 1
            pf.usr_phone_present,
            pf.usr_phone_area_code_matches_state,
            cast(
                split(u.email, '@')[0] in (
                    'info',
                    'admin',
                    'contact',
                    'office',
                    'hello',
                    'team',
                    'press',
                    'media',
                    'support',
                    'mail',
                    'campaign',
                    'elect',
                    'vote',
                    'donate',
                    'volunteer',
                    'staff',
                    'news'
                ) as int
            ) as usr_email_is_role_address,
            -- Scanned across the whole address: a custom campaign domain
            -- (janedoeforcouncil.com) is exactly the signal wanted.
            cast(
                coalesce(u.email, '')
                rlike 'council|mayor|sheriff|judge|senate|school'
                || '|schoolboard|board|commissioner|trustee|alderman|supervisor|clerk'
                || '|treasurer|assessor|coroner|constable|legislature|assembly|congress'
                || '|governor|elect|campaign|vote|4office|foroffice' as int
            ) as usr_email_has_office_words,
            cast(
                cd.det['occupation'] is not null
                and trim(cd.det['occupation']) not in ('', 'null') as int
            ) as det_has_occupation,
            cast(
                cd.det['pastExperience'] is not null
                and trim(cd.det['pastExperience']) not in ('', 'null') as int
            ) as det_has_past_experience,
            cast(
                cd.det['website'] is not null
                and trim(cd.det['website']) not in ('', 'null') as int
            ) as det_has_website,
            cast(
                cd.det['runningAgainst'] is not null
                and trim(cd.det['runningAgainst']) not in ('', 'null') as int
            ) as det_has_running_against,
            cast(
                cd.det['campaignCommittee'] is not null
                and trim(cd.det['campaignCommittee']) not in ('', 'null') as int
            ) as det_has_campaign_committee,
            cast(
                cd.det['customIssues'] is not null
                and trim(cd.det['customIssues']) not in ('', 'null') as int
            ) as det_has_custom_issues,
            cast(
                cd.det['funFact'] is not null
                and trim(cd.det['funFact']) not in ('', 'null') as int
            ) as det_has_fun_fact,

            -- Block 2
            lower(regexp_extract(u.email, '@(.+)$', 1)) as email_domain,
            trim(
                concat(
                    coalesce(u.user_first_name, ''), ' ', coalesce(u.user_last_name, '')
                )
            ) as full_name,

            -- Block 3
            case
                when zs.state_postal_code is not null and u.campaign_state is not null
                then cast(zs.state_postal_code = u.campaign_state as double)
            end as zip_campaign_state_match,
            case
                when st.state_postal_code is not null and u.campaign_state is not null
                then cast(st.state_postal_code = u.campaign_state as double)
            end as geo_state_match,
            case
                when
                    st.state_postal_code is not null
                    and zs.state_postal_code is not null
                then cast(st.state_postal_code = zs.state_postal_code as double)
            end as geo_zip_state_match,
            case
                when sg.user_id is not null and sg.country is not null
                then cast(sg.country != 'United States' as double)
            end as geo_country_nonus,
            cast(sg.user_id is null as int) as geo_missing,

            -- Block 4
            datediff(t.election_day, u.signup_date) as days_signup_to_election,
            datediff(t.filing_deadline, u.signup_date) as days_signup_to_deadline,
            cast(
                coalesce(u.signup_date > t.filing_deadline, false) as int
            ) as signed_up_after_deadline,
            cast(t.filing_deadline is not null as int) as has_deadline,
            ar.n_br_by_deadline
        from users as u
        left join campaign_details as cd on cd.user_id = u.user_id
        left join phone_features as pf on pf.user_id = u.user_id
        left join zip_state as zs on zs.zip_code = substring(trim(u.user_zip), 1, 5)
        left join signup_geo as sg on sg.user_id = u.user_id
        left join {{ ref("us_states") }} as st on st.state_name = sg.region
        left join timing as t on t.user_id = u.user_id
        left join arrivals as ar on ar.user_id = u.user_id
    )

select
    user_id,
    signup_date,
    campaign_id,
    br_position_id,

    usr_phone_present,
    usr_phone_area_code_matches_state,
    usr_email_is_role_address,
    usr_email_has_office_words,
    det_has_occupation,
    det_has_past_experience,
    det_has_website,
    det_has_running_against,
    det_has_campaign_committee,
    det_has_custom_issues,
    det_has_fun_fact,
    det_has_occupation
    + det_has_past_experience
    + det_has_website
    + det_has_running_against
    + det_has_campaign_committee
    + det_has_custom_issues
    + det_has_fun_fact as det_n_selfreport_keys,

    case
        when email_domain = 'goodparty.org'
        then 'goodparty'
        when
            email_domain in (
                'gmail.com',
                'yahoo.com',
                'hotmail.com',
                'outlook.com',
                'aol.com',
                'icloud.com',
                'msn.com',
                'comcast.net',
                'live.com',
                'protonmail.com',
                'proton.me',
                'me.com',
                'att.net',
                'sbcglobal.net',
                'verizon.net',
                'ymail.com',
                'mac.com',
                'cox.net'
            )
        then 'free'
        when email_domain = ''
        then 'missing'
        else 'custom'
    end as email_domain_class,

    -- Name shapes. Cheap tells for a placeholder or test signup, and none of
    -- them can change after the account is created.
    size(filter(split(full_name, '\\s+'), token -> token != '')) as name_n_tokens,
    length(full_name) as name_len,
    cast(full_name rlike '[0-9]' as int) as name_has_digit,
    cast(full_name rlike '[^A-Za-z .\'\\-]' as int) as name_has_nonalpha,
    cast(
        size(filter(split(full_name, '\\s+'), token -> length(token) = 1)) > 0 as int
    ) as name_single_char_token,

    zip_campaign_state_match,
    geo_state_match,
    geo_zip_state_match,
    geo_country_nonus,
    geo_missing,

    days_signup_to_election,
    days_signup_to_deadline,
    signed_up_after_deadline,
    has_deadline,
    n_br_by_deadline,
    cast(n_br_by_deadline is null as int) as br_arrivals_missing,
    case
        when n_br_by_deadline is not null then cast(n_br_by_deadline > 0 as double)
    end as any_br_by_deadline
from assembled
