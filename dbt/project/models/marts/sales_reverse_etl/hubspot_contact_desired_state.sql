-- What every HubSpot contact this flow owns SHOULD look like: one row per eligible
-- person, plus one held row per eligible candidacy we cannot resolve to a person.
-- Databricks is the source of truth and HubSpot reflects it, so this is a full sync,
-- not a net-new lead list: no activity window and no not-already-in-HubSpot filter.
-- The successor to candidacy_hubspot, which keeps running until the sync is enabled.
--
-- COLUMN NAMES ARE HUBSPOT INTERNAL PROPERTY NAMES. The sending app maps a row's
-- column names straight onto properties with no rename layer, so renaming a column
-- here renames a HubSpot property write, and any column that is not a property must
-- stay out of the sendable view. Values are NULL, never '', wherever we do not know
-- them: HubSpot reads '' as "clear this property".
{% set include_unknown_outcome = var("retl_include_unknown_outcome", false) %}
{% set contact_type_default = var("retl_contact_type_default", "Self-Filer Lead") %}
{% set contact_type_winner = var("retl_contact_type_winner", "Self-Filer Lead") %}

with
    eligible as (
        select
            cy.gp_candidacy_id,
            cy.gp_person_id,
            cy.gp_candidate_id,
            cy.candidacy_result,
            nullif(trim(c.first_name), '') as first_name,
            nullif(trim(c.last_name), '') as last_name,
            nullif(trim(c.phone_number), '') as phone_number,
            nullif(trim(c.email), '') as email,
            nullif(trim(c.website_url), '') as website_url,
            nullif(trim(c.linkedin_url), '') as linkedin_url,
            nullif(trim(c.facebook_url), '') as facebook_url,
            nullif(trim(c.instagram_handle), '') as instagram_handle,
            nullif(trim(c.twitter_handle), '') as twitter_handle,
            nullif(trim(c.street_address), '') as street_address,
            nullif(trim(c.state), '') as state,
            c.birth_date,
            -- HubSpot's dropdown stores exactly Tier 1..Tier 4. Case and spacing
            -- variants and a Tier 5 that has no option would be per-row
            -- INVALID_OPTION rejects on every run forever, so normalize what maps
            -- and omit the rest -- omitting leaves HubSpot's own value alone.
            nullif(
                regexp_extract(
                    upper(regexp_replace(c.candidate_id_tier, '[^A-Za-z0-9]', '')),
                    '^TIER([1-4])$',
                    1
                ),
                ''
            ) as candidate_id_tier_digit,
            nullif(trim(cy.party_affiliation), '') as party_affiliation,
            nullif(trim(cy.official_office_name), '') as official_office_name,
            nullif(trim(cy.candidate_office), '') as candidate_office,
            nullif(trim(cy.office_type), '') as office_type,
            nullif(trim(cy.office_level), '') as office_level,
            cy.is_incumbent,
            cy.is_partisan,
            cy.is_open_seat,
            cy.is_win_icp,
            cy.is_serve_icp,
            cy.is_win_supersize_icp,
            cy.primary_election_date,
            cy.general_election_date,
            cy.viability_score,
            nullif(trim(cy.score_viability_automated), '') as score_viability_automated,
            e.population,
            nullif(trim(e.city), '') as city,
            nullif(trim(e.district), '') as district,
            e.filing_deadline,
            e.election_date,
            e.seats_available,
            e.is_uncontested,
            e.number_of_opponents,
            nullif(
                trim(coalesce(br_int.br_race_id, ts_int.br_race_id)), ''
            ) as br_race_id,
            nullif(trim(ts_int.election_type), '') as election_type_ts,
            -- Digits only: the legacy right-5 slice turned a ZIP+4 like 12345-6789
            -- into '-6789'.
            regexp_replace(ts_int.postal_code, '[^0-9]', '') as postal_digits,
            -- Nearest-upcoming stage, the fallback basis for election_type.
            case
                when cy.primary_election_date >= current_date()
                then 'primary'
                when cy.general_election_date >= current_date()
                then 'general'
            end as election_stage,
            -- Destination-facing recency: greatest available provider EVENT time,
            -- falling back to the extract-stamped canonical timestamps only where
            -- no provider supplies one (DDHQ-only rows). Both greatest() calls
            -- skip nulls, so the fallback fires only when every leg is null,
            -- which keeps this non-null.
            coalesce(
                greatest(
                    br_int.vendor_activity_at,
                    ts_int.vendor_activity_at,
                    case
                        when cy.candidate_id_source = 'gp_api'
                        then greatest(cy.created_at, cy.updated_at)
                    end
                ),
                greatest(cy.created_at, cy.updated_at)
            ) as feed_activity_at
        from {{ ref("candidacy_scored") }} as cy
        join {{ ref("candidate") }} as c on cy.gp_candidate_id = c.gp_candidate_id
        left join {{ ref("election") }} as e on cy.gp_election_id = e.gp_election_id
        left join
            {{ ref("int__civics_candidacy_ballotready") }} as br_int
            on cy.gp_candidacy_id = br_int.gp_candidacy_id
        -- LEFT JOIN, never INNER: an INNER join here silently collapses the feed
        -- back to TechSpeed-sourced candidacies only.
        left join
            {{ ref("int__civics_candidacy_techspeed") }} as ts_int
            on cy.gp_candidacy_id = ts_int.gp_candidacy_id
        where
            -- Contactable, measured on the same normalized values the payload
            -- emits rather than the raw ones. Tested against the raw columns
            -- instead, a whitespace-only or malformed-only contact string admits a
            -- row whose payload then carries neither an email nor a phone.
            (
                c.email rlike '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'
                or nullif(trim(c.phone_number), '') is not null
            )
            -- non-major-party (inherited verbatim from the legacy feed)
            and (
                cy.party_affiliation is null
                or (
                    cy.party_affiliation not ilike '%democrat%'
                    and cy.party_affiliation not ilike '%republican%'
                )
            )
            -- Still in the race, or won. Outcome and dates are complementary, not
            -- substitutes: 'Lost' / 'Withdrew' / 'Not on Ballot' are terminal at any
            -- stage and drop out even with a later stage date on the row, while a
            -- NULL outcome is undetermined and rides on whether any stage is still
            -- ahead. Winners are kept deliberately -- a newly elected official is a
            -- Serve lead.
            and (
                cy.candidacy_result in ('Won', 'Runoff')
                or (
                    cy.candidacy_result is null
                    and greatest(
                        cy.primary_election_date,
                        cy.primary_runoff_election_date,
                        cy.general_election_date,
                        cy.general_runoff_election_date
                    )
                    >= current_date()
                )
                {%- if include_unknown_outcome %}
                    -- Past-election people whose outcome never reached us, admitted
                    -- by the growth ruling on that population.
                    or cy.candidacy_result is null
                {%- endif %}
            )
    ),

    -- One row per person. Unkeyed rows cannot collapse, so each is its own
    -- partition and rides through held; the prefix keeps a candidacy id from ever
    -- colliding with a person id. Survivor is the latest provider event, tie-broken
    -- deterministically so the chosen payload cannot oscillate between builds.
    survivors as (
        select *
        from eligible
        qualify
            row_number() over (
                partition by
                    coalesce(gp_person_id, concat('candidacy:', gp_candidacy_id))
                order by feed_activity_at desc, gp_candidacy_id asc
            )
            = 1
    ),

    -- The UNFILTERED staging relation on purpose: int__hubspot_contacts drops
    -- contacts with no name, and a backfilled no-name contact read as a create
    -- would put sales-owned fields on an update. HubSpot sends '' for an unset
    -- property, so blank has to be handled alongside null.
    contacts_carrying_a_person_id as (
        select id as hubspot_contact_id, nullif(trim(gp_person_id), '') as gp_person_id
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
        where nullif(trim(gp_person_id), '') is not null
    ),

    -- Distinct: one person can carry several contacts until the dedupe runs.
    person_ids_in_hubspot as (
        select distinct gp_person_id from contacts_carrying_a_person_id
    ),

    -- The mirror is a daily copy, so a contact created today is invisible to it
    -- until tomorrow; this flow's own log closes that window.
    already_sent as (
        select distinct tracking_key as gp_person_id
        from {{ source("reverse_etl", "sent_log_hubspot") }}
    ),

    -- Our id disagrees with the id already stamped on a contact this person is
    -- linked to: hold, rather than create a second contact under the newer id.
    -- Inert until the property is created and backfilled, which is the intended
    -- arming condition.
    person_id_mismatches as (
        select distinct pi.gp_person_id
        from {{ ref("person_identifiers") }} as pi
        join contacts_carrying_a_person_id as hc on hc.hubspot_contact_id = pi.source_id
        where pi.source_name = 'hubspot' and hc.gp_person_id <> pi.gp_person_id
    ),

    with_contact_state as (
        select
            b.*,
            -- Evidence sufficient to omit sales-owned fields, not proof the contact
            -- exists right now: after a sales deletion the log leg still reads
            -- "exists", so a recreation arrives update-shaped.
            (
                hs.gp_person_id is not null or sent.gp_person_id is not null
            ) as contact_exists,
            (
                b.gp_person_id is not null and mismatch.gp_person_id is null
            ) as is_sendable,
            case
                when b.gp_person_id is null
                then 'no_person_key'
                when mismatch.gp_person_id is not null
                then 'person_id_mismatch'
            end as hold_reason
        from survivors as b
        left join person_ids_in_hubspot as hs on hs.gp_person_id = b.gp_person_id
        left join already_sent as sent on sent.gp_person_id = b.gp_person_id
        left join
            person_id_mismatches as mismatch on mismatch.gp_person_id = b.gp_person_id
    )

select
    gp_person_id,

    -- Sales-owned: written at create, omitted from every update, so an edit sales
    -- made after talking to the candidate is never overwritten by our stale copy.
    case when contact_exists then null else first_name end as firstname,
    case when contact_exists then null else last_name end as lastname,
    case when contact_exists then null else phone_number end as phone,
    -- Create-only because a full sync would otherwise reassign every existing
    -- contact to one owner, most of which sales already routed to someone else.
    -- Must be a numeric HubSpot owner id, not an owner email.
    case
        when contact_exists
        then null
        else nullif('{{ env_var("DBT_CIVICS_HUBSPOT_CONTACT_OWNER_ID", "") }}', '')
    end as hubspot_owner_id,
    -- Create-only because HubSpot's Type is multi-select and a single-value write
    -- replaces the whole set a contact already carries. Both outcomes map to the
    -- same label until the growth ruling names a winner type.
    case
        when contact_exists
        then null
        when candidacy_result = 'Won'
        then '{{ contact_type_winner }}'
        else '{{ contact_type_default }}'
    end as type,

    -- Everything below is owned: written on every send, create or update.
    -- A malformed address is omitted like a blank; left in, it is a per-row reject
    -- on every run forever.
    case when email rlike '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$' then email end as email,
    case
        when is_incumbent then 'Incumbent' when not is_incumbent then 'Challenger'
    end as candidate_type,
    party_affiliation,
    case
        when candidate_id_tier_digit is not null
        then concat('Tier ', candidate_id_tier_digit)
    end as candidate_id_tier,
    website_url as website,
    linkedin_url,
    facebook_url,
    instagram_handle,
    twitter_handle as twitterhandle,
    birth_date,
    street_address as address,
    state,
    case when length(postal_digits) >= 5 then left(postal_digits, 5) end as zip,
    city,
    -- district, not candidate_district: both properties exist, and this is the one
    -- the feed's value already lives in on contacts. The ingest registry surfaces
    -- only the other one, which is a gap in what we read back, not a sign that we
    -- have been writing the wrong property.
    district,
    population,
    official_office_name,
    candidate_office,
    office_type,
    office_level,
    filing_deadline,
    election_date,
    primary_election_date,
    general_election_date,
    br_race_id,
    -- TS form value where present (legacy semantics); date-derived stage otherwise.
    coalesce(election_type_ts, initcap(election_stage)) as election_type,
    -- Enumeration spellings below are HubSpot's stored options, which are not
    -- uniform: 'UnContested' and lowercase 'partisan' are the real option values.
    case
        when is_uncontested then 'UnContested' when not is_uncontested then 'Contested'
    end as uncontested,
    case when is_open_seat then 'Yes' when not is_open_seat then 'No' end as open_seat,
    case
        when is_partisan then 'partisan' when not is_partisan then 'nonpartisan'
    end as partisan_type,
    try_cast(number_of_opponents as int) + 1 as number_of_candidates,
    seats_available as number_of_seats_available,
    viability_score as viability_score_numeric,
    score_viability_automated as viability_score_dropdown,
    case when is_win_icp then 'true' when not is_win_icp then 'false' end as win_icp,
    case
        when is_serve_icp then 'true' when not is_serve_icp then 'false'
    end as serve_icp,
    case
        when is_win_supersize_icp then 'true' when not is_win_supersize_icp then 'false'
    end as win_icp_supersize,

    -- Not payload: the sendable view excludes everything below, and anything added
    -- here must be excluded there too or it becomes a HubSpot property write.
    -- contact_exists is the create-vs-update shape the ownership cases above chose;
    -- it is what makes the omission auditable without re-deriving the joins.
    contact_exists,
    is_sendable,
    hold_reason,
    gp_candidacy_id,
    gp_candidate_id,
    feed_activity_at,
    current_timestamp() as built_at
from with_contact_state
