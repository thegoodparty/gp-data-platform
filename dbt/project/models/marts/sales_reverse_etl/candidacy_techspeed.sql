-- Civics -> TechSpeed sendable mart (reverse-ETL flow `candidacy_techspeed`).
-- Sends missing-contact, non-major-party, future-election candidacies not already in
-- HubSpot and that TechSpeed does not already have, for phone/email enrichment.
--
-- Output preserves the legacy 42-column shape and order so the TechSpeed import is
-- unchanged; 24 BallotReady-specific columns are null-filled (partner-confirmed
-- unused).
-- Appended: match-back key (candidate_code), reverse-ETL tracking-key inputs
-- (election_year, election_stage), gp_candidacy_id, is_keyable, last_activity_at.
--
-- WINDOW: 16-day rolling on feed_activity_at -- interim/manual-era dedup; the
-- reverse-ETL sent_log anti-join replaces it later (the ALREADY-SENT swap point
-- below). feed_activity_at is the latest real provider EVENT time: for vendor-sourced
-- rows created_at/updated_at are pipeline extract stamps, so a vendor re-delivering
-- its whole file re-stamps its entire universe as active today and floods this feed.
-- The BallotReady intermediate's additive vendor_activity_at column carries the event
-- time instead; gp_api rows already carry real product timestamps. There is no
-- TechSpeed leg -- the ALREADY-SENT filter below excludes exactly the candidacies one
-- could match -- so this feed's recency is BallotReady's event time or the canonical
-- fallback. The window is bounded above as well, since the non-vendor legs are not
-- clamped and one future-dated value would otherwise never leave the window.
with
    civics_base as (
        select
            cy.gp_candidacy_id,
            cy.gp_candidate_id,
            cy.official_office_name,
            cy.candidate_office,
            cy.office_level,
            cy.office_type,
            cy.party_affiliation,
            cy.primary_election_date,
            cy.general_election_date,
            cy.created_at,
            cy.updated_at,
            c.first_name,
            c.last_name,
            c.state,
            c.email,
            c.phone_number,
            e.is_judicial,
            e.seats_available,
            br_int.br_candidacy_id,
            br_int.br_race_id,
            br_int.br_position_database_id,
            -- Destination-facing recency: greatest available provider EVENT time,
            -- falling back to the extract-stamped canonical timestamps only where
            -- no provider supplies one. greatest() skips nulls, so the fallback
            -- fires only when every leg is null, which keeps this non-null.
            --
            -- No TechSpeed leg here, deliberately: the ALREADY-SENT filter below
            -- keeps only candidacies with no 'techspeed' in source_systems, and
            -- the candidacy mart sets that flag exactly when a row exists in the
            -- TechSpeed intermediate -- so a join to it can never match. Add the
            -- leg back when that filter becomes the sent_log anti-join.
            coalesce(
                greatest(
                    br_int.vendor_activity_at,
                    case
                        when cy.candidate_id_source = 'gp_api'
                        then greatest(cy.created_at, cy.updated_at)
                    end
                ),
                greatest(cy.created_at, cy.updated_at)
            ) as feed_activity_at
        from {{ ref("candidacy") }} as cy
        join {{ ref("candidate") }} as c on cy.gp_candidate_id = c.gp_candidate_id
        left join {{ ref("election") }} as e on cy.gp_election_id = e.gp_election_id
        left join
            {{ ref("int__civics_candidacy_ballotready") }} as br_int
            on cy.gp_candidacy_id = br_int.gp_candidacy_id
        where
            -- missing at least one of email/phone (legacy: phone='' OR email='')
            not (
                (c.email is not null and c.email != '')
                and (c.phone_number is not null and c.phone_number != '')
            )
            -- non-major-party (inherited verbatim from the legacy feed)
            and (
                cy.party_affiliation is null
                or (
                    cy.party_affiliation not ilike '%democrat%'
                    and cy.party_affiliation not ilike '%republican%'
                )
            )
            -- future election: explicit OR (keeps any upcoming stage; not COALESCE)
            and (
                cy.general_election_date > current_date() + interval 3 day
                or cy.primary_election_date > current_date() + interval 3 day
            )
            -- belt-and-suspenders not-already-in-HubSpot
            and cy.hubspot_contact_id is null
            and not exists (
                select 1
                from {{ ref("int__hubspot_contacts") }} as hs
                where
                    (
                        c.email is not null
                        and c.email != ''
                        and lower(trim(hs.email)) = lower(trim(c.email))
                    )
                    or (
                        c.phone_number is not null
                        and c.phone_number != ''
                        and length(regexp_replace(c.phone_number, '[^0-9]', '')) >= 10
                        and regexp_replace(hs.phone_number, '[^0-9]', '')
                        = regexp_replace(c.phone_number, '[^0-9]', '')
                    )
                    or (
                        br_int.br_candidacy_id is not null
                        and hs.br_candidacy_id = try_cast(br_int.br_candidacy_id as int)
                    )
            )
            -- ALREADY-SENT FILTER (pluggable swap point): reverse-ETL
            -- replaces this
            -- single predicate with a sent_log anti-join. Today: exclude any
            -- candidacy TechSpeed
            -- has already touched -- the most current source signal that they hold
            -- the data.
            and not array_contains(cy.source_systems, 'techspeed')
    ),

    -- Recency filter lives here rather than in civics_base's WHERE so the window
    -- basis is defined exactly once: the same expression is filtered on and
    -- emitted, and the two cannot drift apart.
    in_window as (
        select *
        from civics_base
        where
            feed_activity_at >= current_date() - interval 16 day
            -- Bounded above as well: gp_api product timestamps and the canonical
            -- fallback are not clamped the way the vendor legs are, and a single
            -- future-dated value would otherwise satisfy the lower bound forever,
            -- putting one row on every export indefinitely.
            and feed_activity_at <= current_date() + interval 1 day
    )

select
    -- === legacy 42-column TechSpeed contract (shape + order preserved) ===
    cast(null as string) as id,
    b.br_candidacy_id as candidacy_id,
    cast(null as string) as election_id,
    cast(null as string) as election_name,
    -- emit the future date that satisfied the predicate (prefer general, matching
    -- legacy
    -- intent), not a blind coalesce that could emit a past general when a future
    -- primary
    -- qualified the row. NB: this is the TechSpeed contract date; election_stage
    -- below is
    -- the separate reverse-ETL dedup grain and may differ by design.
    case
        when b.general_election_date > current_date() + interval 3 day
        then b.general_election_date
        when b.primary_election_date > current_date() + interval 3 day
        then b.primary_election_date
        else coalesce(b.general_election_date, b.primary_election_date)
    end as election_day,
    b.br_position_database_id as position_id,
    cast(null as string) as mtfcc,
    cast(null as string) as geo_id,
    b.official_office_name as position_name,
    cast(null as string) as sub_area_name,
    cast(null as string) as sub_area_value,
    cast(null as string) as sub_area_name_secondary,
    cast(null as string) as sub_area_value_secondary,
    b.state as state,
    b.office_level as level,
    cast(null as string) as tier,
    b.is_judicial as is_judicial,
    cast(null as boolean) as is_retention,
    b.seats_available as number_of_seats,
    cast(null as string) as normalized_position_id,
    b.candidate_office as normalized_position_name,
    b.br_race_id as race_id,
    cast(null as string) as geofence_id,
    cast(null as boolean) as geofence_is_not_exact,
    cast(null as boolean) as is_primary,
    cast(null as boolean) as is_runoff,
    cast(null as boolean) as is_unexpired,
    cast(null as int) as candidate_id,
    b.first_name as first_name,
    cast(null as string) as middle_name,
    cast(null as string) as nickname,
    b.last_name as last_name,
    cast(null as string) as suffix,
    b.phone_number as phone,
    b.email as email,
    cast(null as string) as image_url,
    b.party_affiliation as parties,
    cast(null as string) as urls,
    cast(null as string) as election_result,
    cast(b.created_at as string) as candidacy_created_at,
    cast(b.updated_at as string) as candidacy_updated_at,
    current_timestamp() as upload_datetime,

    -- === appended: match-back key + reverse-ETL key inputs + recency ===
    -- 4-part deterministic slug (no city) -- matches the reverse-ETL sent_log
    -- tracking key.
    {{
        generate_candidate_code(
            "b.first_name", "b.last_name", "b.state", "b.office_type"
        )
    }} as candidate_code,
    candidate_code is not null as is_keyable,
    -- election_year / election_stage = the nearest-upcoming stage (reverse-ETL dedup
    -- grain),
    -- deliberately distinct from election_day's legacy prefer-general date.
    year(
        coalesce(
            case
                when b.primary_election_date >= current_date()
                then b.primary_election_date
            end,
            b.general_election_date,
            b.primary_election_date
        )
    ) as election_year,
    case
        when b.primary_election_date >= current_date() then 'primary' else 'general'
    end as election_stage,
    b.gp_candidacy_id,
    b.gp_candidate_id,
    -- Name kept: ops sorts and filters on it. Only the basis improves -- it is now
    -- the provider event time rather than the pipeline extract stamp.
    b.feed_activity_at as last_activity_at
from in_window as b
