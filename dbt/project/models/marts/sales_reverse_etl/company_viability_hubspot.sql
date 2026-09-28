{{ config(materialized="view", tblproperties={}) }}

-- Viability 2.0 and race attributes -> HubSpot Company import feed (DATA-1976).
-- Companion to `candidacy_hubspot`: that feed CREATES contacts already carrying a
-- score, this one syncs onto companies that exist. Match on Record ID.
--
-- Attribute scope is set by conflict, not preference. gp-api's OutgoingProperty
-- enum (omni `crm.types.ts`) writes none of these, so there is no competing writer.
-- But it FETCHES `incumbent` and `number_of_opponents` via IncomingProperty, so
-- Databricks writing them would make it the source of two values product reads --
-- inverting the ticket's "Product owns the inputs" split. Those two are excluded
-- pending a call from product. open_seat, partisan and seats appear in neither
-- enum: no reader, no writer.
--
-- Null attributes emit as NULL, never as a negative. HubSpot's CSV import skips
-- empty cells, so an unknown stays untouched; casting null to 'No' would assert
-- "not an open seat" for ~9.9k races we know nothing about.
--
-- A view, against the schema's table default: ops reads it immediately before an
-- import and wants current values. Runs in under a second, so the sibling feeds'
-- reason for materializing (heavy anti-joins re-run by every test) does not apply.
-- Empty tblproperties: the schema's Delta column mapping is meaningless on a view.
with
    exploded as (
        select
            -- JSON array string, not a Spark array, and not single-element
            explode(from_json(cy.hubspot_company_ids, 'array<string>')) as record_id,
            cy.gp_candidacy_id,
            cy.viability_score,
            cy.is_open_seat,
            cy.is_partisan,
            e.seats_available,
            cy.updated_at,
            -- null once every stage is in the past
            least(
                case
                    when cy.primary_election_date >= current_date()
                    then cy.primary_election_date
                end,
                case
                    when cy.general_election_date >= current_date()
                    then cy.general_election_date
                end
            ) as next_election_date,
            greatest(
                cy.primary_election_date, cy.general_election_date
            ) as latest_election_date
        from {{ ref("candidacy_scored") }} as cy
        left join {{ ref("election") }} as e on cy.gp_election_id = e.gp_election_id
        where
            cy.viability_score is not null
            and cy.hubspot_company_ids is not null
            and trim(cy.hubspot_company_ids) not in ('', '[]')
    ),

    -- The 2025 HubSpot archive reuses company ids across cycles, so one id can be
    -- carried by several candidacies that disagree. Live cycle wins.
    ranked as (
        select
            *,
            row_number() over (
                partition by record_id
                order by
                    case when next_election_date is null then 1 else 0 end,
                    next_election_date asc,
                    latest_election_date desc,
                    updated_at desc,
                    gp_candidacy_id
            ) as rn
        from exploded
        where record_id is not null and trim(record_id) <> ''
    ),

    ours as (select * from ranked where rn = 1),

    -- Read the staging model's cast columns, not the raw properties, so every
    -- comparison below is like-typed rather than string-versus-boolean.
    theirs as (
        select
            cast(id as string) as record_id,
            try_cast(viability_score as double) as hs_score,
            is_open_seat as hs_open_seat,
            is_partisan as hs_partisan,
            seats_available as hs_seats
        from {{ ref("stg_airbyte_source__hubspot_api_companies") }}
    )

-- INNER join: a Record ID missing from the ingest no longer exists in HubSpot and
-- would only error on import. round() so float noise is not read as a change.
select
    o.record_id as `Record ID`,
    o.viability_score as viability_2_0,
    case
        when o.is_open_seat then 'Yes' when not o.is_open_seat then 'No'
    end as open_seat_,
    case
        when o.is_partisan then 'Partisan' when not o.is_partisan then 'Nonpartisan'
    end as partisan_np,
    -- 11 election rows carry 0 seats, which is a defect, not a race with no seats.
    -- Blank rather than push it: HubSpot already holds 9.4k such zeros.
    case when o.seats_available >= 1 then o.seats_available end as seats_available,
    t.hs_score as hubspot_current_viability
from ours as o
join theirs as t on o.record_id = t.record_id
where
    -- emit where any published value needs a write; equal_null treats a blank
    -- HubSpot value as a difference, which is what makes this fill and overwrite
    t.hs_score is null
    or round(t.hs_score, 2) <> round(o.viability_score, 2)
    or (o.is_open_seat is not null and not equal_null(o.is_open_seat, t.hs_open_seat))
    or (o.is_partisan is not null and not equal_null(o.is_partisan, t.hs_partisan))
    or (o.seats_available >= 1 and not equal_null(o.seats_available, t.hs_seats))
