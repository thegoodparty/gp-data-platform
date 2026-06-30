-- ddhq-miss-audit: deterministic pre-classification of gp_api candidacy-stages
-- with no DDHQ match. Run via the Databricks SQL statement API. See SKILL.md for the
-- design rules this encodes and the fixtures that guard them.
--
-- PER-AUDIT PARAMETERS:
-- gp_api window  -> miss0 WHERE election_date BETWEEN '<start>' AND '<end>'
-- DDHQ lookback  -> dd0 WHERE election_date >= '<start - ~6mo>' (covers prior stages)
--
-- The matching entity is a candidacy_stage = person + office + election_date. Office
-- strings are NOT comparable across gp_api and DDHQ (different word order, different
-- locality embedding, divergent office_type vocab) -- resolving office identity is what
-- the probabilistic matcher is for. This audit therefore matches PEOPLE on name + state
-- + date only and never re-implements an office match to grade the matcher. Coverage
-- ("is this race in DDHQ at all") is a coarse locality-name presence signal, refined
-- downstream by web verification; it is never used for person matching.
with
    c as (select * from goodparty_data_catalog.er_source.clustered_candidacy_stages),
    cl as (
        select
            cluster_id,
            max(case when source_name = 'ddhq' then 1 else 0 end) hd,
            max(case when source_name = 'ballotready' then 1 else 0 end) hb,
            max(case when source_name = 'techspeed' then 1 else 0 end) ht
        from c
        group by 1
    ),
    -- generic words stripped from an office name to leave LOCALITY (place) tokens. Used
    -- ONLY for the coarse coverage signal, never for person matching. Locality naming
    -- still differs across sources, so coverage is a signal, not ground truth.
    w as (
        select
            array(
                'the',
                'for',
                'and',
                'of',
                'a',
                'an',
                'at',
                'seat',
                'place',
                'ward',
                'zone',
                'district',
                'subdistrict',
                'position',
                'precinct',
                'no',
                'number',
                'large',
                'general',
                'special',
                'election',
                'member',
                'school',
                'board',
                'council',
                'councilor',
                'councilman',
                'councilwoman',
                'councilmember',
                'assembly',
                'mayor',
                'mayoral',
                'supervisor',
                'trustee',
                'clerk',
                'treasurer',
                'judge',
                'court',
                'justice',
                'commissioner',
                'commission',
                'representative',
                'representatives',
                'congressional',
                'senate',
                'house',
                'legislature',
                'director',
                'directors',
                'education',
                'county',
                'city',
                'town',
                'village',
                'municipal',
                'township',
                'borough',
                'area',
                'public',
                'attorney',
                'auditor',
                'aldermanic',
                'alderman',
                'selectman',
                'selectmen',
                'freeholder',
                'constable',
                'marshal',
                'register',
                'coroner',
                'assessor',
                'controller',
                'comptroller',
                'sheriff'
            ) stop
    ),
    miss0 as (
        select
            g.unique_id,
            g.cluster_id,
            g.state,
            g.election_date dt,
            lower(trim(g.first_name)) fn,
            lower(trim(g.last_name)) ln,
            g.official_office_name office,
            g.office_level,
            cl.hb,
            cl.ht,
            -- office name -> distinct-by-overlap tokens >=3 chars
            filter(
                split(
                    regexp_replace(lower(g.official_office_name), '[^a-z0-9]+', ' '),
                    ' '
                ),
                x -> length(x) >= 3
            ) t
        from c g
        join cl on g.cluster_id = cl.cluster_id
        where
            g.source_name = 'gp_api'
            and g.election_date between '2026-01-01' and '2026-05-31'
            and cl.hd = 0
            -- exclude test users: implicit (@goodparty.org email). is_demo in
            -- int__civics_candidacy_gp_api is all-false, so it adds nothing here.
            and lower(coalesce(g.email, '')) not like '%@goodparty.org'
    ),
    miss as (select m.*, array_except(t, (select stop from w)) loc from miss0 m),
    dd0 as (
        select
            state_postal_code state,
            lower(trim(candidate_first_name)) fn,
            lower(trim(candidate_last_name)) ln,
            official_office_name ddhq_office,
            election_date dt,
            lower(cast(is_winner as string)) = 'true' win,
            filter(
                split(
                    regexp_replace(lower(official_office_name), '[^a-z0-9]+', ' '), ' '
                ),
                x -> length(x) >= 3
            ) t
        from goodparty_data_catalog.dbt.stg_airbyte_source__ddhq_gdrive_election_results
        where election_date >= '2025-06-01'
    ),
    dd as (select d.*, array_except(t, (select stop from w)) loc from dd0 d),
    -- same person (name + state) at ANY date. Office is NOT compared: a hit may be the
    -- same person under a differently-worded office OR a namesake. Web verification
    -- confirms identity; do not assume a name+state hit is the same candidacy_stage.
    person as (
        select
            m.unique_id,
            count(*) np,
            max(case when d.win then 1 else 0 end) won,
            max(case when d.dt = m.dt then 1 else 0 end) person_sd,
            max(d.ddhq_office) any_office
        from miss m
        join dd d on d.state = m.state and d.fn = m.fn and d.ln = m.ln
        group by m.unique_id
    ),
    -- coarse coverage: does DDHQ hold a race in this LOCALITY (place-token overlap),
    -- regardless of person. Cross-source locality naming is noisy, so this only
    -- pre-sorts C1 vs C2; web verification is the arbiter of whether someone ran.
    race as (
        select
            m.unique_id,
            max(case when arrays_overlap(d.loc, m.loc) then 1 else 0 end) race_any,
            1 has_state
        from miss m
        join dd d on d.state = m.state
        group by m.unique_id
    )
select
    m.unique_id,
    m.state,
    m.dt,
    m.fn,
    m.ln,
    m.office,
    m.office_level,
    m.hb,
    m.ht,
    coalesce(p.np, 0) np,
    coalesce(p.won, 0) won,
    coalesce(p.person_sd, 0) person_sd,
    coalesce(r.race_any, 0) race_any,
    coalesce(r.has_state, 0) has_state,
    -- ns_* mirror the person (name+state) hit for verify_contract.md compatibility
    coalesce(p.np, 0) ns_n,
    coalesce(p.won, 0) ns_won,
    p.any_office,
    case
        -- same person (name+state) + same date present in DDHQ but did not cluster: the
        -- only candidate matcher FN. Name-based, so verification must confirm identity.
        when coalesce(p.person_sd, 0) = 1
        then 'C3b_person_sameday_nearFN'
        -- same person (name+state) at a DIFFERENT date: we may already hold this
        -- candidacy at another date, or it is a namesake. won vs not split for the
        -- report.
        when coalesce(p.np, 0) > 0 and coalesce(p.won, 0) = 1
        then 'C3_singleton_WON'
        when coalesce(p.np, 0) > 0
        then 'C3_singleton_lost_or_unk'
        -- person absent from DDHQ, but DDHQ holds a race in this locality: on-ballot
        -- but
        -- absent from results, DDHQ omission, or product wrong-office. Web splits
        -- these.
        when coalesce(r.race_any, 0) = 1
        then 'C2_race_tracked_person_absent'
        -- no DDHQ rows for the state at all
        when coalesce(r.has_state, 0) = 0
        then 'C1_absent_for_state'
        -- office name was all generic words (e.g. "town clerk") so loc is empty and the
        -- coverage signal is indeterminate, not negative. Route to C2 so web
        -- verification
        -- decides, rather than falsely asserting the race is absent from DDHQ.
        when size(m.loc) = 0
        then 'C2_race_tracked_person_absent'
        -- state covered but no locality match: likely race not in DDHQ (coverage gap)
        else 'C1_race_not_tracked'
    end category
from miss m
left join person p on p.unique_id = m.unique_id
left join race r on r.unique_id = m.unique_id
