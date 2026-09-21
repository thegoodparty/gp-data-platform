-- BallotReady position attributes for the Win candidate feature layer, at
-- position grain. Cost of entry to a race (filing fees, signature counts,
-- paperwork burden, salary, seats) is a race property rather than candidate
-- behavior, so every column here is knowable before a signup exists.
--
-- Caveat carried in the column metadata: BR position records are a current
-- snapshot, so a fee or salary edited after a signup reads at its edited
-- value. Same class as voter_count and br_normalized_position_type.
--
-- Null discipline throughout: an unknown filing fee is not a zero fee, and a
-- position with no office-holder record has an unknown holder count, not zero
-- holders. Unknowns stay null and get a companion *_known indicator, because
-- filling them conflates "we did not observe it" with "it is absent".
with
    -- Only positions a Win signup has actually declared. The full BR position
    -- table is far larger and none of it can join to a user.
    declared_positions as (
        select distinct cast(ballotready_position_id as bigint) as br_position_id
        from {{ ref("users_win_candidacy") }}
        where
            is_latest_version
            and not coalesce(is_demo, false)
            and ballotready_position_id is not null
    ),

    positions as (
        select
            cast(pos.database_id as bigint) as br_position_id,
            pos.seats,
            pos.tier,
            pos.minimum_age,
            pos.maximum_filing_fee,
            pos.salary,
            pos.employment_type,
            pos.partisan_type,
            pos.level,
            pos.has_primary,
            pos.has_majority_vote_primary,
            pos.has_ranked_choice_general,
            pos.is_partisan,
            pos.is_judicial,
            pos.is_staggered_term,
            pos.has_unknown_boundaries,
            pos.must_be_registered_voter,
            pos.must_be_resident,
            pos.must_have_professional_experience,
            pos.filing_requirements,
            pos.eligibility_requirements,
            pos.paperwork_instructions,
            pos.election_frequencies
        from {{ ref("stg_airbyte_source__ballotready_api_position") }} as pos
        inner join
            declared_positions as d
            on d.br_position_id = cast(pos.database_id as bigint)
    ),

    -- election_frequencies holds PositionElectionFrequency ids, not position
    -- ids; the cadence values live in the intermediate model. Joining that
    -- model straight to a position id returns zero rows. Explode in its own
    -- CTE because a lateral view cannot be followed by a join.
    frequency_ids as (
        select br_position_id, ef.databaseid as freq_id
        from positions
        lateral view explode(positions.election_frequencies) as ef
    ),

    frequency as (
        select
            fi.br_position_id,
            -- frequency is an array: a position can carry several cadences.
            array_max(max(f.frequency)) as election_frequency_years
        from frequency_ids as fi
        inner join
            {{ ref("int__ballotready_position_election_frequency") }} as f
            on cast(f.database_id as string) = cast(fi.freq_id as string)
        group by fi.br_position_id
    ),

    holders as (
        select
            cast(br_position_id as bigint) as br_position_id,
            count(*) as holder_count,
            max(case when is_vacant then 1 else 0 end) as holder_any_vacant
        from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
        where br_position_id is not null
        group by 1
    ),

    parsed as (
        select
            p.br_position_id,
            p.seats,
            p.tier,
            p.minimum_age,
            p.maximum_filing_fee,
            p.employment_type,
            p.partisan_type,
            p.level,
            p.has_primary,
            p.has_majority_vote_primary,
            p.has_ranked_choice_general,
            p.is_partisan,
            p.is_judicial,
            p.is_staggered_term,
            p.has_unknown_boundaries,
            p.must_be_registered_voter,
            p.must_be_resident,
            p.must_have_professional_experience,
            p.filing_requirements,
            p.eligibility_requirements,
            p.paperwork_instructions,
            p.salary,
            lower(p.salary) as salary_lower,
            f.election_frequency_years,
            h.holder_count,
            h.holder_any_vacant,
            -- The boolean "mentions signatures" is true for 94% of positions and
            -- so cannot discriminate. The count is the feature: 25 signatures and
            -- 2,000 signatures are different barriers entirely.
            -- regexp_extract returns '' on no match, and casting '' errors; nullif
            -- turns a miss into the unknown it actually is.
            cast(
                nullif(
                    replace(
                        regexp_extract(
                            p.filing_requirements,
                            '(?i)([0-9][0-9,]*)\\s*(?:valid\\s+|registered\\s+voter\\s+)?signatures?',
                            1
                        ),
                        ',',
                        ''
                    ),
                    ''
                ) as double
            ) as signature_count,
            -- Salary is free text ("$127,134/year", "Only expenses", "Unpaid").
            -- Unparseable text stays null rather than becoming zero.
            cast(
                nullif(
                    replace(
                        regexp_extract(p.salary, '\\$\\s*([0-9,]+(?:\\.[0-9]+)?)', 1),
                        ',',
                        ''
                    ),
                    ''
                ) as double
            ) as salary_amount
        from positions as p
        left join frequency as f on f.br_position_id = p.br_position_id
        left join holders as h on h.br_position_id = p.br_position_id
    )

select
    br_position_id,

    -- Race structure
    seats as pos_seats,
    -- pos_selections_allowed is not carried: 0.9989 correlated with seats.
    tier as pos_tier,
    cast(minimum_age as double) as pos_min_age,
    cast(maximum_filing_fee as double) as pos_max_filing_fee,
    cast(maximum_filing_fee is not null as int) as pos_filing_fee_known,
    case
        when maximum_filing_fee is not null then cast(maximum_filing_fee = 0 as double)
    end as pos_filing_fee_is_zero,

    -- Cost-of-entry text. Length is a crude but honest proxy for procedural
    -- burden. Lengths only: the *_present indicators all sat at 99.8% present,
    -- so their minority class is far too small to split on.
    cast(length(filing_requirements) as double) as pos_filing_req_len,
    cast(length(eligibility_requirements) as double) as pos_eligibility_len,
    cast(length(paperwork_instructions) as double) as pos_paperwork_len,

    signature_count as pos_signature_count,
    cast(signature_count is not null as int) as pos_signature_count_known,

    -- Three-state booleans (true / false / unknown), kept as doubles so the
    -- unknown survives into the model frame.
    cast(has_primary as double) as pos_has_primary,
    cast(has_majority_vote_primary as double) as pos_majority_vote_primary,
    cast(has_ranked_choice_general as double) as pos_ranked_choice_general,
    cast(is_partisan as double) as pos_is_partisan,
    cast(is_judicial as double) as pos_is_judicial,
    -- is_appointed is not carried: constant false across every BR position.
    -- is_retention is not carried: 10 positives in the modeled population, and
    -- is_judicial already covers the judicial signal.
    cast(is_staggered_term as double) as pos_is_staggered_term,
    cast(has_unknown_boundaries as double) as pos_unknown_boundaries,
    cast(must_be_registered_voter as double) as pos_must_be_registered_voter,
    cast(must_be_resident as double) as pos_must_be_resident,
    cast(must_have_professional_experience as double) as pos_must_have_prof_experience,

    -- Salary: amount, period, and a three-way paid / expenses / unpaid class.
    salary_amount as pos_salary_amount,
    -- A leading boundary only, not a full word boundary. "$750/monthly",
    -- "board meetings" and "bi-weekly" are all real period statements that a
    -- trailing boundary would throw away (28 positions); requiring only that
    -- the word is not preceded by a letter drops "determined" -> term, which
    -- is the single genuine false positive in the corpus.
    case
        when salary_lower rlike '(^|[^a-z])year'
        then 'year'
        when salary_lower rlike '(^|[^a-z])month'
        then 'month'
        when salary_lower rlike '(^|[^a-z])week'
        then 'week'
        when salary_lower rlike '(^|[^a-z])day'
        then 'day'
        when salary_lower rlike '(^|[^a-z])hour'
        then 'hour'
        when salary_lower rlike '(^|[^a-z])meeting'
        then 'meeting'
        when salary_lower rlike '(^|[^a-z])session'
        then 'session'
        when salary_lower rlike '(^|[^a-z])term'
        then 'term'
        when salary_lower rlike '(^|[^a-z])annum'
        then 'annum'
    end as pos_salary_period,
    case
        when salary_lower rlike 'unpaid|no salary|none|volunteer'
        then 'unpaid'
        when salary_lower like '%expense%'
        then 'expenses_only'
        when salary_amount is not null
        then 'paid'
    end as pos_salary_class,
    cast(salary is not null as int) as pos_salary_known,

    employment_type as pos_employment_type,
    partisan_type as pos_partisan_type,
    level as pos_br_level,
    election_frequency_years as pos_election_frequency_years,

    -- Office-holder block. These are POSITION-level: they say how many holders
    -- a seat has, not whether this signup is one of them. The person-level
    -- version is int__win_candidate_incumbency.
    cast(holder_count as double) as pos_holder_count,
    cast(holder_count is not null as int) as pos_holder_known,
    cast(holder_any_vacant as double) as pos_holder_any_vacant
from parsed
