{{
    config(
        materialized="table",
        tags=["intermediate", "entity_resolution"],
    )
}}

-- Entity Resolution Step 2: Deterministic matching with cascading priority rules.
-- Each record matches at most once, with higher-priority rules taking precedence.
with
    ts_records as (
        select *
        from {{ ref("int__er_match_features_union") }}
        where source_system = 'techspeed'
    ),

    br_records as (
        select *
        from {{ ref("int__er_match_features_union") }}
        where source_system = 'ballotready'
    ),

    -- Rule 1: BR race_id cross-reference + name match
    rule_1_br_race_id_xref as (
        select
            ts.record_id as ts_record_id,
            br.record_id as br_record_id,
            'rule_1_br_race_id_xref' as match_rule,
            1 as match_confidence_rank,
            'deterministic' as decided_by
        from ts_records ts
        inner join
            br_records br
            on ts.br_race_id is not null
            and ts.br_race_id = br.br_race_id
            and ts.first_name_clean = br.first_name_clean
            and ts.last_name_clean = br.last_name_clean
    ),

    -- Rule 2: Candidate code exact match
    rule_2_candidate_code as (
        select
            ts.record_id as ts_record_id,
            br.record_id as br_record_id,
            'rule_2_candidate_code' as match_rule,
            2 as match_confidence_rank,
            'deterministic' as decided_by
        from ts_records ts
        inner join
            br_records br
            on ts.candidate_code is not null
            and ts.candidate_code = br.candidate_code
        where
            -- Exclude records already matched by Rule 1
            ts.record_id not in (select ts_record_id from rule_1_br_race_id_xref)
            and br.record_id not in (select br_record_id from rule_1_br_race_id_xref)
    ),

    matched_through_2 as (
        select ts_record_id, br_record_id
        from rule_1_br_race_id_xref
        union all
        select ts_record_id, br_record_id
        from rule_2_candidate_code
    ),

    -- Rule 3: Name + State + Election date exact match
    rule_3_name_state_date as (
        select
            ts.record_id as ts_record_id,
            br.record_id as br_record_id,
            'rule_3_name_state_date' as match_rule,
            3 as match_confidence_rank,
            'deterministic' as decided_by
        from ts_records ts
        inner join
            br_records br
            on ts.first_name_clean = br.first_name_clean
            and ts.last_name_clean = br.last_name_clean
            and ts.state_abbr = br.state_abbr
            and ts.election_date = br.election_date
        where
            ts.record_id not in (select ts_record_id from matched_through_2)
            and br.record_id not in (select br_record_id from matched_through_2)
    ),

    matched_through_3 as (
        select ts_record_id, br_record_id
        from matched_through_2
        union all
        select ts_record_id, br_record_id
        from rule_3_name_state_date
    ),

    -- Rule 4: Email + State exact match
    rule_4_email_state as (
        select
            ts.record_id as ts_record_id,
            br.record_id as br_record_id,
            'rule_4_email_state' as match_rule,
            4 as match_confidence_rank,
            'deterministic' as decided_by
        from ts_records ts
        inner join
            br_records br
            on ts.email_clean = br.email_clean
            and ts.state_abbr = br.state_abbr
        where
            ts.email_clean is not null
            and ts.email_clean != ''
            and ts.record_id not in (select ts_record_id from matched_through_3)
            and br.record_id not in (select br_record_id from matched_through_3)
    ),

    matched_through_4 as (
        select ts_record_id, br_record_id
        from matched_through_3
        union all
        select ts_record_id, br_record_id
        from rule_4_email_state
    ),

    -- Rule 5: Phone + State exact match (10-digit phone)
    rule_5_phone_state as (
        select
            ts.record_id as ts_record_id,
            br.record_id as br_record_id,
            'rule_5_phone_state' as match_rule,
            5 as match_confidence_rank,
            'deterministic' as decided_by
        from ts_records ts
        inner join
            br_records br
            on ts.phone_clean = br.phone_clean
            and ts.state_abbr = br.state_abbr
        where
            ts.phone_clean is not null
            and length(ts.phone_clean) = 10
            and br.phone_clean is not null
            and length(br.phone_clean) = 10
            and ts.record_id not in (select ts_record_id from matched_through_4)
            and br.record_id not in (select br_record_id from matched_through_4)
    ),

    matched_through_5 as (
        select ts_record_id, br_record_id
        from matched_through_4
        union all
        select ts_record_id, br_record_id
        from rule_5_phone_state
    ),

    -- Rule 6: Name + State + Office type + Near-date (within 180 days)
    rule_6_name_state_office_neardate as (
        select
            ts.record_id as ts_record_id,
            br.record_id as br_record_id,
            'rule_6_name_state_office_neardate' as match_rule,
            6 as match_confidence_rank,
            'deterministic' as decided_by
        from ts_records ts
        inner join
            br_records br
            on ts.first_name_clean = br.first_name_clean
            and ts.last_name_clean = br.last_name_clean
            and ts.state_abbr = br.state_abbr
            and ts.office_type_clean = br.office_type_clean
        where
            ts.election_date is not null
            and br.election_date is not null
            and abs(datediff(ts.election_date, br.election_date)) <= 180
            and ts.record_id not in (select ts_record_id from matched_through_5)
            and br.record_id not in (select br_record_id from matched_through_5)
    ),

    -- Union all rules
    all_matches as (
        select *
        from rule_1_br_race_id_xref
        union all
        select *
        from rule_2_candidate_code
        union all
        select *
        from rule_3_name_state_date
        union all
        select *
        from rule_4_email_state
        union all
        select *
        from rule_5_phone_state
        union all
        select *
        from rule_6_name_state_office_neardate
    ),

    -- Enforce 1:1: each TS record maps to at most one BR record and vice versa.
    -- Within each rule, keep the best match (lowest confidence_rank wins, then
    -- alphabetical record_id as tiebreaker).
    deduped as (
        select *
        from all_matches
        qualify
            row_number() over (
                partition by ts_record_id
                order by match_confidence_rank asc, br_record_id asc
            )
            = 1
            and row_number() over (
                partition by br_record_id
                order by match_confidence_rank asc, ts_record_id asc
            )
            = 1
    )

select ts_record_id, br_record_id, match_rule, match_confidence_rank, decided_by
from deduped
