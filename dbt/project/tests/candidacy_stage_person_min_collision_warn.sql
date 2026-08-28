{{ config(severity="warn") }}
-- Warn when a candidacy_stage row's own native source keys resolve to more than
-- one gp_person_id. Cluster-merged rows are one person by construction, so the
-- mart takes the deterministic min; this surfaces any residual disagreement the
-- min() silently collapsed.
with
    pid as (
        select record_key, gp_person_id
        from {{ ref("int__civics_person_canonical_ids") }}
    ),

    br as (
        select distinct
            cast(br_candidacy_id as string) as br_candidacy_id,
            cast(br_candidate_id as string) as br_candidate_id
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidacy_id is not null
    ),

    campaign_user as (
        select
            cast(campaign_id as string) as campaign_id,
            cast(user_id as string) as user_id
        from {{ ref("campaigns") }}
        where is_latest_version and user_id is not null
    ),

    ts_code as (
        select
            {{ strip_ts_stage_suffix("substring_index(record_key, '|', -1)") }}
            as ts_code,
            min(gp_person_id) as gp_person_id
        from pid
        where record_key like 'techspeed|%'
        group by 1
    ),

    resolved as (
        select
            cs.gp_candidacy_stage_id,
            bp.gp_person_id as br_person,
            tp.gp_person_id as ts_person,
            gp.gp_person_id as gp_api_person,
            dp.gp_person_id as ddhq_person
        from {{ ref("candidacy_stage") }} as cs
        left join br on br.br_candidacy_id = cs.br_candidacy_id
        left join pid as bp on bp.record_key = 'ballotready|' || br.br_candidate_id
        left join ts_code as tp on tp.ts_code = cs.ts_source_candidate_id
        left join campaign_user as cu on cu.campaign_id = cs.gp_api_campaign_id
        left join pid as gp on gp.record_key = 'gp_api|' || cu.user_id
        left join
            pid as dp
            on dp.record_key = 'ddhq|' || cs.ddhq_candidate_id || '_' || cs.ddhq_race_id
    )

select gp_candidacy_stage_id
from resolved
where
    size(
        array_distinct(
            array_compact(array(br_person, ts_person, gp_api_person, ddhq_person))
        )
    )
    > 1
