-- Test #8/#9: cross-source name agreement on bridge-matched rows.
-- Returns rows where the failure threshold is breached (test fails if any returned).
--
-- Last name agreement should be >= 0.95.
-- First name agreement should be >= 0.92.
-- Test ALSO fails if total_pairs is zero — catches bridge dropout that would
-- silently pass a NULL ratio comparison.
with
    bridge_pairs as (
        select
            b.gp_api_elected_office_id,
            gp.last_name as gp_last,
            gp.first_name as gp_first,
            br.last_name as br_last,
            br.first_name as br_first
        from {{ ref("int__civics_elected_official_gp_api_bridge") }} b
        inner join
            {{ ref("stg_er_source__clustered_elected_officials") }} gp
            on gp.source_name = 'gp_api'
            and gp.source_id = b.gp_api_elected_office_id
        inner join
            {{ ref("stg_er_source__clustered_elected_officials") }} br
            on br.source_name = 'ballotready_techspeed'
            and br.br_office_holder_id = b.br_office_holder_id
    ),

    agreement_rates as (
        select
            count(*) as total_pairs,
            coalesce(
                count(*) filter (where lower(trim(gp_last)) = lower(trim(br_last)))
                * 1.0
                / nullif(count(*), 0),
                0
            ) as last_name_agreement,
            coalesce(
                count(*) filter (where lower(trim(gp_first)) = lower(trim(br_first)))
                * 1.0
                / nullif(count(*), 0),
                0
            ) as first_name_agreement
        from bridge_pairs
    )

select *
from agreement_rates
where last_name_agreement < 0.95 or first_name_agreement < 0.92 or total_pairs = 0
