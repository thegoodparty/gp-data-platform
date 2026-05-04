{{ config(severity="warn") }}

-- Bridge collision audit. Reports the count of pass-1 rows that were
-- dropped in pass-2 due to br_office_holder_id collisions. Warns (not
-- errors) so that typical drift doesn't block the build; investigate
-- when collision count grows materially.
with
    pass1_simulation as (
        select g.source_id as gp_api_elected_office_id, b.br_office_holder_id
        from {{ ref("stg_er_source__clustered_elected_officials") }} g
        inner join
            {{ ref("stg_er_source__clustered_elected_officials") }} b using (cluster_id)
        where g.source_name = 'gp_api' and b.source_name = 'ballotready_techspeed'
        qualify
            row_number() over (
                partition by g.source_id
                order by
                    abs(datediff(b.term_start_date, g.term_start_date)) asc nulls last,
                    b.term_start_date desc nulls last,
                    b.br_office_holder_id asc
            )
            = 1
    ),

    pass2_count as (select count(*) as pass1_rows from pass1_simulation),

    deduped_count as (
        select count(*) as deduped_rows
        from {{ ref("int__civics_elected_official_gp_api_bridge") }}
    )

select
    p.pass1_rows, d.deduped_rows, (p.pass1_rows - d.deduped_rows) as collisions_dropped
from pass2_count p, deduped_count d
where (p.pass1_rows - d.deduped_rows) > 25
