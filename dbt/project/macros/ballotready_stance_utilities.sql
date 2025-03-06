{% macro get_stance_freshness() %}
    {#
  This macro checks the freshness of stance data by comparing it with candidacy updates
  Usage: {{ get_stance_freshness() }}
  #}
    with
        candidacy_updates as (
            select candidacy_id, max(updated_at) as last_candidacy_update
            from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
            group by candidacy_id
        ),

        stance_updates as (
            select candidacy_id, max(updated_at) as last_stance_update
            from {{ ref("int__ballotready_stance") }}
            group by candidacy_id
        )

    select
        c.candidacy_id,
        c.last_candidacy_update,
        s.last_stance_update,
        {{ dbt.datediff("s.last_stance_update", "c.last_candidacy_update", "day") }}
        as days_difference
    from candidacy_updates c
    left join stance_updates s on c.candidacy_id = s.candidacy_id
    where
        {{ dbt.datediff("s.last_stance_update", "c.last_candidacy_update", "day") }}
        > 14
        or s.last_stance_update is null
{% endmacro %}

{% macro validate_api_connectivity() %}
    {#
  Validates API connectivity by checking if any stances were retrieved in recent runs
  Usage: {{ validate_api_connectivity() }}
  #}
    select count(*) as total_stances, max(updated_at) as last_update
    from {{ ref("int__ballotready_stance") }}
    where updated_at >= current_date - interval '2' day
    having count(*) = 0 or max(updated_at) < current_date - interval '2' day
{% endmacro %}

{% macro monitor_stance_counts() %}
    {#
  Monitors stance counts by candidacy to detect anomalies
  Usage: {{ monitor_stance_counts() }}
  #}
    with
        stance_count as (
            select candidacy_id, array_size(stances) as num_stances, updated_at
            from {{ ref("int__ballotready_stance") }}
        ),

        stance_stats as (
            select
                percentile(num_stances, 0.25) as p25,
                percentile(num_stances, 0.5) as median,
                percentile(num_stances, 0.75) as p75,
                percentile(num_stances, 0.75) - percentile(num_stances, 0.25) as iqr,
                avg(num_stances) as avg_stances
            from stance_count
        )

    select sc.candidacy_id, sc.num_stances, sc.updated_at
    from stance_count sc
    cross join stance_stats ss
    where
        -- Detect outliers: stances that are less than (Q1 - 1.5*IQR) or more than (Q3
        -- + 1.5*IQR)
        sc.num_stances < (ss.p25 - 1.5 * ss.iqr)
        or sc.num_stances > (ss.p75 + 1.5 * ss.iqr)
        -- Only look at recent records to avoid historical data issues
        and sc.updated_at >= current_date - interval '30' day
{% endmacro %}
