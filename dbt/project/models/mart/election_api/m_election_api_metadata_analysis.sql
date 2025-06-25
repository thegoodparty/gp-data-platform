{% test projected_turnout_recovered(model, threshold) %}


with missing_before as (
    select details, data,
    get_json_object(details, '$.positionId') AS br_position_id
    get_json_object(data, '$.hubspotUpdates.path_to_victory_status') AS p2v_status
    from ({ ref('stg_airbyte_source__gp_api_db_campaign') })
    where p2v_status = "Locked"
    OR p2v_status = "Waiting"
    OR p2v_status = "Failed"
),
recovered as (
    select count(*) as recovered_cnt
    from {{ model }} cur
    where cur.user_id in (select user_id from missing_before)
        and cur.projected_turnout is not null
),
tot as (
    select count(*) as total_cnt from missing_before
)
select * from recovered, tot
where recovered_cnt::decimal / nullif(total_cnt, 0)