{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["intermediate", "ballotready", "enhanced_person"],
    )
}}


with
    latest_person as (
        select {{ dbt_utils.star(from=ref("int__ballotready_person"), except=[]) }}
        from {{ ref("int__ballotready_person") }}
        {% if is_incremental() %}
            where updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    ),
    enhanced_person as (
        select *, case when bio_text is not null then bio_text else null end as about
        -- TODO: in case bio_text is null, use
        -- https://github.com/thegoodparty/tgp-api/blob/74b4b6247b75cc39077ad4b16bfbe83dd997b6cf/api/helpers/candidate/generate-presentation.js#L175
        -- requires gp-api match to get top issues and endorsements as part of the text
        from latest_person
    )

select *
from enhanced_person
