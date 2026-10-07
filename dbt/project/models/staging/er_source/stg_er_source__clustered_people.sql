with

    source as (select * from {{ source("er_source", "clustered_people") }}),

    renamed as (

        select unique_id, cluster_id, identity_id, source_name, source_id from source

    )

select *
from renamed
