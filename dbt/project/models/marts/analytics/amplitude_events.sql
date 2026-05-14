{{ config(materialized="view") }}

/*
    Passthrough exposure of stg_airbyte_source__amplitude_api_events into
    mart_analytics for Sigma BI POV consumption. View materialization (not
    table) because this is a thin alias and storage duplication isn't
    justified. Promote to a transformed table model if Sigma usage patterns
    warrant it post-POV. Sigma SP access to mart_analytics is provisioned
    in thegoodparty/gp-terraform-dataplatform#29.
*/
select *
from {{ ref("stg_airbyte_source__amplitude_api_events") }}
