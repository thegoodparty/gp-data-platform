{{ config(materialized="view") }} select * from {{ ref("stg_amplitude__events") }}
