{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["intermediate", "ballotready", "position_fun_facts"],
    )
}}
