{{
    codegen.generate_base_model(
        source_name="airbyte_source",
        table_name="ballotready_s3_uscities_v1.77",
        materialized="view",
    )
}}

{{
    codegen.generate_base_model(
        source_name="airbyte_source",
        table_name="ballotready_s3_uscities_v1.77_short",
        materialized="view",
    )
}}

{{
    codegen.generate_base_model(
        source_name="airbyte_source",
        table_name="ballotready_s3_uscounties_v1.73",
        materialized="view",
    )
}}

{{
    codegen.generate_base_model(
        source_name="airbyte_source",
        table_name="ballotready_s3_uscounties_v1.73_short",
        materialized="view",
    )
}}
