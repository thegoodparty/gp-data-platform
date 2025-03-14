{{
    codegen.generate_base_model(
        source_name="airbyte_source",
        table_name="ballotready_s3_uscities_v1_77",
        materialized="view",
    )
}}

{{
    codegen.generate_base_model(
        source_name="airbyte_source",
        table_name="ballotready_s3_uscounties_v1_73",
        materialized="view",
    )
}}
