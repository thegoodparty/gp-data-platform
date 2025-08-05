{{
    config(
        materialized="incremental",
        unique_key="id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["mart", "general", "candidacy", "hubspot"],
    )
}}

select tbl_hs_contacts.contact_id as hubspot_contact_id
from {{ ref("int__hubspot_contacts_w_companies") }} as tbl_hs_contacts
left join
    {{ ref("stg_airbyte_source__gp_api_db_user") }} as tbl_gp_user
    on tbl_hs_contacts.gp_candidacy_id = tbl_gp_user.data:candidacyid::string
