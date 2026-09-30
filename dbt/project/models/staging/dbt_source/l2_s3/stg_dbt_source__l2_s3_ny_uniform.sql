{% set source_ref = source("dbt_source", "l2_s3_ny_uniform") %}

select
    {{
        dbt_utils.star(
            from=source_ref,
            except=[
                "VoterTelephones_CellPhoneFormatted",
                "VoterTelephones_LandlineFormatted",
            ],
        )
    }},
    {{ valid_phone_number("`VoterTelephones_CellPhoneFormatted`") }}
    as `VoterTelephones_CellPhoneFormatted`,
    {{ valid_phone_number("`VoterTelephones_LandlineFormatted`") }}
    as `VoterTelephones_LandlineFormatted`
from {{ source_ref }}
