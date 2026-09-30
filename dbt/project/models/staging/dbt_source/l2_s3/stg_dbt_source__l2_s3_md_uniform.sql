{% set source_ref = source("dbt_source", "l2_s3_md_uniform") %}

-- Full phone numbers are kept only when they strip to a 10-digit US number;
-- anything else becomes null so downstream counts and exports see dialable
-- numbers only. Fragment columns (area code, 7-digit) pass through untouched.
select
    {{
        dbt_utils.star(
            from=source_ref,
            except=[
                "VoterTelephones_LandlineUnformatted",
                "VoterTelephones_LandlineFormatted",
                "VoterTelephones_CellPhoneUnformatted",
                "VoterTelephones_CellPhoneFormatted",
            ],
        )
    }},
    {{ sanitize_phone_number("`VoterTelephones_LandlineUnformatted`") }}
    as `VoterTelephones_LandlineUnformatted`,
    {{ sanitize_phone_number("`VoterTelephones_LandlineFormatted`") }}
    as `VoterTelephones_LandlineFormatted`,
    {{ sanitize_phone_number("`VoterTelephones_CellPhoneUnformatted`") }}
    as `VoterTelephones_CellPhoneUnformatted`,
    {{ sanitize_phone_number("`VoterTelephones_CellPhoneFormatted`") }}
    as `VoterTelephones_CellPhoneFormatted`
from {{ source_ref }}
