#!/bin/bash

# Array of state abbreviations
states=("ak" "al" "ar" "az" "ca" "co" "ct" "dc" "de" "fl" "ga" "hi" "ia" "id" "il" "in" "ks" "ky" "la" "ma" "md" "me" "mi" "mn" "mo" "ms" "mt" "nc" "nd" "ne" "nh" "nj" "nm" "nv" "ny" "oh" "ok" "or" "pa" "ri" "sc" "sd" "tn" "tx" "ut" "va" "vt" "wa" "wi" "wv" "wy")

# Create directory if it doesn't exist
mkdir -p dbt/project/models/staging/dbt_source/l2_s3

# Generate SQL files for each state
for state in "${states[@]}"; do
    # Create the SQL file for the uniform dataset
    cat > "dbt/project/models/staging/dbt_source/l2_s3/stg_dbt_source__l2_s3_${state}_uniform.sql" << EOF
{{
    config(
        materialized='view'
    )
}}

{% set source_ref = source("dbt_source", "l2_s3_${state}_uniform") %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}  -- use \`except\` for any columns to transform individually
from {{ source_ref }}
EOF

    echo "Generated stg_dbt_source__l2_s3_${state}_uniform.sql"

    # Create the SQL file for the uniform data dictionary
    cat > "dbt/project/models/staging/dbt_source/l2_s3/stg_dbt_source__l2_s3_${state}_uniform_data_dictionary.sql" << EOF
{{
    config(
        materialized='view'
    )
}}

{% set source_ref = source("dbt_source", "l2_s3_${state}_uniform_data_dictionary") %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}  -- use \`except\` for any columns to transform individually
from {{ source_ref }}
EOF

    echo "Generated stg_dbt_source__l2_s3_${state}_uniform_data_dictionary.sql"
done

echo "All uniform staging model files have been generated!"
