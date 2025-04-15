#!/bin/bash

# stop on error
set -euo pipefail

# load query strings
source ./transform_load_queries.sh

# the order of the scripts is important to abide by foreign key constraints
transform_load_queries=(
    "Place|Place|$place_create_staging|$place_upsert"
    "Race|Race|$race_create_staging|$race_upsert"
)


# load from csv to gp db
for queries in "${transform_load_queries[@]}"; do

    # parse pipe separated arguments and account for line breaks
    queries=$(echo "$queries" | tr '\n' ' ')
    IFS='|' read -r original_table_name new_table_name staging_query upsert_query <<< "$queries"

    # execute the transform load
    caffeinate ./transform_load_executor.sh \
        --db_host "$DB_HOST_GP" \
        --db_port "$DB_PORT_GP" \
        --db_user "$DB_USER_GP" \
        --db_name "$DB_NAME_GP" \
        --original_table_name "$original_table_name" \
        --new_table_name "$new_table_name" \
        --staging_query "$staging_query" \
        --upsert_query "$upsert_query"
done
