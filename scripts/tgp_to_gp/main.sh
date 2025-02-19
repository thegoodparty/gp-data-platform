#!/bin/bash

# stop on error
set -euo pipefail

tables_in_tgp=(
    "aichat"
    "campaignplanversion" # 2-6 minutes, 57mb on disk
    "campaignupdatehistory"
    "candidateposition"
    "censusentity"
    "county"
    "pathtovictory"
    "topissue"
    "position"
)

slow_tables_in_tgp=(
    "municipality" # 7-25 minutes, 264MB
    "campaign" # 8-40 minutes, 382 MB
)
# many_to_many_tables=(
# )

# download data from tgp-api dbs in parallel
# latest run on 2025-02-18 18:00:00 ET
for table in "${tables_in_tgp[@]}"; do
    ./table_extract.sh \
        --db_host "$DB_HOST_TGP" \
        --db_port "$DB_PORT_TGP" \
        --db_user "$DB_USER_TGP" \
        --db_name "$DB_NAME_TGP" \
        --table_name "$table" \
        --cutoff_date "2026-02-14 00:00:00" \
        --is_incremental "$IS_INCREMENTAL" &
done

# download full user data to assist with foreign key constraints
# latest run on 2025-02-18 18:00:00 ET
./table_extract.sh \
    --db_host "$DB_HOST_TGP" \
    --db_port "$DB_PORT_TGP" \
    --db_user "$DB_USER_TGP" \
    --db_name "$DB_NAME_TGP" \
    --table_name "user" \
    --cutoff_date "2030-01-01 00:00:00" \
    --is_incremental false &


# comment out large table downloads for dev work
# latest run on 2025-02-18 18:00:00 ET
for table in "${slow_tables_in_tgp[@]}"; do
    caffeinate ./table_extract.sh \
        --db_host "$DB_HOST_TGP" \
        --db_port "$DB_PORT_TGP" \
        --db_user "$DB_USER_TGP" \
        --db_name "$DB_NAME_TGP" \
        --table_name "$table" \
        --cutoff_date "2026-02-14 00:00:00" \
        --is_incremental "$IS_INCREMENTAL" &
done


## need to add many-to-many mapping tables (full table replications, may need to have these created in prisma for prod env)


# let parallel downloads finish
wait

# prompt user to switch to GP network before continuing
read -p "Switch to GP network to continue with data loading. Press enter to continue..."

# load query strings
source ./transform_load_queries.sh

# the order of the scripts is important to abide by foreign key constraints
transform_load_queries=(
    "user|$user_create_staging|$user_upsert"  # 1 minute
    "campaign|$campaign_create_staging|$campaign_upsert"  # 4-14 minutes
    "aichat|$aichat_create_staging|$aichat_upsert"  # 8 seconds
    "campaignplanversion|$campaignplanversion_create_staging|$campaignplanversion_upsert"  # 30 seconds
    "topissue|$topissue_create_staging|$topissue_upsert"  # 6 seconds
    "position|$position_create_staging|$position_upsert"  # 6 seconds
    "candidateposition|$candidateposition_create_staging|$candidateposition_upsert"  # 10 seconds
    "censusentity|$censusentity_create_staging|$censusentity_upsert"  # 15 seconds
    "county|$county_create_staging|$county_upsert"  # 12 seconds
    "campaignupdatehistory|$campaignupdatehistory_create_staging|$campaignupdatehistory_upsert"  # 8 seconds
    "pathtovictory|$pathtovictory_create_staging|$pathtovictory_upsert"  # 72 seconds
    "municipality|$municipality_create_staging|$municipality_upsert"  # 2-8 minutes
)


# load from csv to gp db
for queries in "${transform_load_queries[@]}"; do

    # parse pipe separated arguments and account for line breaks
    queries=$(echo "$queries" | tr '\n' ' ')
    IFS='|' read -r original_table_name staging_query upsert_query <<< "$queries"

    # execute the transform load
    caffeinate ./transform_load_executor.sh \
        --db_host "$DB_HOST_GP" \
        --db_port "$DB_PORT_GP" \
        --db_user "$DB_USER_GP" \
        --db_name "$DB_NAME_GP" \
        --original_table_name "$original_table_name" \
        --staging_query "$staging_query" \
        --upsert_query "$upsert_query"
done
