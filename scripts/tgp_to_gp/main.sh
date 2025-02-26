#!/bin/bash

# stop on error
set -euo pipefail

tables_in_tgp=(
    "aichat"
    "campaignupdatehistory"
    "candidateposition"
    "censusentity"
    "county"
    "pathtovictory"
    "topissue"
    "position"
    "electiontype"
)

slow_tables_in_tgp=(
    "campaignplanversion" # 2-23 minutes, 58mb on disk
    "municipality" # 7-70 minutes, 264MB
    "campaign" # 8-72 minutes, 382 MB
)

## download data from tgp-api dbs in parallel
## latest run on 2025-02-26 13:00:00 ET
for table in "${tables_in_tgp[@]}"; do
    ./table_extract.sh \
        --db_host "$DB_HOST_TGP" \
        --db_port "$DB_PORT_TGP" \
        --db_user "$DB_USER_TGP" \
        --db_name "$DB_NAME_TGP" \
        --table_name "$table" \
        --cutoff_date "2026-02-26 00:00:00" \
        --is_incremental "$IS_INCREMENTAL" &
done

## download full user data to assist with foreign key constraints
## latest run on 2025-02-26 13:00:00 ET
./table_extract.sh \
    --db_host "$DB_HOST_TGP" \
    --db_port "$DB_PORT_TGP" \
    --db_user "$DB_USER_TGP" \
    --db_name "$DB_NAME_TGP" \
    --table_name "user" \
    --cutoff_date "2030-01-01 00:00:00" \
    --is_incremental false &


## comment out large table downloads for dev work
## latest run on 2025-02-26 13:00:00 ET
for table in "${slow_tables_in_tgp[@]}"; do
    caffeinate ./table_extract.sh \
        --db_host "$DB_HOST_TGP" \
        --db_port "$DB_PORT_TGP" \
        --db_user "$DB_USER_TGP" \
        --db_name "$DB_NAME_TGP" \
        --table_name "$table" \
        --cutoff_date "2026-02-26 00:00:00" \
        --is_incremental "$IS_INCREMENTAL" &
done

## there's only one many-to-many table and it lacks an `updatedAt` column
## so we'll just download the full table
download_start=$(date +%s)
psql \
    -h "$DB_HOST_TGP" \
    -p "$DB_PORT_TGP" \
    -U "$DB_USER_TGP" \
    -d "$DB_NAME_TGP" \
    -c "COPY (
        SELECT *
        FROM public.\"campaign_topIssues__topissue_campaigns\"
    ) TO STDOUT WITH CSV HEADER" \
    > ./tmp_data/campaign_topIssues__topissue_campaigns.csv
download_end=$(date +%s)
download_time=$((download_end-download_start))
printf "Time to download table campaign_topIssues__topissue_campaigns: %d seconds\n" "$download_time"


#let parallel downloads finish
wait

# prompt user to switch to GP network before continuing
read -p "Switch to GP network to continue with data loading. Press enter to continue..."

# load query strings
source ./transform_load_queries.sh

# the order of the scripts is important to abide by foreign key constraints
transform_load_queries=(
    "user|user|$user_create_staging|$user_upsert"  # 1 minute
    "campaign|campaign|$campaign_create_staging|$campaign_upsert"  # 4-14 minutes
    "aichat|ai_chat|$aichat_create_staging|$aichat_upsert"  # 8 seconds
    "campaignplanversion|campaign_plan_version|$campaignplanversion_create_staging|$campaignplanversion_upsert"  # 30 seconds
    "topissue|top_issue|$topissue_create_staging|$topissue_upsert"  # 6 seconds
    "campaign_topIssues__topissue_campaigns|_CampaignToTopIssue|$campaign_topIssues__topissue_campaigns_create_staging|$campaign_topIssues__topissue_campaigns_upsert"
    "position|position|$position_create_staging|$position_upsert"  # 6 seconds
    "candidateposition|campaign_position|$candidateposition_create_staging|$candidateposition_upsert"  # 10 seconds
    "censusentity|census_entity|$censusentity_create_staging|$censusentity_upsert"  # 15 seconds
    "county|county|$county_create_staging|$county_upsert"  # 12 seconds
    "campaignupdatehistory|campaign_update_history|$campaignupdatehistory_create_staging|$campaignupdatehistory_upsert"  # 8 seconds
    "pathtovictory|path_to_victory|$pathtovictory_create_staging|$pathtovictory_upsert"  # 72 seconds
    "municipality|municipality|$municipality_create_staging|$municipality_upsert"  # 2-8 minutes
    "electiontype|election_type|$electiontype_create_staging|$electiontype_upsert"  # 2-8 minutes
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
