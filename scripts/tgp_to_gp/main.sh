#!/bin/bash

# stop on error
set -E

tables_in_tgp=(
    "aichat"
    "campaignupdatehistory"

    "candidateposition"
    "censusentity"
    "county"
    "municipality"
    "pathtovictory"
    "ballotrace"
    "topissue"
    "position"
)

slow_tables_in_tgp=(
    "campaignplanversion" # 1 minute
    "municipality" # 6 minutes
    "ballotrace" # 21 minutes
    "campaign" # 60 minutes
)
# many_to_many_tables=(
# )

# download data from tgp-api dbs in parallel
for table in "${tables_in_tgp[@]}"; do
    ./table_extract.sh \
        --db_host "$DB_HOST_TGP" \
        --db_port "$DB_PORT_TGP" \
        --db_user "$DB_USER_TGP" \
        --db_name "$DB_NAME_TGP" \
        --table_name "$table" \
        --cutoff_date "2025-02-14 00:00:00" \
        --is_incremental "$IS_INCREMENTAL" &
done

# download full user data to assist with foreign key constraints
./table_extract.sh \
    --db_host "$DB_HOST_TGP" \
    --db_port "$DB_PORT_TGP" \
    --db_user "$DB_USER_TGP" \
    --db_name "$DB_NAME_TGP" \
    --table_name "user" \
    --cutoff_date "2030-01-01 00:00:00" \
    --is_incremental false &


# commend out large table downloads for dev work
# for table in "${slow_tables_in_tgp[@]}"; do
#     ./table_extract.sh \
#         --db_host "$DB_HOST_TGP" \
#         --db_port "$DB_PORT_TGP" \
#         --db_user "$DB_USER_TGP" \
#         --db_name "$DB_NAME_TGP" \
#         --table_name "$table" \
#         --cutoff_date "2025-02-14 00:00:00" \
#         --is_incremental "$IS_INCREMENTAL" &
# done


## need to add many-to-many mapping tables (full table replications, may need to have these created in prisma for prod env)

wait
echo "Stopping the script as per instructions."
exit 0

source ./transform_load_queries.sh
transform_load_queries = (
    "aichat" "$aichat_create_staging" "$aichat_upsert"
)

tuples=(
  (apple red)
  (banana yellow)
  (grape purple)
)

# the order of the scripts is important to abide by foreign key constraints
# psql to create schema


./user_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./campaign_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./aichat_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./campaignplanversion_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./topissue_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./position_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./candidateposition_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./campaignupdatehistory_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
