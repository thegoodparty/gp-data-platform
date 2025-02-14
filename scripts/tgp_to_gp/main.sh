#!/bin/bash

# stop on error
set -E

# download data from tgp-api dbs in parallel
./user_extract.sh --db_host "$DB_HOST_TGP" --db_port "$DB_PORT_TGP" --db_user "$DB_USER_TGP" --db_name "$DB_NAME_TGP" &
./campaign_extract.sh --db_host "$DB_HOST_TGP" --db_port "$DB_PORT_TGP" --db_user "$DB_USER_TGP" --db_name "$DB_NAME_TGP" --is_incremental "$IS_INCREMENTAL" &
./aichat_extract.sh --db_host "$DB_HOST_TGP" --db_port "$DB_PORT_TGP" --db_user "$DB_USER_TGP" --db_name "$DB_NAME_TGP" --is_incremental "$IS_INCREMENTAL" &
./campaignplanversion_extract.sh --db_host "$DB_HOST_TGP" --db_port "$DB_PORT_TGP" --db_user "$DB_USER_TGP" --db_name "$DB_NAME_TGP" --is_incremental "$IS_INCREMENTAL" &
./topissue_extract.sh --db_host "$DB_HOST_TGP" --db_port "$DB_PORT_TGP" --db_user "$DB_USER_TGP" --db_name "$DB_NAME_TGP" --is_incremental "$IS_INCREMENTAL" &
./position_extract.sh --db_host "$DB_HOST_TGP" --db_port "$DB_PORT_TGP" --db_user "$DB_USER_TGP" --db_name "$DB_NAME_TGP" --is_incremental "$IS_INCREMENTAL" &
./candidateposition_extract.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP" &

## need to add many-to-many mapping tables (full table replications, may need to have these created in prisma for prod env)

wait

# the order of the scripts is important to abide by foreign key constraints
./user_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./campaign_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./aichat_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./campaignplanversion_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./topissue_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./position_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./candidateposition_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
