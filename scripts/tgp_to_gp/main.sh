#!/bin/bash

# stop on error
set -E

# download data from tgp-api dbs in parallel
./user_extract.sh --db_host "$DB_HOST_TGP" --db_port "$DB_PORT_TGP" --db_user "$DB_USER_TGP" --db_name "$DB_NAME_TGP" &
./campaign_extract.sh --db_host "$DB_HOST_TGP" --db_port "$DB_PORT_TGP" --db_user "$DB_USER_TGP" --db_name "$DB_NAME_TGP" --is_incremental "$IS_INCREMENTAL" &
./aichat_extract.sh --db_host "$DB_HOST_TGP" --db_port "$DB_PORT_TGP" --db_user "$DB_USER_TGP" --db_name "$DB_NAME_TGP" --is_incremental "$IS_INCREMENTAL" &

wait

# the order of the scripts is important to abide by foreign key constraints
./user_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./campaign_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
./aichat_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
