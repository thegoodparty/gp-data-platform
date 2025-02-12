#!/bin/bash

./aichat_extract.sh --db_host "$DB_HOST_TGP" --db_port "$DB_PORT_TGP" --db_user "$DB_USER_TGP" --db_name "$DB_NAME_TGP" --is_incremental "$IS_INCREMENTAL"
./aichat_transform_load.sh --db_host "$DB_HOST_GP" --db_port "$DB_PORT_GP" --db_user "$DB_USER_GP" --db_name "$DB_NAME_GP"
