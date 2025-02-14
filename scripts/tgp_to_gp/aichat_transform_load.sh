#!/bin/bash

# Parse command line arguments
db_host=""
db_port=""
db_user=""
db_name=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --db_host)
            db_host="$2"
            shift 2
            ;;
        --db_port)
            db_port="$2"
            shift 2
            ;;
        --db_user)
            db_user="$2"
            shift 2
            ;;
        --db_name)
            db_name="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 --db_host hostname --db_port port --db_user username --db_name dbname"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

# Validate required parameters
if [ -z "$db_host" ] || [ -z "$db_port" ] || [ -z "$db_user" ] || [ -z "$db_name" ]; then
    echo "Error: Missing required database connection parameters"
    echo "Use --help for usage information"
    exit 1
fi

# Check if GP_PGPASSWORD environment variable is set
if [ -z "${GP_PGPASSWORD}" ]; then
    echo "Error: GP_PGPASSWORD environment variable is not set"
    echo "Please set it using: export GP_PGPASSWORD='your_password'"
    exit 1
fi

# Set PGPASSWORD for this script's duration
export PGPASSWORD="${GP_PGPASSWORD}"

## switch VPNs and load the data

# Start the timer
start_time=$(date +%s)
echo "Timer started."

# Start the timer for schema creation
echo "Creating staging table schema..."
schema_start=$(date +%s)
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "CREATE SCHEMA IF NOT EXISTS staging;
  DROP TABLE IF EXISTS staging.aichat;
  CREATE TABLE staging.aichat (
    \"createdAt\" bigint NULL,
    \"updatedAt\" bigint NULL,
    id serial NOT NULL,
    assistant text NULL,
    thread text NULL,
    data json NULL,
    \"user\" integer NULL,
    campaign integer NULL
  );"
schema_end=$(date +%s)
schema_time=$((schema_end - schema_start))
printf "Schema creation completed in %02d:%02d:%02d\n" $((schema_time/3600)) $((schema_time/60%60)) $((schema_time%60))

# Start the timer for data upload
echo "Uploading data to staging table..."
upload_start=$(date +%s)
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "\COPY staging.aichat FROM './tmp_data/aichat.csv' WITH CSV HEADER"
upload_end=$(date +%s)
upload_time=$((upload_end - upload_start))
printf "Data upload completed in %02d:%02d:%02d\n" $((upload_time/3600)) $((upload_time/60%60)) $((upload_time%60))

## upsert with transforms into destination table
echo "Upserting data into destination table..."
upsert_start=$(date +%s)
sql_command="
    INSERT INTO public.ai_chat (
        created_at,
        updated_at,
        id,
        assistant,
        thread_id,
        data,
        user_id,
        campaign_id
    )
    SELECT
        to_timestamp(\"createdAt\"::double precision/1000),
        to_timestamp(\"updatedAt\"::double precision/1000),
        id,
        assistant,
        thread,
        data,
        \"user\",
        campaign
    FROM staging.aichat
    WHERE campaign in (select id from public.campaign)
    ON CONFLICT (id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        assistant = EXCLUDED.assistant,
        thread_id = EXCLUDED.thread_id,
        data = EXCLUDED.data,
        user_id = EXCLUDED.user_id,
        campaign_id = EXCLUDED.campaign_id;"

psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "$sql_command"
upsert_end=$(date +%s)
upsert_time=$((upsert_end - upsert_start))
printf "Upsert completed in %02d:%02d:%02d\n" $((upsert_time/3600)) $((upsert_time/60%60)) $((upsert_time%60))

## drop the staging table
echo "Dropping staging table..."
drop_start=$(date +%s)
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "DROP TABLE staging.aichat;"
drop_end=$(date +%s)
drop_time=$((drop_end - drop_start))
printf "Staging table drop completed in %02d:%02d:%02d\n" $((drop_time/3600)) $((drop_time/60%60)) $((drop_time%60))

# Unset PGPASSWORD for security
unset PGPASSWORD

## end timer
end_time=$(date +%s)
elapsed_time=$((end_time - start_time))
echo "Timer stopped."

# Calculate and print the elapsed time
seconds=$((elapsed_time % 60))
minutes=$((elapsed_time / 60 % 60))
hours=$((elapsed_time / 3600))

printf "Elapsed time: %02d:%02d:%02d\n" $hours $minutes $seconds
