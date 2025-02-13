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

## switch VPNs and load the data

# Start the timer
start_time=$(date +%s)
echo "Timer started."

# create staging table schema
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

# upload the data to a staging table
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "\COPY staging.aichat FROM './data/aichat.csv' WITH CSV HEADER"

## upsert with transforms into destination table
sql_command="
    INSERT INTO public.ai_chat (
        created_at,
        updated_at,
        id,
        assistant,
        thread,
        data,
        \"user\",
        campaign
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
    ON CONFLICT (id) DO UPDATE SET
        created_at = to_timestamp(EXCLUDED.createdAt::double precision/1000),
        updated_at = to_timestamp(EXCLUDED.updatedAt::double precision/1000),
        assistant = EXCLUDED.assistant,
        thread = EXCLUDED.thread,
        data = EXCLUDED.data,
        \"user\" = EXCLUDED.\"user\",
        campaign = EXCLUDED.campaign;"

psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "$sql_command"

## drop the staging table
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "DROP TABLE staging.aichat;"

## end timer
end_time=$(date +%s)
elapsed_time=$((end_time - start_time))
echo "Timer stopped."

# Calculate and print the elapsed time
seconds=$((elapsed_time % 60))
minutes=$((elapsed_time / 60 % 60))
hours=$((elapsed_time / 3600))

printf "Elapsed time: %02d:%02d:%02d\n" $hours $minutes $seconds
