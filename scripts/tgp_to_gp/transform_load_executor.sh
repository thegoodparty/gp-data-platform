#!/bin/bash
set -euo pipefail

# Parse command line arguments
db_host=""
db_port=""
db_user=""
db_name=""
original_table_name=""

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
        --original_table_name)
            original_table_name="$2"
            shift 2
            ;;
        --staging_query)
            staging_query="$2"
            shift 2
            ;;
        --upsert_query)
            upsert_query="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 --db_host hostname --db_port port --db_user username --db_name dbname --original_table_name table_name --staging_query staging_query --upsert_query upsert_query"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

# Validate required parameters
if [ -z "$db_host" ] || [ -z "$db_port" ] || [ -z "$db_user" ] || [ -z "$db_name" ] || [ -z "$original_table_name" ] || [ -z "$staging_query" ] || [ -z "$upsert_query" ]; then
    echo "Error: Missing required database connection parameters or original_table_name"
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

# Start the timer for schema creation
schema_start=$(date +%s)

# Execute the staging_query
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "$staging_query"

# End the timer and calculate the duration
schema_end=$(date +%s)
schema_time=$((schema_end - schema_start))

# Start the timer for data upload
upload_start=$(date +%s)
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "\COPY staging.\"${original_table_name}\" FROM './tmp_data/${original_table_name}.csv' WITH CSV HEADER"
upload_end=$(date +%s)
upload_time=$((upload_end - upload_start))

## upsert with transforms into destination table
upsert_start=$(date +%s)
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "$upsert_query"
upsert_end=$(date +%s)
upsert_time=$((upsert_end - upsert_start))

## drop the staging table
drop_start=$(date +%s)
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "DROP TABLE staging.\"${original_table_name}\";"
drop_end=$(date +%s)
drop_time=$((drop_end - drop_start))

# Unset PGPASSWORD for security
unset PGPASSWORD

## end timer
end_time=$(date +%s)
elapsed_time=$((end_time - start_time))
total_minutes=$((elapsed_time / 60))

## print elapsed time and row count
printf "Elapsed time for table %s: %02d minutes (%02d seconds)\n" \
  "$original_table_name" \
  "$total_minutes" \
  "$elapsed_time"
