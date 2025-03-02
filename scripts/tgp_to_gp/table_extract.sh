#!/bin/bash

# Parse command line arguments
is_incremental="false"
db_host=""
db_port=""
db_user=""
db_name=""
table_name=""
cutoff_date="2025-02-11 00:00:00"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --is_incremental)
            is_incremental="$2"
            shift 2
            ;;
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
        --table_name)
            table_name="$2"
            shift 2
            ;;
        --cutoff_date)
            cutoff_date="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 --db_host hostname --db_port port --db_user username --db_name dbname --table_name tablename [--is_incremental true|false] [--cutoff_date 'YYYY-MM-DD HH:MM:SS']"
            echo "Note: When is_incremental=false (default), all data up until cutoff_date will be loaded"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

# Validate required parameters
if [ -z "$db_host" ] || [ -z "$db_port" ] || [ -z "$db_user" ] || [ -z "$db_name" ] || [ -z "$table_name" ]; then
    echo "Error: Missing required database connection parameters"
    echo "Use --help for usage information"
    exit 1
fi

# Check if TGP_PGPASSWORD environment variable is set
if [ -z "${TGP_PGPASSWORD}" ]; then
    echo "Error: TGP_PGPASSWORD environment variable is not set"
    echo "Please set it using: export TGP_PGPASSWORD='your_password'"
    exit 1
fi

# Set PGPASSWORD for this script's duration
export PGPASSWORD="${TGP_PGPASSWORD}"

# Start the timer
start_time=$(date +%s)

## download the data, use incremental if needed
if [ "$is_incremental" = "true" ]; then
    sql_command="COPY (
        SELECT *
        FROM public.$table_name
        WHERE \"updatedAt\" >= EXTRACT(EPOCH FROM timestamp '$cutoff_date')*1000
    ) TO STDOUT WITH CSV HEADER"
else
    sql_command="COPY (
        SELECT *
        FROM public.$table_name
    ) TO STDOUT WITH CSV HEADER"
fi

# Create tmp_data directory if it doesn't exist, remove existing $table_name.csv if it exists
mkdir -p ./tmp_data
rm -f ./tmp_data/$table_name.csv

psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "$sql_command" \
  > ./tmp_data/$table_name.csv

# Get the row count pulled from the source table
if [ "$is_incremental" = "true" ]; then
    row_count=$(
        psql \
            -h "$db_host" \
            -p "$db_port" \
            -U "$db_user" \
            -d "$db_name" \
            -t -c "
                SELECT COUNT(*)
                FROM public.$table_name
                WHERE \"updatedAt\" >= EXTRACT(EPOCH FROM timestamp '$cutoff_date')*1000
            "
    )
else
    row_count=$(
        psql \
            -h "$db_host" \
            -p "$db_port" \
            -U "$db_user" \
            -d "$db_name" \
            -t -c "
                SELECT COUNT(*)
                FROM public.$table_name
                WHERE \"updatedAt\" < EXTRACT(EPOCH FROM timestamp '$cutoff_date')*1000
            "
    )
fi

## end timer
end_time=$(date +%s)
elapsed_time=$((end_time - start_time))

# Calculate and print the elapsed time
total_minutes=$((elapsed_time / 60))

printf "Time download table %s (row count: %d): %02d minutes (%02d seconds)\n" \
  "$table_name" \
  "$row_count" \
  "$total_minutes" \
  "$elapsed_time"

# Cleanup
unset PGPASSWORD
