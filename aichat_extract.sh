#!/bin/bash

# Parse command line arguments
is_incremental=false
db_host=""
db_port=""
db_user=""
db_name=""

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
        --help)
            echo "Usage: $0 --db_host hostname --db_port port --db_user username --db_name dbname [--is_incremental true|false]"
            echo "Note: When is_incremental=false (default), all data up until 2025-02-11 will be loaded"
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

# Document the behavior
echo "Running export with is_incremental=$is_incremental"

# Start the timer
start_time=$(date +%s)
echo "Timer started."


## download the data, use incremental if needed
if [ "$is_incremental" = "true" ]; then
    sql_command="COPY (
        SELECT *
        FROM public.aichat
        WHERE \"updatedAt\" >= EXTRACT(EPOCH FROM timestamp '2025-02-11 00:00:00')*1000
    ) TO STDOUT WITH CSV HEADER"
else
    sql_command="COPY (
        SELECT *
        FROM public.aichat
        WHERE \"updatedAt\" < EXTRACT(EPOCH FROM timestamp '2025-02-11 00:00:00')*1000
    ) TO STDOUT WITH CSV HEADER"
fi

# Create tmp_data directory if it doesn't exist
mkdir -p ./tmp_data

psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "$sql_command" \
  > ./tmp_data/aichat.csv


## end timer
end_time=$(date +%s)
elapsed_time=$((end_time - start_time))
echo "Timer stopped."

# Calculate and print the elapsed time
seconds=$((elapsed_time % 60))
minutes=$((elapsed_time / 60 % 60))
hours=$((elapsed_time / 3600))

printf "Elapsed time: %02d:%02d:%02d\n" $hours $minutes $seconds
# about 1 minute
