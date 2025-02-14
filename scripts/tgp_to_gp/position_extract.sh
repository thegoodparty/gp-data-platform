#!/bin/bash

# Parse command line arguments
db_host=""
db_port=""
db_user=""
db_name=""
is_incremental=false

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
        --is_incremental)
            is_incremental="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 --db_host hostname --db_port port --db_user username --db_name dbname [--is_incremental] is_incremental=true|false"
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

# Check if TGP_PGPASSWORD environment variable is set
if [ -z "${TGP_PGPASSWORD}" ]; then
    echo "Error: TGP_PGPASSWORD environment variable is not set"
    echo "Please set it using: export TGP_PGPASSWORD='your_password'"
    exit 1
fi

# Set PGPASSWORD for this script's duration
export PGPASSWORD="${TGP_PGPASSWORD}"

# Document the behavior
echo "Running export"

# Start the timer
start_time=$(date +%s)
echo "Timer started."

## download the data, use incremental if needed
if [ "$is_incremental" = "true" ]; then
    sql_command="COPY (
        SELECT *
        FROM public.position
        WHERE \"updatedAt\" >= EXTRACT(EPOCH FROM timestamp '2025-02-11 00:00:00')*1000
    ) TO STDOUT WITH CSV HEADER"
else
    sql_command="COPY (
        SELECT *
        FROM public.position
        WHERE \"updatedAt\" < EXTRACT(EPOCH FROM timestamp '2025-02-11 00:00:00')*1000
    ) TO STDOUT WITH CSV HEADER"
fi


# Create tmp_data directory if it doesn't exist, remove existing position.csv if it exists
mkdir -p ./tmp_data
rm -f ./tmp_data/position.csv

psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "$sql_command" \
  > ./tmp_data/position.csv

## end timer
end_time=$(date +%s)
elapsed_time=$((end_time - start_time))
echo "Timer stopped."

# Calculate and print the elapsed time
seconds=$((elapsed_time % 60))
minutes=$((elapsed_time / 60 % 60))
hours=$((elapsed_time / 3600))

printf "Elapsed time: %02d:%02d:%02d\n" $hours $minutes $seconds
# about  minute do run and  MB on disk

# Cleanup
unset PGPASSWORD
