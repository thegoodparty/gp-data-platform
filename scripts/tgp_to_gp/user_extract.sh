#!/bin/bash

# Parse command line arguments
# is_incremental=false
db_host=""
db_port=""
db_user=""
db_name=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        # --is_incremental)
        #     is_incremental="$2"
        #     shift 2
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

## download the data
sql_command="COPY (
    SELECT *
    FROM public.\"user\"
) TO STDOUT WITH CSV HEADER"

# Create tmp_data directory if it doesn't exist, remove existing user.csv if it exists
mkdir -p ./tmp_data
rm -f ./tmp_data/user.csv

psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "$sql_command" \
  > ./tmp_data/user.csv

## end timer
end_time=$(date +%s)
elapsed_time=$((end_time - start_time))
echo "Timer stopped."

# Calculate and print the elapsed time
seconds=$((elapsed_time % 60))
minutes=$((elapsed_time / 60 % 60))
hours=$((elapsed_time / 3600))

printf "Elapsed time: %02d:%02d:%02d\n" $hours $minutes $seconds
# it takes 3.5 minutes to download
# it takes 10 MB on disk

# Cleanup
unset PGPASSWORD
