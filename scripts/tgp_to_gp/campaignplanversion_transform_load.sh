#!/bin/bash
set +e  # Don't exit on error
trap 'read -p "Command failed. Press enter to continue..."' ERR

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

# Start the overall timer
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
  DROP TABLE IF EXISTS staging.campaignplanversion;
  CREATE TABLE staging.campaignplanversion (
    "createdAt" bigint NULL,
    "updatedAt" bigint NULL,
    id serial NOT NULL,
    data json NULL,
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
  -c "\COPY staging.campaignplanversion FROM './tmp_data/campaignplanversion.csv' WITH CSV HEADER"
upload_end=$(date +%s)
upload_time=$((upload_end - upload_start))
printf "Data upload completed in %02d:%02d:%02d\n" $((upload_time/3600)) $((upload_time/60%60)) $((upload_time%60))

# Start the timer for upserting data
echo "Upserting data into destination table..."
upsert_start=$(date +%s)
sql_command="
    INSERT INTO public.campaign_plan_version (
        id,
        campaign_id,
        data,
        created_at,
        updated_at
    )
    SELECT
        id,
        campaign,
        data,
        to_timestamp("createdAt"::double precision/1000),
        to_timestamp("updatedAt"::double precision/1000)
    FROM staging.campaignplanversion
    WHERE campaign IN (SELECT id FROM public.campaign)
    ON CONFLICT (id) DO UPDATE SET
        campaign_id = EXCLUDED.campaign_id,
        data = EXCLUDED.data,
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at;
"

psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "$sql_command"
upsert_end=$(date +%s)
upsert_time=$((upsert_end - upsert_start))
printf "Upsert completed in %02d:%02d:%02d\n" $((upsert_time/3600)) $((upsert_time/60%60)) $((upsert_time%60))

# Start the timer for dropping the staging table
echo "Dropping staging table..."
drop_start=$(date +%s)
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "DROP TABLE staging.campaignplanversion;"
drop_end=$(date +%s)
drop_time=$((drop_end - drop_start))
printf "Staging table drop completed in %02d:%02d:%02d\n" $((drop_time/3600)) $((drop_time/60%60)) $((drop_time%60))

# Unset PGPASSWORD for security
unset PGPASSWORD

# End the overall timer
end_time=$(date +%s)
elapsed_time=$((end_time - start_time))
echo "Timer stopped."

# Calculate and print the elapsed time
seconds=$((elapsed_time % 60))
minutes=$((elapsed_time / 60 % 60))
hours=$((elapsed_time / 3600))

printf "Elapsed time: %02d:%02d:%02d\n" $hours $minutes $seconds
