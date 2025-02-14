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
  DROP TABLE IF EXISTS staging.campaign;
  CREATE TABLE staging.campaign (
    \"createdAt\" bigint NULL,
    \"updatedAt\" bigint NULL,
    id serial NOT NULL,
    slug text NULL,
    \"isActive\" boolean NULL,
    data json NULL,
    \"user\" integer NULL,
    \"isVerified\" boolean NULL,
    \"isPro\" boolean NULL,
    \"didWin\" boolean NULL,
    tier text NULL,
    \"dateVerified\" date NULL,
    \"dataCopy\" json NULL,
    details jsonb NULL,
    \"pathToVictory\" integer NULL,
    \"aiContent\" jsonb NULL,
    \"ballotCandidate\" integer NULL,
    \"isDemo\" boolean NULL DEFAULT false,
    \"vendorTsData\" jsonb NULL DEFAULT '{}'::jsonb
  );"
schema_end=$(date +%s)
schema_time=$((schema_end - schema_start))
printf "Schema creation completed in %02d:%02d:%02d\n" $((schema_time/3600)) $((schema_time/60%60)) $((schema_time%60))

# Start the timer for data upload
echo "Uploading data to staging table..."
upload_start=$(date +%s)
export PGSSLMODE=require
export PGCONNECT_TIMEOUT=90
psql \
  -h "$db_host" \
  -p "$db_port" \
  -U "$db_user" \
  -d "$db_name" \
  -c "\COPY staging.campaign FROM './tmp_data/campaign.csv' WITH CSV HEADER"
upload_end=$(date +%s)
upload_time=$((upload_end - upload_start))
printf "Data upload completed in %02d:%02d:%02d\n" $((upload_time/3600)) $((upload_time/60%60)) $((upload_time%60))

# Start the timer for upserting data
echo "Upserting data into destination table..."
upsert_start=$(date +%s)
sql_command="
    INSERT INTO public.campaign (
        id,
        created_at,
        updated_at,
        slug,
        is_active,
        is_verified,
        is_pro,
        is_demo,
        did_win,
        date_verified,
        tier,
        data,
        details,
        ai_content,
        vendor_ts_data,
        user_id
    )
    SELECT
        id,
        to_timestamp(\"createdAt\"::double precision/1000),
        to_timestamp(\"updatedAt\"::double precision/1000),
        slug,
        \"isActive\",
        \"isVerified\",
        \"isPro\",
        \"isDemo\",
        \"didWin\",
        \"dateVerified\",
        tier::\"CampaignTier\",
        COALESCE(data::jsonb, '{}'::jsonb),
        COALESCE(details, '{}'::jsonb),
        COALESCE(\"aiContent\", '{}'::jsonb),
        COALESCE(\"vendorTsData\", '{}'::jsonb),
        \"user\"
    FROM staging.campaign
    WHERE \"user\" IN (SELECT id FROM public.user)
    ON CONFLICT (id) DO UPDATE SET
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        slug = EXCLUDED.slug,
        is_active = EXCLUDED.is_active,
        is_verified = EXCLUDED.is_verified,
        is_pro = EXCLUDED.is_pro,
        is_demo = EXCLUDED.is_demo,
        did_win = EXCLUDED.did_win,
        date_verified = EXCLUDED.date_verified,
        tier = EXCLUDED.tier,
        data = EXCLUDED.data,
        details = EXCLUDED.details,
        ai_content = EXCLUDED.ai_content,
        vendor_ts_data = EXCLUDED.vendor_ts_data,
        user_id = EXCLUDED.user_id;
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
  -c "DROP TABLE staging.campaign;"
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
