-- convert CamelCase names to snake_case

DO $$
DECLARE
    stmt TEXT;
BEGIN
    FOR stmt IN
        SELECT format(
            'ALTER TABLE campaign RENAME COLUMN "%I" TO %I;',
            column_name,
            regexp_replace(column_name, '([a-z0-9])([A-Z])', '\1_\2', 'g')::text
        )
    FROM information_schema.columns
    WHERE table_name = 'campaign'
    LOOP
        EXECUTE stmt;
    END LOOP;
END $$;

-- convert the following columns to timestamp
ALTER TABLE campaign ALTER COLUMN created_at TYPE timestamp USING (created_at/1000)::timestamp;
ALTER TABLE campaign ALTER COLUMN updated_at TYPE timestamp USING (updated_at/1000)::timestamp;
ALTER TABLE campaign ALTER COLUMN date_verified TYPE timestamp USING date_verified::timestamp;
