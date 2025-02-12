-- rename the table
ALTER TABLE aichat RENAME TO ai_chat;

-- rename columns
ALTER TABLE ai_chat RENAME COLUMN thread TO thread_id;
ALTER TABLE ai_chat RENAME COLUMN campaign TO campaign_id;

-- case when to convert milliseconds to timestamp
ALTER TABLE ai_chat ADD COLUMN created_at TIMESTAMP;
ALTER TABLE ai_chat ADD COLUMN updated_at TIMESTAMP;

UPDATE ai_chat SET created_at = to_timestamp(createdAt / 1000.0);
UPDATE ai_chat SET updated_at = to_timestamp(updatedAt / 1000.0);

-- drop the original columns
ALTER TABLE ai_chat DROP COLUMN createdAt;
ALTER TABLE ai_chat DROP COLUMN updatedAt;

-- rename the new columns
ALTER TABLE ai_chat RENAME COLUMN created_at TO createdAt;
ALTER TABLE ai_chat RENAME COLUMN updated_at TO updatedAt;

-- drop unlisted columns
DO $$
DECLARE
    col_name text;
    allowed_columns text[] := ARRAY['thread_id', 'data', 'user_id', 'campaign_id', 'created_at', 'updated_at'];
BEGIN
    FOR col_name IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'ai_chat'
        AND column_name != ALL(allowed_columns)
    LOOP
        EXECUTE format('ALTER TABLE ai_chat DROP COLUMN IF EXISTS %I', col_name);
    END LOOP;
END $$;
