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

-- drop unused columns
ALTER TABLE ai_chat
DROP COLUMN IF EXISTS created_at, updated_at;
