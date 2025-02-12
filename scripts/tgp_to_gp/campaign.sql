-- rename the following columns
ALTER TABLE campaign RENAME COLUMN "createdAt" to created_at;
ALTER TABLE campaign RENAME COLUMN "updatedAt" to updated_at;
ALTER TABLE campaign RENAME COLUMN "dateVerified" to date_verified;
ALTER TABLE campaign RENAME COLUMN "isActive" TO is_active;
ALTER TABLE campaign RENAME COLUMN "isVerified" TO is_verified;
ALTER TABLE campaign RENAME COLUMN "isPro" TO is_pro;
ALTER TABLE campaign RENAME COLUMN "didWin" TO did_win;
ALTER TABLE campaign RENAME COLUMN "dateVerified" TO date_verified;
ALTER TABLE campaign RENAME COLUMN "dataCopy" TO data_copy;
ALTER TABLE campaign RENAME COLUMN "pathToVictory" TO path_to_victory;
ALTER TABLE campaign RENAME COLUMN "aiContent" TO ai_content;
ALTER TABLE campaign RENAME COLUMN "ballotCandidate" TO ballot_candidate;
ALTER TABLE campaign RENAME COLUMN "isDemo" TO is_demo;
ALTER TABLE campaign RENAME COLUMN "vendorTsData" TO vendor_ts_data;

-- convert the following columns to timestamp
ALTER TABLE campaign ALTER COLUMN created_at TYPE timestamp USING (created_at/1000)::timestamp;
ALTER TABLE campaign ALTER COLUMN updated_at TYPE timestamp USING (updated_at/1000)::timestamp;
ALTER TABLE campaign ALTER COLUMN date_verified TYPE timestamp USING date_verified::timestamp;
