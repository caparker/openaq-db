CREATE TABLE rejects (t timestamptz, tbl text, r jsonb);

ALTER TABLE rejects
ADD COLUMN fetchlogs_id int
REFERENCES fetchlogs ON DELETE CASCADE;
