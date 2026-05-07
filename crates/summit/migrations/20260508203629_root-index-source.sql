-- Add migration script here

ALTER TABLE profile RENAME COLUMN index_uri TO index_source;
ALTER TABLE profile_remote RENAME COLUMN index_uri TO index_source;

UPDATE profile
SET index_source = json_object('uri', index_source);

UPDATE profile_remote
SET index_source = json_object('uri', index_source);
