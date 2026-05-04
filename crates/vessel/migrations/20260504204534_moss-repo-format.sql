ALTER TABLE channel_version ADD COLUMN format TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE channel_version_entry ADD COLUMN format TEXT NOT NULL DEFAULT 'legacy';
