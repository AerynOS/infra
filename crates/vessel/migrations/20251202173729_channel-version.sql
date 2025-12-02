CREATE TABLE IF NOT EXISTS channel_version (
  channel_version_id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL,
  version TEXT NOT NULL,
  history_id INTEGER,
  created BIGINT NOT NULL DEFAULT (unixepoch ()),
  updated BIGINT NOT NULL DEFAULT (unixepoch ()),
  UNIQUE (channel, version),
  FOREIGN KEY (history_id) REFERENCES channel_version (channel_version_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS channel_version_entry (
  channel_version_entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel_version_id INTEGER NOT NULL,
  package_id TEXT NOT NULL,
  name TEXT NOT NULL,
  arch TEXT NOT NULL,
  source_id TEXT NOT NULL,
  source_version TEXT NOT NULL,
  source_release BIGINT NOT NULL,
  build_release BIGINT NOT NULL,
  UNIQUE (channel_version_id, name),
  FOREIGN KEY (channel_version_id) REFERENCES channel_version (channel_version_id) ON DELETE CASCADE
);

-- Will get handled at runtime to migate this to new format
ALTER TABLE collection RENAME TO pending_migration_collection;
