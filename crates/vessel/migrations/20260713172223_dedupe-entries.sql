CREATE TABLE IF NOT EXISTS channel_package (
  channel_package_id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL,
  format TEXT NOT NULL,
  package_id TEXT NOT NULL,
  name TEXT NOT NULL,
  arch TEXT NOT NULL,
  source_id TEXT NOT NULL,
  source_version TEXT NOT NULL,
  source_release BIGINT NOT NULL,
  build_release BIGINT NOT NULL,
  UNIQUE (channel, format, package_id)
);

CREATE TABLE IF NOT EXISTS channel_version_package (
  channel_version_package_id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel_version_id INTEGER NOT NULL,
  channel_package_id INTEGER NOT NULL,
  FOREIGN KEY (channel_version_id) REFERENCES channel_version (channel_version_id) ON DELETE CASCADE,
  FOREIGN KEY (channel_package_id) REFERENCES channel_package (channel_package_id) ON DELETE CASCADE,
  UNIQUE (channel_version_id, channel_package_id)
);

INSERT INTO channel_package (
  channel,
  format,
  package_id,
  name,
  arch,
  source_id,
  source_version,
  source_release,
  build_release
)
SELECT
  cv.channel,
  cve.format,
  cve.package_id,
  cve.name,
  cve.arch,
  cve.source_id,
  cve.source_version,
  cve.source_release,
  cve.build_release
FROM
  channel_version_entry cve
  JOIN channel_version cv USING (channel_version_id)
ON CONFLICT (channel, format, package_id) DO NOTHING;

INSERT INTO channel_version_package (
  channel_version_id,
  channel_package_id
)
SELECT
  cv.channel_version_id,
  cp.channel_package_id
FROM
  channel_version_entry cve
  JOIN channel_version cv USING (channel_version_id)
  JOIN channel_package cp ON cv.channel = cp.channel AND cve.format = cp.format AND cve.package_id = cp.package_id;
  
DROP TABLE channel_version_entry;
