use std::{collections::HashSet, time::Duration};

use chrono::{DateTime, Utc};
use color_eyre::eyre::{OptionExt, Result, ensure, eyre};
use futures_util::TryStreamExt;
use moss::repository::Format;
use service::database::Transaction;
use sqlx::{FromRow, SqliteConnection};
use tokio::time;
use tracing::debug;

use crate::channel::version::{Entry, HistoryIdentifier, Identifier, TagIdentifier, Version};

/// Lookup if a package name already exists in the provided channel version
pub async fn lookup_entry(
    conn: &mut SqliteConnection,
    channel: &str,
    version: &Version,
    name: &str,
    arch: &str,
) -> sqlx::Result<Option<Entry>> {
    let Some(history) = find_related_history(conn, channel, version).await? else {
        return Ok(None);
    };

    sqlx::query_as(
        "
        SELECT
          package_id,
          name,
          arch,
          source_id,
          source_version,
          source_release,
          build_release,
          format
        FROM
          channel_version_entry
        WHERE
          channel_version_id = ?
          AND name = ?
          AND arch = ?;
        ",
    )
    .bind(history.channel_version_id)
    .bind(name)
    .bind(arch)
    .fetch_optional(conn)
    .await
}

/// List entries for the provided channel version
pub async fn entries(conn: &mut SqliteConnection, channel: &str, version: &Version) -> sqlx::Result<Vec<Entry>> {
    let Some(history) = find_related_history(conn, channel, version).await? else {
        return Ok(vec![]);
    };

    sqlx::query_as(
        "
        SELECT
          package_id,
          name,
          arch,
          source_id,
          source_version,
          source_release,
          build_release,
          format
        FROM
          channel_version_entry
        WHERE
          channel_version_id = ?;
        ",
    )
    .bind(history.channel_version_id)
    .fetch_all(conn)
    .await
}

/// List all entries for the provided channel across all versions
pub async fn all_entries(conn: &mut SqliteConnection, channel: &str) -> sqlx::Result<HashSet<Entry>> {
    sqlx::query_as(
        "
        SELECT
          package_id,
          name,
          arch,
          source_id,
          source_version,
          source_release,
          build_release,
          format
        FROM
          channel_version_entry
        WHERE
          channel_version_id IN (SELECT channel_version_id FROM channel_version WHERE channel = ?);
        ",
    )
    .bind(channel)
    .fetch(conn)
    .try_collect()
    .await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionRow {
    pub version: Version,
    pub format: Format,
    pub history: Option<Version>,
}

/// List all versions for the provided channel with most recently updated versions returned first
pub async fn all_versions(conn: &mut SqliteConnection, channel: &str) -> Result<Vec<VersionRow>> {
    #[derive(Debug, FromRow)]
    struct Row {
        #[sqlx(try_from = "String")]
        version: Version,
        #[sqlx(try_from = "&'a str")]
        format: Format,
        history: Option<String>,
    }

    let rows = sqlx::query_as::<_, Row>(
        "
        SELECT
          a.version,
          a.format,
          b.version as history
        FROM
          channel_version a
          LEFT JOIN channel_version b ON a.history_id = b.channel_version_id
        WHERE
          a.channel = ?
        ORDER BY
          a.updated DESC,
          a.channel_version_id
        ",
    )
    .bind(channel)
    .fetch(conn)
    .try_collect::<Vec<_>>()
    .await?;

    rows.into_iter()
        .map(|row| {
            let history = row
                .history
                .map(Version::try_from)
                .transpose()
                .map_err(|e| eyre!("parse channel_version.version as Version: {e}"))?;

            Ok(VersionRow {
                version: row.version,
                format: row.format,
                history,
            })
        })
        .collect()
}

#[derive(Debug, FromRow)]
pub struct HistoryVersion {
    channel_version_id: i64,
    #[sqlx(try_from = "String")]
    pub version: Version,
    #[sqlx(try_from = "&'a str")]
    pub format: Format,
}

/// Finds the related history for the incoming version
pub async fn find_related_history(
    conn: &mut SqliteConnection,
    channel: &str,
    version: &Version,
) -> sqlx::Result<Option<HistoryVersion>> {
    let where_clause = if matches!(version, Version::History { .. }) {
        "WHERE channel = ? AND version = ?"
    } else {
        "
        WHERE channel_version_id = (
          SELECT
            history_id
          FROM
            channel_version
          WHERE
            channel = ?
            AND version = ?
        )
        "
    };

    sqlx::query_as(&format!(
        "
        SELECT
          channel_version_id,
          version,
          format
        FROM
          channel_version
        {where_clause}
        ",
    ))
    .bind(channel)
    .bind(version.slug())
    .fetch_optional(conn)
    .await
}

/// Records a new `history` version with the supplied entries and returns
/// the [`Version`] for that new history.
///
/// This also updates `volatile` to link to this newer version
pub async fn record_history(
    tx: &mut Transaction,
    channel: &str,
    entries: &[Entry],
    format: &Format,
) -> Result<HistoryVersion> {
    let prev_history = find_related_history(tx.as_mut(), channel, &Version::Volatile).await?;
    let prev_version = prev_history.as_ref().map(|h| h.version.to_string()).unwrap_or_default();
    let prev_format = prev_history.as_ref().map(|h| &h.format);

    if let Some(prev_format) = prev_format {
        ensure!(
            format >= prev_format,
            "cannot downgrade format from volatile {prev_format} to {format}"
        );
    }

    let (new_identifier, new_version) = loop {
        let now = Utc::now().timestamp();
        let identifier = HistoryIdentifier::from(Identifier::new(&now.to_string()).expect("numeric identifier"));
        let new_version = Version::History {
            identifier: identifier.clone(),
        };

        // Record can only be called every 1s per channel
        // since we use unix timestamp seconds as the history
        // identifier
        //
        // This will go away when we require caller to provide
        // a unique history reference to record
        if prev_version == new_version.slug() {
            time::sleep(Duration::from_secs(1)).await;

            continue;
        }

        break (identifier, new_version);
    };

    debug!(
        %channel,
        %prev_version,
        %new_version,
        num_entries = entries.len(),
        "Recording new channel history"
    );

    // Create new version row
    let (new_id,) = sqlx::query_as::<_, (i64,)>(
        "
        INSERT INTO channel_version
        (
          channel,
          version,
          format
        )
        VALUES (?,?,?)
        RETURNING channel_version_id;
        ",
    )
    .bind(channel)
    .bind(new_version.slug())
    .bind(format.to_string())
    .fetch_one(tx.as_mut())
    .await?;

    // Copy entries from old version to new version
    if let Some(prev_history) = prev_history {
        sqlx::query(
            "
            INSERT INTO channel_version_entry
            (
              channel_version_id,
              package_id,
              name,
              arch,
              source_id,
              source_version,
              source_release,
              build_release,
              format
            )
            SELECT
              ?,
              package_id,
              name,
              arch,
              source_id,
              source_version,
              source_release,
              build_release,
              format
            FROM
              channel_version_entry
            WHERE
              channel_version_id = ?
            ",
        )
        .bind(new_id)
        .bind(prev_history.channel_version_id)
        .execute(tx.as_mut())
        .await?;
    }

    // Update new version with incoming entries
    for chunk in entries.chunks(32766 / 8) {
        let values_binds = ",(?,?,?,?,?,?,?,?,?)"
            .repeat(chunk.len())
            .chars()
            .skip(1)
            .collect::<String>();

        let query_str = format!(
            "
            INSERT INTO channel_version_entry
            (
              channel_version_id,
              package_id,
              name,
              arch,
              source_id,
              source_version,
              source_release,
              build_release,
              format
            )
            VALUES {values_binds}
            ON CONFLICT(channel_version_id, name, arch) DO UPDATE SET
              package_id=excluded.package_id,
              source_id=excluded.source_id,
              source_version=excluded.source_version,
              source_release=excluded.source_release,
              build_release=excluded.build_release,
              format=excluded.format;
            "
        );

        let mut query = sqlx::query(&query_str);

        for entry in chunk {
            query = query
                .bind(new_id)
                .bind(&entry.package_id)
                .bind(&entry.name)
                .bind(&entry.arch)
                .bind(&entry.source_id)
                .bind(&entry.source_version)
                .bind(entry.source_release)
                .bind(entry.build_release)
                .bind(entry.format.to_string());
        }

        query.execute(tx.as_mut()).await?;
    }

    // Link volatile to new history entry
    link_version_to_history(tx, channel, &Version::Volatile, &new_identifier).await?;

    Ok(HistoryVersion {
        channel_version_id: new_id,
        version: new_version,
        format: format.clone(),
    })
}

pub async fn link_version_to_history(
    tx: &mut Transaction,
    channel: &str,
    version: &Version,
    history: &HistoryIdentifier,
) -> Result<()> {
    let history_version = Version::History {
        identifier: history.clone(),
    };

    let history = find_related_history(tx.as_mut(), channel, &history_version)
        .await?
        .ok_or_eyre(format!("{history_version} doesn't exist in db"))?;

    let result = sqlx::query(
        "
        INSERT INTO channel_version
        (
          channel,
          version,
          format,
          history_id
        )
        SELECT
          ?,
          ?,
          ?,
          channel_version_id
        FROM
          channel_version
        WHERE
          channel = ?
          AND version = ?
        ON CONFLICT(channel, version) DO UPDATE SET
          format=excluded.format,
          history_id=excluded.history_id,
          updated=unixepoch();
        ",
    )
    .bind(channel)
    .bind(version.slug())
    .bind(history.format.to_string())
    .bind(channel)
    .bind(history_version.slug())
    .execute(tx.as_mut())
    .await?;

    ensure!(
        result.rows_affected() == 1,
        "{history_version} doesn't exist in channel {channel}"
    );

    Ok(())
}

/// Delete all history versions that aren't currently linked against
/// and are created before the provided time
pub async fn delete_stale_history(
    tx: &mut Transaction,
    channel: &str,
    created_before: DateTime<Utc>,
) -> sqlx::Result<Vec<HistoryVersion>> {
    sqlx::query_as::<_, HistoryVersion>(
        "
        WITH referenced_history AS (
          SELECT DISTINCT
            history_id
          FROM
            channel_version
          WHERE
            channel = ?
            AND history_id IS NOT NULL
        )
        DELETE FROM
          channel_version
        WHERE
          channel = ?
          AND version LIKE 'history/%'
          AND channel_version_id NOT IN (SELECT * FROM referenced_history)
          AND created < ?
        RETURNING
          channel_version_id,
          version,
          format
        ",
    )
    .bind(channel)
    .bind(channel)
    .bind(created_before.timestamp())
    .fetch_all(tx.as_mut())
    .await
}

/// Delete a tag
pub async fn delete_tag(tx: &mut Transaction, channel: &str, tag: &TagIdentifier) -> Result<()> {
    let tag_version = Version::Tag {
        identifier: tag.clone(),
    };

    let result = sqlx::query(
        "
        DELETE FROM
          channel_version
        WHERE
          channel = ?
          AND version =?
        ",
    )
    .bind(channel)
    .bind(tag_version.slug())
    .execute(tx.as_mut())
    .await?;

    ensure!(
        result.rows_affected() == 1,
        "{tag_version} doesn't exist in channel {channel}"
    );

    Ok(())
}

#[cfg(test)]
mod test {
    #![allow(clippy::too_many_arguments)]

    use test_case::test_case;
    use url::Url;

    use crate::channel::version::LegacyVersion;

    use super::*;

    #[test]
    fn test_identifier() {
        assert!(Identifier::new("123789").is_ok());
        assert!(Identifier::new("abcxyz").is_ok());
        assert!(Identifier::new("abc123789xyz").is_ok());
        assert!(Identifier::new("abc123-789xyz").is_ok());

        assert!(Identifier::new("-abc123-789xyz").is_err());

        for char in [
            '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '+', '=', '[', ']', '{', '}', '|', '\\', '/', ';',
            ':', '\'', '"', ',', '.', '<', '>', '?', '`', '~', '\n', '\t',
        ] {
            assert!(Identifier::new(&format!("abc123{char}789xyz")).is_err());
        }
    }

    #[test_case(volatile(), "legacy/stream/volatile", "../../history/0")]
    #[test_case(unstable(), "legacy/stream/unstable", "../../history/0")]
    #[test_case(tag("some-tag"), "legacy/tag/some-tag", "../../history/0")]
    fn test_legacy_symlinks(version: Version, expected_base_dir: &str, expected_symlink_source: &str) {
        let history = HistoryIdentifier::from(Identifier::new("0").unwrap());
        let version = LegacyVersion { version: &version };

        let base_dir = version.relative_base_dir();
        let symlink_source = version.relative_symlink_to_history(&history);

        assert_eq!(base_dir, expected_base_dir);
        assert_eq!(symlink_source, expected_symlink_source);

        let url = Url::from_file_path(format!("/{base_dir}")).unwrap();
        let history_url = url.join(&symlink_source).unwrap();

        assert_eq!(history_url.path(), "/history/0");
    }

    #[test_case(
        entry("nano", "nano", "x86_64", "0.1.0", 1, 1, Format::Legacy),
        "nano-0.1.0-1-1-x86_64.stone",
        "legacy/pool/n/nano"
    )]
    #[test_case(
        entry("less", "less", "x86_64", "2.0", 1, 1, Format::V0),
        "less-2.0-1-1-x86_64.stone",
        "pool/v0/l/less"
    )]
    #[test_case(
        entry("libzip", "libzip", "aarch64", "abdefg.1-alpha", 99, 50, Format::V0),
        "libzip-abdefg.1-alpha-99-50-aarch64.stone",
        "pool/v0/libz/libzip"
    )]
    fn test_entry_paths(entry: Entry, expected_filename: &str, expected_relative_base: &str) {
        let filename = entry.filename();
        let relative_path = entry.relative_path();

        assert_eq!(filename, expected_filename);
        assert_eq!(relative_path, format!("{expected_relative_base}/{expected_filename}"));

        // Confirm we can construct a relative path to the file from the index by joining
        // the relative path to pool dir + relative path under pool to file

        let history = HistoryIdentifier::from(Identifier::new("0").unwrap());
        let index_url = Url::from_file_path(format!("/{}", history.relative_index_path("x86_64"))).unwrap();
        let root_url = index_url
            .join(HistoryIdentifier::RELATIVE_INDEX_TO_CHANNEL_ROOT)
            .unwrap();
        let file_path = root_url.join(&format!("./{relative_path}")).unwrap();

        assert_eq!(
            file_path.path(),
            format!("/{expected_relative_base}/{expected_filename}",)
        );
        // Manually constructed equals helper function
        assert_eq!(
            file_path.path(),
            index_url
                .join(&history.relative_index_to_entry_path(&entry))
                .unwrap()
                .path(),
        );
    }

    fn volatile() -> Version {
        Version::Volatile
    }

    fn unstable() -> Version {
        Version::Unstable
    }

    fn tag(s: &str) -> Version {
        Version::Tag {
            identifier: Identifier::new(s).unwrap().into(),
        }
    }

    fn entry(
        name: &str,
        source_id: &str,
        arch: &str,
        source_version: &str,
        source_release: i64,
        build_release: i64,
        format: Format,
    ) -> Entry {
        Entry {
            package_id: "123456".to_owned(),
            name: name.to_owned(),
            arch: arch.to_owned(),
            source_id: source_id.to_owned(),
            source_release,
            source_version: source_version.to_owned(),
            build_release,
            format,
        }
    }

    #[tokio::test]
    async fn test_delete_stale_history() {
        const CHANNEL: &str = "test";
        const FORMAT: Format = Format::Legacy;

        let db = crate::test::database().await;

        let mut tx = db.begin().await.unwrap();
        let mut deletable = HashSet::new();

        // Record before cutoff
        deletable.insert(record_history(&mut tx, CHANNEL, &[], &FORMAT).await.unwrap().version);

        // Wait 1 second
        time::sleep(Duration::from_secs(1)).await;

        let cutoff = Utc::now();

        // Record after cutoff
        record_history(&mut tx, CHANNEL, &[], &FORMAT).await.unwrap();

        // Record new state, links volatile to this
        record_history(&mut tx, CHANNEL, &[], &FORMAT).await.unwrap();

        // Previous 3 states should be removable
        let deleted = delete_stale_history(&mut tx, CHANNEL, cutoff)
            .await
            .unwrap()
            .into_iter()
            .map(|h| h.version)
            .collect::<HashSet<_>>();

        assert_eq!(deleted, deletable);
    }

    #[tokio::test]
    async fn test_record_history_format_mismatch() {
        const CHANNEL: &str = "test";

        let db = crate::test::database().await;

        let mut tx = db.begin().await.unwrap();

        // Record volatile as `v0`
        record_history(&mut tx, CHANNEL, &[], &Format::V0).await.unwrap();

        // Upgrading is allowed
        record_history(
            &mut tx,
            CHANNEL,
            &[],
            &Format::Unsupported("some-new-version".to_owned()),
        )
        .await
        .unwrap();

        // Downgrading is an error
        let err = record_history(&mut tx, CHANNEL, &[], &Format::Legacy)
            .await
            .expect_err("format mismatch is error");

        assert!(err.to_string().contains("cannot downgrade format"));
    }

    #[tokio::test]
    async fn test_all_versions() {
        const CHANNEL: &str = "test";
        const FORMAT: Format = Format::Legacy;

        let db = crate::test::database().await;

        let mut tx = db.begin().await.unwrap();

        let a = record_history(&mut tx, CHANNEL, &[], &Format::Legacy).await.unwrap();
        let b = record_history(&mut tx, CHANNEL, &[], &Format::Legacy).await.unwrap();
        let c = record_history(&mut tx, CHANNEL, &[], &Format::Legacy).await.unwrap();

        let expected_versions = vec![
            VersionRow {
                version: volatile(),
                format: FORMAT.clone(),
                history: Some(c.version.clone()),
            },
            VersionRow {
                version: c.version,
                format: FORMAT.clone(),
                history: None,
            },
            VersionRow {
                version: b.version,
                format: FORMAT.clone(),
                history: None,
            },
            VersionRow {
                version: a.version,
                format: FORMAT.clone(),
                history: None,
            },
        ];

        let versions = all_versions(tx.as_mut(), CHANNEL).await.unwrap();

        assert_eq!(versions, expected_versions);
    }
}
