use std::{collections::HashSet, time::Duration};

use chrono::{DateTime, Utc};
use color_eyre::eyre::{Result, ensure};
use futures_util::TryStreamExt;
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
          build_release
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
          build_release
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
          build_release
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

#[derive(Debug, FromRow)]
pub struct HistoryVersion {
    channel_version_id: i64,
    #[sqlx(try_from = "String")]
    pub version: Version,
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
          version
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
pub async fn record_history(tx: &mut Transaction, channel: &str, entries: &[Entry]) -> Result<Version> {
    let prev_history = find_related_history(tx.as_mut(), channel, &Version::Volatile).await?;
    let prev_version = prev_history.as_ref().map(|h| h.version.to_string()).unwrap_or_default();

    let (new_identifier, new_version) = loop {
        let now = Utc::now().timestamp();
        let identifier = HistoryIdentifier::from(Identifier::new(now).expect("numeric identifier"));
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
          version
        )
        VALUES (?,?)
        RETURNING channel_version_id;
        ",
    )
    .bind(channel)
    .bind(new_version.slug())
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
              build_release
            )
            SELECT
              ?,
              package_id,
              name,
              arch,
              source_id,
              source_version,
              source_release,
              build_release
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
        let values_binds = ",(?,?,?,?,?,?,?,?)"
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
              build_release
            )
            VALUES {values_binds}
            ON CONFLICT(channel_version_id, name, arch) DO UPDATE SET
              package_id=excluded.package_id,
              source_id=excluded.source_id,
              source_version=excluded.source_version,
              source_release=excluded.source_release,
              build_release=excluded.build_release;
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
                .bind(entry.build_release);
        }

        query.execute(tx.as_mut()).await?;
    }

    // Link volatile to new history entry
    link_version_to_history(tx, channel, &Version::Volatile, &new_identifier).await?;

    Ok(new_version)
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

    let result = sqlx::query(
        "
        INSERT INTO channel_version
        (
          channel,
          version,
          history_id
        )
        SELECT
          ?,
          ?,
          channel_version_id
        FROM
          channel_version
        WHERE
          channel = ?
          AND version = ?
        ON CONFLICT(channel, version) DO UPDATE SET
          history_id=excluded.history_id;
        ",
    )
    .bind(channel)
    .bind(version.slug())
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
) -> sqlx::Result<HashSet<Version>> {
    #[derive(FromRow)]
    struct Row {
        #[sqlx(try_from = "String")]
        version: Version,
    }

    Ok(sqlx::query_as::<_, Row>(
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
        RETURNING version
        ",
    )
    .bind(channel)
    .bind(channel)
    .bind(created_before.timestamp())
    .fetch_all(tx.as_mut())
    .await?
    .into_iter()
    .map(|row| row.version)
    .collect())
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
            assert!(Identifier::new(format!("abc123{char}789xyz")).is_err());
        }
    }

    #[test_case(volatile(), "x86_64", "stream/volatile/x86_64/stone.index", "../../..")]
    #[test_case(unstable(), "aarch64", "stream/unstable/aarch64/stone.index", "../../..")]
    #[test_case(history("12345"), "x86_64", "history/12345/x86_64/stone.index", "../../..")]
    #[test_case(tag("some-tag"), "aarch64", "tag/some-tag/aarch64/stone.index", "../../..")]
    fn test_version_paths(version: Version, arch: &str, expected_index_path: &str, expected_root_path: &str) {
        let index_path = version.relative_index_path(arch);
        let index_to_root = version.relative_index_to_channel_root();

        assert_eq!(index_path, expected_index_path);
        assert_eq!(index_to_root, expected_root_path);

        let url = Url::from_file_path(format!("/public/{index_path}")).unwrap();
        let root_url = url.join(index_to_root).unwrap();

        assert_eq!(root_url.path(), "/public/");
    }

    #[test_case(
        entry("nano", "nano", "x86_64", "0.1.0", 1, 1),
        "nano-0.1.0-1-1-x86_64.stone",
        "pool/n/nano"
    )]
    #[test_case(
        entry("less", "less", "x86_64", "2.0", 1, 1),
        "less-2.0-1-1-x86_64.stone",
        "pool/l/less"
    )]
    #[test_case(
        entry("libzip", "libzip", "aarch64", "abdefg.1-alpha", 99, 50),
        "libzip-abdefg.1-alpha-99-50-aarch64.stone",
        "pool/libz/libzip"
    )]
    fn test_entry_paths(entry: Entry, expected_filename: &str, expected_relative_base: &str) {
        let filename = entry.filename();
        let relative_path = entry.relative_path();

        assert_eq!(filename, expected_filename);
        assert_eq!(relative_path, format!("{expected_relative_base}/{expected_filename}"));

        // Confirm we can construct a relative path to the file from the index by joining
        // the relative path to pool dir + relative path under pool to file

        let version = volatile();
        let index_url = Url::from_file_path(format!("/public/{}", version.relative_index_path("x86_64"))).unwrap();
        let root_url = index_url.join(version.relative_index_to_channel_root()).unwrap();
        let file_path = root_url.join(&format!("./{relative_path}")).unwrap();

        assert_eq!(
            file_path.path(),
            format!("/public/{expected_relative_base}/{expected_filename}",)
        );
        // Manually constructed equals helper function
        assert_eq!(
            file_path.path(),
            index_url
                .join(&version.relative_index_to_entry_path(&entry))
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

    fn history(s: &str) -> Version {
        Version::History {
            identifier: Identifier::new(s).unwrap().into(),
        }
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
    ) -> Entry {
        Entry {
            package_id: "123456".to_owned(),
            name: name.to_owned(),
            arch: arch.to_owned(),
            source_id: source_id.to_owned(),
            source_release,
            source_version: source_version.to_owned(),
            build_release,
        }
    }

    #[tokio::test]
    async fn test_delete_stale_history() {
        const CHANNEL: &str = "test";

        let db = crate::test::database().await;

        let mut tx = db.begin().await.unwrap();
        let mut deletable = HashSet::new();

        // Record before cutoff
        deletable.insert(record_history(&mut tx, CHANNEL, &[]).await.unwrap());

        // Wait 1 second
        time::sleep(Duration::from_secs(1)).await;

        let cutoff = Utc::now();

        // Record after cutoff
        record_history(&mut tx, CHANNEL, &[]).await.unwrap();

        // Record new state, links volatile to this
        record_history(&mut tx, CHANNEL, &[]).await.unwrap();

        // Previous 3 states should be removable
        let deleted = delete_stale_history(&mut tx, CHANNEL, cutoff).await.unwrap();

        assert_eq!(deleted, deletable);
    }
}
