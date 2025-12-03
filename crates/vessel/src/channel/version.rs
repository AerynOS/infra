use std::{collections::HashSet, fmt, time::Duration};

use chrono::Utc;
use futures_util::TryStreamExt;
use service::database::Transaction;
use snafu::Snafu;
use sqlx::{FromRow, SqliteConnection};
use tokio::time;
use tracing::debug;

/// A specific version of packages within a channel
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Version {
    /// All indexes flow through here. All other index versions
    /// are simply copied from here as a tag, volatile or unstable
    History {
        /// Identifier of this history entry
        identifier: Identifier,
    },
    /// Custom immutable tags created from `history` index
    Tag {
        /// Identifier of this tag
        identifier: Identifier,
    },
    /// Auto-updates to latest `history` index
    Volatile,
    /// Manually updated to a `history` index
    Unstable,
}

impl Version {
    /// Unique slug that references this version
    pub fn slug(&self) -> String {
        match self {
            Version::History { identifier } => format!("history/{identifier}"),
            Version::Tag { identifier, .. } => format!("tag/{identifier}"),
            Version::Volatile => "stream/volatile".to_owned(),
            Version::Unstable => "stream/unstable".to_owned(),
        }
    }

    /// Relative path to the base directory where all index files associated
    /// to this version will live
    pub fn relative_base_dir(&self) -> String {
        self.slug()
    }

    /// Relative path to a specific index file for the provided `arch`
    pub fn relative_index_path(&self, arch: &str) -> String {
        format!("{}/{arch}/stone.index", self.relative_base_dir())
    }

    /// Relative path to traverse from an index file to the channel root directory
    pub fn relative_index_to_channel_root(&self) -> &'static str {
        match self {
            Version::History { .. } | Version::Tag { .. } | Version::Volatile | Version::Unstable => "../../..",
        }
    }

    /// Relative path to traverse from an index file to an entries file
    ///
    /// This is the value we store as the associated meta URI so moss knows
    /// how to concatenate index URI + this to download the respective stone
    /// for this entry
    pub fn relative_index_to_entry_path(&self, entry: &Entry) -> String {
        format!("{}/{}", self.relative_index_to_channel_root(), entry.relative_path())
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.slug().fmt(f)
    }
}

impl TryFrom<String> for Version {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.split_once('/').ok_or("missing delimiter")? {
            ("history", ident) => Ok(Version::History {
                identifier: Identifier::new(ident).map_err(|_| "invalid idenitifier")?,
            }),
            ("tag", ident) => Ok(Version::Tag {
                identifier: Identifier::new(ident).map_err(|_| "invalid idenitifier")?,
            }),
            ("stream", "volatile") => Ok(Version::Volatile),
            ("stream", "unstable") => Ok(Version::Unstable),
            _ => Err("unknown version"),
        }
    }
}

/// A URL safe identifier which matches `[a-zA-Z0-9][a-zA-Z0-9-]*`
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Display, derive_more::AsRef)]
pub struct Identifier(String);

impl Identifier {
    pub fn new(s: impl ToString) -> Result<Self, InvalidIdentifier> {
        let s = s.to_string();

        if !s.is_empty() && s.as_bytes()[0] != b'-' && s.chars().all(|char| char.is_alphanumeric() || char == '-') {
            Ok(Self(s.to_owned()))
        } else {
            Err(InvalidIdentifier {
                identifier: s.to_owned(),
            })
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(display("invalid identifier `{identifier}`, must match [a-zA-Z0-9][a-zA-Z0-9-]*"))]
pub struct InvalidIdentifier {
    identifier: String,
}

/// An entry in a channel version
#[derive(Debug, Clone, PartialEq, Eq, Hash, FromRow)]
pub struct Entry {
    pub package_id: String,
    pub name: String,
    pub arch: String,
    pub source_id: String,
    pub source_version: String,
    pub source_release: i64,
    pub build_release: i64,
}

impl Entry {
    pub fn new(id: &moss::package::Id, meta: &moss::package::Meta) -> Self {
        Self {
            package_id: id.to_string(),
            name: meta.name.to_string(),
            arch: meta.architecture.clone(),
            source_id: meta.source_id.clone(),
            source_version: meta.version_identifier.clone(),
            source_release: meta.source_release as i64,
            build_release: meta.build_release as i64,
        }
    }

    /// Current definition of how we compute an on-disk filename for the provided [`Entry`].
    pub fn filename(&self) -> String {
        format!(
            "{}-{}-{}-{}-{}.stone",
            self.name, self.source_version, self.source_release, self.build_release, self.arch
        )
    }

    /// Relative path under channel root that this entry lives at
    pub fn relative_path(&self) -> String {
        let source_id = self.source_id.to_lowercase();

        let mut portion = &source_id[0..1];

        if source_id.len() > 4 && source_id.starts_with("lib") {
            portion = &source_id[0..4];
        }

        format!("pool/{portion}/{source_id}/{}", self.filename())
    }
}

/// Lookup if a package name already exists in the provided channel version
pub async fn lookup(
    conn: &mut SqliteConnection,
    channel: &str,
    version: &Version,
    name: &str,
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
          AND name = ?;
        ",
    )
    .bind(history.channel_version_id)
    .bind(name)
    .fetch_optional(&mut *conn)
    .await
}

/// List entries for the provided channel version
pub async fn list(conn: &mut SqliteConnection, channel: &str, version: &Version) -> sqlx::Result<Vec<Entry>> {
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
    .fetch_all(&mut *conn)
    .await
}

/// List all entries for the provided channel across all versions
pub async fn list_all(conn: &mut SqliteConnection, channel: &str) -> sqlx::Result<HashSet<Entry>> {
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
    .fetch(&mut *conn)
    .try_fold(HashSet::new(), |mut acc, a| async move {
        acc.insert(a);
        Ok(acc)
    })
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
pub async fn record(tx: &mut Transaction, channel: &str, entries: &[Entry]) -> sqlx::Result<Version> {
    let prev_history = find_related_history(tx.as_mut(), channel, &Version::Volatile).await?;
    let prev_version = prev_history.as_ref().map(|h| h.version.to_string()).unwrap_or_default();

    let new_version = loop {
        let now = Utc::now().timestamp();
        let identifier = Identifier::new(now).expect("numeric identifier");
        let new_version = Version::History { identifier };

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

        break new_version;
    };

    debug!(
        %channel,
        %prev_version,
        %new_version,
        num_entries = entries.len(),
        "Recording new channel history"
    );

    // Create new version row
    let (new_history_id,) = sqlx::query_as::<_, (i64,)>(
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
        .bind(new_history_id)
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
            ON CONFLICT(channel_version_id, name) DO UPDATE SET
              package_id=excluded.package_id,
              arch=excluded.arch,
              source_id=excluded.source_id,
              source_version=excluded.source_version,
              source_release=excluded.source_release,
              build_release=excluded.build_release;
            "
        );

        let mut query = sqlx::query(&query_str);

        for entry in chunk {
            query = query
                .bind(new_history_id)
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

    // Link volatile to this new history id
    sqlx::query(
        "
        INSERT INTO channel_version
        (
          channel,
          version,
          history_id
        )
        VALUES (?,?,?)
        ON CONFLICT(channel, version) DO UPDATE SET
          history_id=excluded.history_id;
        ",
    )
    .bind(channel)
    .bind(Version::Volatile.slug())
    .bind(new_history_id)
    .execute(tx.as_mut())
    .await?;

    Ok(new_version)
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
            identifier: Identifier::new(s).unwrap(),
        }
    }

    fn tag(s: &str) -> Version {
        Version::Tag {
            identifier: Identifier::new(s).unwrap(),
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
}
