use std::fmt;

use moss::repository::Format;
use snafu::Snafu;
use sqlx::FromRow;

pub use moss::repository::format::Identifier;

/// A specific version of packages within a channel
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Version {
    /// All indexes flow through here. All other index versions
    /// are simply copied from here as a tag, volatile or unstable
    History {
        /// Identifier of this history entry
        identifier: HistoryIdentifier,
    },
    /// Custom immutable tags created from `history` index
    Tag {
        /// Identifier of this tag
        identifier: TagIdentifier,
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
                identifier: HistoryIdentifier(Identifier::new(ident).map_err(|_| "invalid identifier")?),
            }),
            ("tag", ident) => Ok(Version::Tag {
                identifier: TagIdentifier(Identifier::new(ident).map_err(|_| "invalid identifier")?),
            }),
            ("stream", "volatile") => Ok(Version::Volatile),
            ("stream", "unstable") => Ok(Version::Unstable),
            ("stream", _) => Err("invalid stream"),
            _ => Err("invalid version slug"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum Stream {
    Volatile,
    Unstable,
}

impl Stream {
    pub fn version(&self) -> Version {
        match self {
            Stream::Volatile => Version::Volatile,
            Stream::Unstable => Version::Unstable,
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    derive_more::From,
    derive_more::Into,
    derive_more::Display,
    derive_more::AsRef,
)]
#[as_ref(forward)]
pub struct HistoryIdentifier(Identifier);

impl HistoryIdentifier {
    /// Relative path to traverse from a history index file to the channel root directory
    pub const RELATIVE_INDEX_TO_CHANNEL_ROOT: &'static str = "../../..";

    /// Relative path to the base directory where all index files associated
    /// to this history version will live
    pub fn relative_base_dir(&self) -> String {
        Version::History {
            identifier: self.clone(),
        }
        .slug()
    }

    /// Relative path to a specific history index file for the provided `arch`
    pub fn relative_index_path(&self, arch: &str) -> String {
        format!("history/{self}/{arch}/stone.index")
    }

    /// Relative path to traverse from a history index file to an entries file
    ///
    /// This is the value we store as the associated meta URI so moss knows
    /// how to concatenate index URI + this to download the respective stone
    /// for this entry
    pub fn relative_index_to_entry_path(&self, entry: &Entry) -> String {
        format!("{}/{}", Self::RELATIVE_INDEX_TO_CHANNEL_ROOT, entry.relative_path())
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    derive_more::From,
    derive_more::Into,
    derive_more::Display,
    derive_more::AsRef,
)]
#[as_ref(forward)]
pub struct TagIdentifier(Identifier);

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
    #[sqlx(try_from = "&'a str")]
    pub format: Format,
}

impl Entry {
    pub fn new(id: &moss::package::Id, meta: &moss::package::Meta, format: Format) -> Self {
        Self {
            package_id: id.to_string(),
            name: meta.name.to_string(),
            arch: meta.architecture.clone(),
            source_id: meta.source_id.clone(),
            source_version: meta.version_identifier.clone(),
            source_release: meta.source_release as i64,
            build_release: meta.build_release as i64,
            format,
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
        let prefix = match &self.format {
            Format::Legacy => "legacy/pool".to_owned(),
            Format::V0 => "pool/v0".to_owned(),
            Format::Unsupported(format) => format!("pool/{format}"),
        };

        let source_id = self.source_id.to_lowercase();

        let mut portion = &source_id[0..1];

        if source_id.len() > 4 && source_id.starts_with("lib") {
            portion = &source_id[0..4];
        }

        format!("{prefix}/{portion}/{source_id}/{}", self.filename())
    }
}

/// Wrapper around [`Version`] for legacy format aware
pub struct LegacyVersion<'a> {
    pub version: &'a Version,
}

impl LegacyVersion<'_> {
    /// Relative path to the base directory where all index files associated
    /// to this legacy version will live
    pub fn relative_base_dir(&self) -> String {
        format!("legacy/{}", self.version.slug())
    }

    /// Relative path to construct a symlink from this version's base dir
    /// to the supplied history index base dir
    pub fn relative_symlink_to_history(&self, history: &HistoryIdentifier) -> String {
        format!("../../{}", history.relative_base_dir())
    }
}
