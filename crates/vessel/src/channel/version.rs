use std::fmt;

use snafu::Snafu;
use sqlx::FromRow;

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
                identifier: HistoryIdentifier(Identifier::new(ident).map_err(|_| "invalid idenitifier")?),
            }),
            ("tag", ident) => Ok(Version::Tag {
                identifier: TagIdentifier(Identifier::new(ident).map_err(|_| "invalid idenitifier")?),
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

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::From, derive_more::Display, derive_more::AsRef,
)]
#[as_ref(forward)]
pub struct HistoryIdentifier(Identifier);

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::From, derive_more::Display, derive_more::AsRef,
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
