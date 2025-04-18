//! Describe remote services and connect to them

use std::fmt;
use std::str::FromStr;

use chrono::Utc;
use derive_more::From;
use http::Uri;
use serde::{Deserialize, Serialize};
use service_grpc::endpoint::Role as ProtoRole;
use sqlx::FromRow;
use uuid::Uuid;

use crate::{
    Token, account, database,
    token::{self, VerifiedToken},
};

pub(crate) use self::service::service;

pub mod enrollment;

pub(crate) mod service;

/// Unique identifier of an [`Endpoint`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, From)]
#[serde(try_from = "&str", into = "String")]
pub struct Id(Uuid);

impl Id {
    /// Generate a new [`Id`]
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

impl FromStr for Id {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse::<Uuid>().map(Id)
    }
}

impl<'a> TryFrom<&'a str> for Id {
    type Error = uuid::Error;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        value.parse::<Uuid>().map(Id)
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Id> for String {
    fn from(id: Id) -> Self {
        id.to_string()
    }
}

/// Details of a remote endpoint (service) that we are connected to
#[derive(Debug, Clone, Serialize, FromRow)]
pub struct Endpoint {
    /// Unique identifier of the endpoint
    #[sqlx(rename = "endpoint_id", try_from = "Uuid")]
    pub id: Id,
    /// [`Uri`] we can reach the endpoint at
    #[serde(with = "http_serde::uri")]
    #[sqlx(try_from = "&'a str")]
    pub host_address: Uri,
    /// Current status of the endpoint
    #[sqlx(try_from = "&'a str")]
    pub status: Status,
    /// Error message, if any, due to the endpoint being in an
    /// error [`Status`]
    pub error: Option<String>,
    /// Related service account identifier for this endpoint
    #[sqlx(rename = "account_id", try_from = "Uuid")]
    pub account: account::Id,
    /// Account identifier for us on the remote service
    #[sqlx(rename = "remote_account_id", try_from = "Uuid")]
    pub remote_account: account::Id,
    /// Role specific data
    #[sqlx(flatten)]
    #[serde(flatten)]
    pub kind: Kind,
}

impl Endpoint {
    /// Get an endpoint with the provided [`Id`]
    pub async fn get<'a, T>(conn: &'a mut T, id: Id) -> Result<Self, database::Error>
    where
        &'a mut T: database::Executor<'a>,
    {
        let endpoint = sqlx::query_as(
            "
            SELECT
              endpoint_id,
              host_address,
              status,
              error,
              account_id,
              remote_account_id,
              role,
              work_status
            FROM endpoint
            WHERE endpoint_id = ?;
            ",
        )
        .bind(id.0)
        .fetch_one(conn)
        .await?;

        Ok(endpoint)
    }

    /// Get an endpoint associated to the provided [`account::Id`]
    pub async fn get_by_account_id<'a, T>(conn: &'a mut T, id: account::Id) -> Result<Option<Self>, database::Error>
    where
        &'a mut T: database::Executor<'a>,
    {
        let endpoint = sqlx::query_as(
            "
            SELECT
              endpoint_id,
              host_address,
              status,
              error,
              account_id,
              remote_account_id,
              role,
              work_status
            FROM endpoint
            WHERE account_id = ?;
            ",
        )
        .bind(Uuid::from(id))
        .fetch_optional(conn)
        .await?;

        Ok(endpoint)
    }

    /// Create or update this endpoint to the provided [`Database`]
    pub async fn save(&self, tx: &mut database::Transaction) -> Result<(), database::Error> {
        sqlx::query(
            "
            INSERT INTO endpoint
            (
              endpoint_id,
              host_address,
              status,
              error,
              account_id,
              remote_account_id,
              role,
              work_status
            )
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(account_id) DO UPDATE SET 
              host_address=excluded.host_address,
              status=excluded.status,
              error=excluded.error,
              account_id=excluded.account_id,
              remote_account_id=excluded.remote_account_id,
              role=excluded.role,
              work_status=excluded.work_status;
            ",
        )
        .bind(self.id.0)
        .bind(self.host_address.to_string())
        .bind(self.status.to_string())
        .bind(&self.error)
        .bind(Uuid::from(self.account))
        .bind(Uuid::from(self.remote_account))
        .bind(self.kind.role().to_string())
        .bind(self.kind.work_status().map(ToString::to_string))
        .execute(tx.as_mut())
        .await?;

        Ok(())
    }

    /// List all endpoints from the provided [`Database`]
    pub async fn list<'a, T>(conn: &'a mut T) -> Result<Vec<Endpoint>, database::Error>
    where
        &'a mut T: database::Executor<'a>,
    {
        let endpoints: Vec<Endpoint> = sqlx::query_as(
            "
            SELECT
              endpoint_id,
              host_address,
              status,
              error,
              account_id,
              remote_account_id,
              role,
              work_status
            FROM endpoint;
            ",
        )
        .fetch_all(conn)
        .await?;

        Ok(endpoints)
    }

    /// Delete this endpoint from the provided [`Database`]
    pub async fn delete(&self, tx: &mut database::Transaction) -> Result<(), database::Error> {
        sqlx::query(
            "
            DELETE FROM endpoint
            WHERE endpoint_id = ?;
            ",
        )
        .bind(self.id.0)
        .execute(tx.as_mut())
        .await?;

        Ok(())
    }

    /// Return [`builder`] related information if this endpoint is a [`Role::Builder`]
    pub fn builder(&self) -> Option<&builder::Extension> {
        if let Kind::Builder(ext) = &self.kind {
            Some(ext)
        } else {
            None
        }
    }

    /// Returns true if this endpoint is an idle, operational builder
    pub fn is_idle_builder(&self) -> bool {
        matches!(self.status, Status::Operational)
            && matches!(self.builder().map(|e| e.work_status), Some(builder::WorkStatus::Idle))
    }

    /// Set the work status of the builder endpoint
    pub fn set_work_status(&mut self, new_status: builder::WorkStatus) {
        if let Kind::Builder(builder::Extension { work_status }) = &mut self.kind {
            *work_status = new_status;
        }
    }
}

/// Auth tokens used to connect to the endpoint
#[derive(Debug, Clone, FromRow)]
pub struct Tokens {
    /// Current bearer token
    pub bearer_token: Option<String>,
    /// Current access token
    pub access_token: Option<String>,
}

impl Tokens {
    /// Save the tokens related to [`Id`] to the provided [`Database`]
    pub async fn save(&self, tx: &mut database::Transaction, id: Id) -> Result<(), database::Error> {
        sqlx::query(
            "
            UPDATE endpoint
            SET
              bearer_token = ?,
              access_token = ?
            WHERE endpoint_id = ?;
            ",
        )
        .bind(&self.bearer_token)
        .bind(&self.access_token)
        .bind(id.0)
        .execute(tx.as_mut())
        .await?;

        Ok(())
    }

    /// Get the tokens for [`Id`] from the provided [`Database`]
    pub async fn get<'a, T>(conn: &'a mut T, id: Id) -> Result<Self, database::Error>
    where
        &'a mut T: database::Executor<'a>,
    {
        let tokens: Tokens = sqlx::query_as(
            "
            SELECT
              bearer_token,
              access_token
            FROM endpoint
            WHERE endpoint_id = ?;
            ",
        )
        .bind(id.0)
        .fetch_one(conn)
        .await?;

        Ok(tokens)
    }
}

/// Status of the [`Endpoint`]
#[derive(Debug, Clone, Copy, strum::Display, strum::EnumString, Serialize)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum Status {
    /// Awaiting enrollment acceptance for the endpoint
    AwaitingAcceptance,
    /// Endpoint is in a failed state
    Failed,
    /// Endpoint is enrolled and operational
    Operational,
    /// Authorization to the endpoint is forbidden
    Forbidden,
    /// Endpoint cannot be reeached
    Unreachable,
}

/// Extension details related to the [`Role`] of the endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "role", content = "extension", rename_all = "kebab-case")]
pub enum Kind {
    /// Hub
    Hub,
    /// Repository Manager
    RepositoryManager,
    /// Builder
    Builder(builder::Extension),
}

impl Kind {
    /// [`Role`] defined by this endpoint
    pub fn role(&self) -> Role {
        match self {
            Self::Hub => Role::Hub,
            Self::RepositoryManager => Role::RepositoryManager,
            Self::Builder(_) => Role::Builder,
        }
    }

    /// Work status of a [`Role::Builder`] endpoint
    pub fn work_status(&self) -> Option<&builder::WorkStatus> {
        if let Self::Builder(ext) = self {
            Some(&ext.work_status)
        } else {
            None
        }
    }
}

impl<'a> FromRow<'a, sqlx::sqlite::SqliteRow> for Kind {
    fn from_row(row: &'a sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        #[derive(Debug, FromRow)]
        struct Row {
            #[sqlx(try_from = "&'a str")]
            role: Role,

            // Builder fields
            work_status: Option<String>,
        }

        let row = Row::from_row(row)?;

        match (row.role, row.work_status) {
            (Role::Builder, Some(value)) => {
                let work_status = value.parse().map_err(|e| sqlx::Error::Decode(Box::from(e)))?;
                Ok(Kind::Builder(builder::Extension { work_status }))
            }
            (Role::Builder, _) => Err(sqlx::Error::Decode(Box::from(
                "extension can't be null for builder endpoint",
            ))),
            (Role::Hub, _) => Ok(Kind::Hub),
            (Role::RepositoryManager, _) => Ok(Kind::RepositoryManager),
        }
    }
}

pub mod builder {
    //! Builder specific endpoint details
    use serde::{Deserialize, Serialize};

    /// Builder extension details
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Extension {
        /// Work status of the endpoint
        pub work_status: WorkStatus,
    }

    /// Work status of the builder
    #[derive(Debug, Clone, Copy, strum::Display, strum::EnumString, Serialize, Deserialize)]
    #[serde(rename_all = "kebab-case")]
    #[strum(serialize_all = "kebab-case")]
    pub enum WorkStatus {
        /// Builder is idle
        Idle,
        /// Builder is running
        Running,
    }
}

/// Endpoint role
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display, strum::EnumString)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum Role {
    /// Builder
    Builder,
    /// Repository Manager
    RepositoryManager,
    /// Hub
    Hub,
}

impl Role {
    /// Service name associated to each role
    pub fn service_name(&self) -> &'static str {
        match self {
            Role::Hub => "summit",
            Role::RepositoryManager => "vessel",
            Role::Builder => "avalanche",
        }
    }

    /// Convert from a protobuf role into a [`Role`]
    pub fn from_proto(role: ProtoRole) -> Option<Role> {
        match role {
            ProtoRole::Unknown => None,
            ProtoRole::Builder => Some(Role::Builder),
            ProtoRole::RepositoryManager => Some(Role::RepositoryManager),
            ProtoRole::Hub => Some(Role::Hub),
        }
    }
}

impl From<Role> for ProtoRole {
    fn from(role: Role) -> Self {
        match role {
            Role::Builder => ProtoRole::Builder,
            Role::RepositoryManager => ProtoRole::RepositoryManager,
            Role::Hub => ProtoRole::Hub,
        }
    }
}

pub(crate) fn create_token(
    purpose: token::Purpose,
    endpoint: Id,
    account: account::Id,
    role: Role,
    ourself: &enrollment::Issuer,
) -> Result<VerifiedToken, token::Error> {
    let now = Utc::now();
    let expires_on = now + purpose.duration();

    let token = Token::new(token::Payload {
        aud: role.service_name().to_string(),
        exp: expires_on.timestamp(),
        iat: now.timestamp(),
        iss: ourself.role.service_name().to_string(),
        sub: endpoint.to_string(),
        purpose,
        account_id: account.into(),
        account_type: account::Kind::Service,
    });
    let account_token = token.sign(&ourself.key_pair)?;

    Ok(VerifiedToken {
        encoded: account_token,
        decoded: token,
    })
}
