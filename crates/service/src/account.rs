//! Manage data for admin, user, bot & service accounts

use std::str::FromStr;

use chrono::{DateTime, Utc};
use derive_more::{Display, From, Into};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use thiserror::Error;
use tracing::debug;
use uuid::Uuid;

use crate::{Database, crypto::EncodedPublicKey, database};

pub use service_core::account::Kind;

pub(crate) use self::service::service;

mod service;

/// Unique identifier of an [`Account`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, From, Into, Display)]
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

impl From<Id> for String {
    fn from(id: Id) -> Self {
        id.to_string()
    }
}

/// Details for an account registered with this service
#[derive(Debug, Clone, Serialize, FromRow)]
pub struct Account {
    /// Unique identifier of the account
    #[sqlx(rename = "account_id", try_from = "Uuid")]
    pub id: Id,
    /// Account type
    #[sqlx(rename = "type", try_from = "&'a str")]
    pub kind: Kind,
    /// Username
    pub username: String,
    /// Email
    pub email: Option<String>,
    /// Name
    pub name: Option<String>,
    /// Public key used for authentication
    #[sqlx(try_from = "String")]
    pub public_key: EncodedPublicKey,
}

impl Account {
    /// Create a service account
    pub fn service(id: Id, public_key: EncodedPublicKey) -> Self {
        Self {
            id,
            kind: Kind::Service,
            username: format!("@{id}"),
            email: None,
            name: None,
            public_key,
        }
    }

    /// Get the account for [`Id`] from the provided [`Database`]
    pub async fn get<'a, T>(conn: &'a mut T, id: Id) -> Result<Self, Error>
    where
        &'a mut T: database::Executor<'a>,
    {
        let account: Account = sqlx::query_as(
            "
            SELECT
              account_id,
              type,
              username,
              email,
              name,
              public_key
            FROM account
            WHERE account_id = ?;
            ",
        )
        .bind(id.0)
        .fetch_one(conn)
        .await?;

        Ok(account)
    }

    /// Lookup an account using `username` and `publickey` from the provided [`Database`]
    pub async fn lookup_with_credentials<'a, T>(
        conn: &'a mut T,
        username: &str,
        public_key: &EncodedPublicKey,
    ) -> Result<Self, Error>
    where
        &'a mut T: database::Executor<'a>,
    {
        let account: Account = sqlx::query_as(
            "
            SELECT
              account_id,
              type,
              username,
              email,
              name,
              public_key
            FROM account
            WHERE
              username = ?
              AND public_key = ?
              AND (type = 'admin' OR type = 'standard');
            ",
        )
        .bind(username)
        .bind(public_key.to_string())
        .fetch_one(conn)
        .await?;

        Ok(account)
    }

    /// Create / update this account to the provided [`Database`]
    pub async fn save(&self, tx: &mut database::Transaction) -> Result<(), Error> {
        sqlx::query(
            "
            INSERT INTO account
            (
              account_id,
              type,
              username,
              email,
              name,
              public_key
            )
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(account_id) DO UPDATE SET
              type=excluded.type,
              username=excluded.username,
              email=excluded.email,
              name=excluded.name,
              public_key=excluded.public_key;
            ",
        )
        .bind(self.id.0)
        .bind(self.kind.to_string())
        .bind(&self.username)
        .bind(&self.email)
        .bind(&self.name)
        .bind(self.public_key.to_string())
        .execute(tx.as_mut())
        .await?;

        Ok(())
    }
}

/// [`Account`] bearer token provisioned for the account after authentication
#[derive(Debug, Clone, FromRow)]
pub struct Token {
    /// Encoded bearer token string
    pub encoded: String,
    /// Token expiration time
    pub expiration: DateTime<Utc>,
}

impl Token {
    /// Set the account's bearer token & expiration
    pub async fn set(
        tx: &mut database::Transaction,
        id: Id,
        encoded: impl ToString,
        expiration: DateTime<Utc>,
    ) -> Result<(), Error> {
        sqlx::query(
            "
            INSERT INTO account_token
            (
              account_id,
              encoded,
              expiration
            )
            VALUES (?,?,?)
            ON CONFLICT(account_id) DO UPDATE SET
              encoded = excluded.encoded,
              expiration = excluded.expiration;
            ",
        )
        .bind(id.0)
        .bind(encoded.to_string())
        .bind(expiration)
        .execute(tx.as_mut())
        .await?;

        Ok(())
    }

    /// Get the account token for [`Id`] from the provided [`Database`]
    pub async fn get<'a, T>(conn: &'a mut T, id: Id) -> Result<Token, Error>
    where
        &'a mut T: database::Executor<'a>,
    {
        let token: Token = sqlx::query_as(
            "
            SELECT
              encoded,
              expiration
            FROM account_token
            WHERE account_id = ?;
            ",
        )
        .bind(id.0)
        .fetch_one(conn)
        .await?;

        Ok(token)
    }
}

/// Admin account details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Admin {
    /// Admin username
    pub username: String,
    /// Admin name
    pub name: String,
    /// Admin email
    pub email: String,
    /// Admin public key
    pub public_key: EncodedPublicKey,
}

/// Ensure only the provided admin account exists in the db.
#[tracing::instrument(
    skip_all,
    fields(
        username = %admin.username,
        public_key = %admin.public_key
    )
)]
pub(crate) async fn sync_admin(db: &Database, admin: Admin) -> Result<(), Error> {
    let mut tx = db.begin().await?;

    let account: Option<Uuid> = sqlx::query_scalar(
        "
        SELECT
          account_id
        FROM account
        WHERE
          type = 'admin'
          AND username = ?
          AND name = ?
          AND email = ?
          AND public_key = ?;
        ",
    )
    .bind(&admin.username)
    .bind(&admin.name)
    .bind(&admin.email)
    .bind(admin.public_key.to_string())
    .fetch_optional(tx.as_mut())
    .await?;

    if account.is_some() {
        return Ok(());
    }

    sqlx::query(
        "
        DELETE FROM account
        WHERE type = 'admin';
        ",
    )
    .execute(tx.as_mut())
    .await?;

    Account {
        id: Id::generate(),
        kind: Kind::Admin,
        username: admin.username.clone(),
        name: Some(admin.name.clone()),
        email: Some(admin.email.clone()),
        public_key: admin.public_key.clone(),
    }
    .save(&mut tx)
    .await?;

    tx.commit().await?;

    debug!("Admin account synced");

    Ok(())
}

/// An account error
#[derive(Debug, Error)]
pub enum Error {
    /// Database error occurred
    #[error("database")]
    Database(#[from] database::Error),
}

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Self {
        Error::Database(error.into())
    }
}
