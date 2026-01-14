//! Service database

use std::path::Path;

use sqlx::{Pool, Sqlite, SqliteConnection, pool::PoolConnection};
use thiserror::Error;

pub use sqlx::migrate::Migrator;

/// Service database
#[derive(Debug, Clone)]
pub struct Database {
    /// Connection pool to the underlying SQLITE database
    pool: Pool<Sqlite>,
}

impl Database {
    /// Opens a connection to the provided database path
    pub async fn new(path: impl AsRef<Path>) -> Result<Self, Error> {
        let pool = sqlx::SqlitePool::connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true)
                .read_only(false)
                .foreign_keys(true),
        )
        .await?;

        sqlx::migrate!("./migrations")
            .set_ignore_missing(true)
            .run(&pool)
            .await?;

        Ok(Self { pool })
    }

    /// Runs the provided migrations on the database
    pub async fn migrate(&self, mut migrator: Migrator) -> Result<(), Error> {
        migrator.set_ignore_missing(true).run(&self.pool).await?;
        Ok(())
    }

    /// Acquire a database connection
    pub async fn acquire(&self) -> Result<PoolConnection<Sqlite>, Error> {
        Ok(self.pool.acquire().await?)
    }

    /// Begin a database transaction
    pub async fn begin(&self) -> Result<Transaction, Error> {
        Ok(Transaction(self.pool.begin_with("BEGIN IMMEDIATE").await?))
    }

    /// Creates a new in-memory database for testing
    pub async fn in_memory() -> Result<Self, Error> {
        let pool = sqlx::SqlitePool::connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename("file:test")
                .shared_cache(true)
                .in_memory(true)
                .read_only(false)
                .foreign_keys(true),
        )
        .await?;

        sqlx::migrate!("./migrations")
            .set_ignore_missing(true)
            .run(&pool)
            .await?;

        Ok(Self { pool })
    }
}

/// A database transaction
pub struct Transaction(sqlx::Transaction<'static, Sqlite>);

impl Transaction {
    /// Commit the transaction
    pub async fn commit(self) -> Result<(), Error> {
        Ok(self.0.commit().await?)
    }
}

impl AsMut<SqliteConnection> for Transaction {
    fn as_mut(&mut self) -> &mut SqliteConnection {
        self.0.as_mut()
    }
}

/// Provides a database connection for executing queries
pub trait Executor<'a>: sqlx::Executor<'a, Database = Sqlite> {}

impl<'a, T> Executor<'a> for &'a mut T where &'a mut T: sqlx::Executor<'a, Database = Sqlite> {}

/// A database error
#[derive(Debug, Error)]
pub enum Error {
    /// Sqlx error
    #[error("sqlx")]
    Sqlx(#[from] sqlx::Error),
    /// Migration error
    #[error("sqlx migrate")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}
