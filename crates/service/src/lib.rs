#![warn(missing_docs)]
//! Shared service code for Serpent OS infrastructure

pub use service_core::{Token, auth, crypto, token};

pub use self::account::Account;
pub use self::database::Database;
pub use self::endpoint::Endpoint;
pub use self::server::Server;
pub use self::state::State;

mod middleware;
mod task;

pub mod account;
pub mod client;
pub mod database;
pub mod endpoint;
pub mod error;
pub mod git;
pub mod grpc;
pub mod process;
pub mod request;
pub mod server;
pub mod signal;
pub mod state;
pub mod tracing;
