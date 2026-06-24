#![warn(missing_docs)]
//! Shared service code for AerynOS infrastructure

pub use service_client as client;
pub use service_core::{Service, Token, auth, crypto, token};

pub use self::account::Account;
pub use self::database::Database;
pub use self::server::Server;
pub use self::state::State;

mod middleware;
mod task;

pub mod account;
pub mod buildinfo;
pub mod database;
pub mod error;
pub mod git;
pub mod grpc;
pub mod process;
pub mod request;
pub mod server;
pub mod signal;
pub mod state;
pub mod tracing;
