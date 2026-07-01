//! Primitive service types

#![warn(missing_docs)]

pub use self::service::Service;
pub use self::token::Token;

mod service;

pub mod account;
pub mod auth;
pub mod crypto;
pub mod token;
