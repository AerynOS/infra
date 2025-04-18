use std::path::PathBuf;

use clap::{Parser, Subcommand};
use color_eyre::eyre::Result;
use service_client::{AuthClient, CredentialsAuth, SummitServiceClient};
use service_core::crypto::KeyPair;
use service_grpc::summit::RetryRequest;

#[tokio::main]
async fn main() -> Result<()> {
    let Args {
        username,
        private_key,
        command,
    } = Args::parse();

    let key_pair = KeyPair::load(private_key)?;

    println!("Using key_pair {}", key_pair.public_key().encode());

    let mut client = SummitServiceClient::connect_with_auth(
        "http://127.0.0.1:5001".parse()?,
        CredentialsAuth::new(username, key_pair),
    )
    .await?;

    match command {
        Command::Retry { task } => {
            client.retry(RetryRequest { task_id: task }).await?;
        }
    }

    Ok(())
}

#[derive(Debug, Parser)]
struct Args {
    /// Admin username
    #[arg(long = "user")]
    username: String,
    /// Path to admin private key
    #[arg(long = "key")]
    private_key: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Retry a failed task
    Retry {
        /// Task id
        task: u64,
    },
}
