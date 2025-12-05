use std::{path::PathBuf, process};

use clap::{Parser, Subcommand, ValueEnum};
use color_eyre::eyre::Result;
use service_client::{AuthClient, CredentialsAuth, SummitServiceClient, VesselServiceClient};
use service_core::crypto::KeyPair;
use service_grpc::{
    summit::{CancelRequest, RetryRequest},
    vessel::{AddTagRequest, RemoveTagRequest, Stream as ProtoStream, UpdateStreamRequest},
};
use tokio::{fs, io};
use tonic::transport::Uri;

#[tokio::main]
async fn main() -> Result<()> {
    let Args { command } = Args::parse();

    match command {
        Command::Summit {
            uri,
            username,
            private_key,
            command,
        } => {
            let key_pair = KeyPair::load(private_key)?;

            println!("Using key_pair {}", key_pair.public_key().encode());

            let mut client =
                SummitServiceClient::connect_with_auth(uri, CredentialsAuth::new(username, key_pair)).await?;

            match command {
                Summit::Retry { task } => {
                    client.retry(RetryRequest { task_id: task }).await?;
                }
                Summit::Cancel { task } => {
                    client.cancel(CancelRequest { task_id: task }).await?;
                }
                Summit::Refresh {} => {
                    client.refresh(()).await?;
                }
                Summit::Pause {} => {
                    client.pause(()).await?;
                }
                Summit::Resume {} => {
                    client.resume(()).await?;
                }
            }
        }
        Command::Vessel {
            uri,
            username,
            private_key,
            command,
        } => {
            let key_pair = KeyPair::load(private_key)?;

            println!("Using key_pair {}", key_pair.public_key().encode());

            let mut client =
                VesselServiceClient::connect_with_auth(uri, CredentialsAuth::new(username, key_pair)).await?;

            let response = match command {
                Vessel::UpdateStream {
                    channel,
                    stream,
                    version,
                } => {
                    client
                        .update_stream(UpdateStreamRequest {
                            channel,
                            stream: match stream {
                                ChannelStream::Volatile => ProtoStream::Volatile,
                                ChannelStream::Unstable => ProtoStream::Unstable,
                            } as i32,
                            version,
                        })
                        .await?
                }
                Vessel::AddTag { channel, tag, history } => {
                    client.add_tag(AddTagRequest { channel, tag, history }).await?
                }
                Vessel::RemoveTag { channel, tag } => client.remove_tag(RemoveTagRequest { channel, tag }).await?,
            }
            .into_inner();

            if !response.success
                && let Some(error) = response.error
            {
                eprintln!("Command failed: {error}");
                process::exit(1);
            }
        }
        Command::Key { command } => match command {
            Key::Generate { format, output } => {
                let key = KeyPair::generate();

                let bytes = match format {
                    KeyFormat::Bytes => key.to_bytes().to_vec(),
                    KeyFormat::Pem => key.pem()?.as_bytes().to_vec(),
                    KeyFormat::Der => key.der()?.as_bytes().to_vec(),
                };

                if let Some(output) = output {
                    fs::write(output, bytes).await?;
                } else {
                    io::copy(&mut bytes.as_slice(), &mut io::stdout()).await?;
                }
            }
            Key::Encode { format, key } => {
                let bytes = if let Some(path) = key {
                    fs::read(path).await?
                } else {
                    let mut bytes = vec![];

                    io::copy(&mut io::stdin(), &mut bytes).await?;

                    bytes
                };

                let key = match format {
                    KeyFormat::Bytes => KeyPair::try_from_bytes(&bytes)?,
                    KeyFormat::Pem => KeyPair::try_from_pem(&String::from_utf8(bytes)?)?,
                    KeyFormat::Der => KeyPair::try_from_der(&bytes)?,
                };

                println!("{}", key.public_key().encode());
            }
        },
    }

    Ok(())
}

#[derive(Debug, Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Summit commands
    Summit {
        /// Uri to connect to
        #[arg(long = "uri", default_value = "http://127.0.0.1:5001")]
        uri: Uri,
        /// Admin username
        #[arg(long = "user")]
        username: String,
        /// Path to admin private key
        #[arg(long = "key")]
        private_key: PathBuf,
        #[command(subcommand)]
        command: Summit,
    },
    /// Vessel commands
    Vessel {
        /// Uri to connect to
        #[arg(long = "uri", default_value = "http://127.0.0.1:5002")]
        uri: Uri,
        /// Admin username
        #[arg(long = "user")]
        username: String,
        /// Path to admin private key
        #[arg(long = "key")]
        private_key: PathBuf,
        #[command(subcommand)]
        command: Vessel,
    },
    /// Work with ed25519 keys
    Key {
        #[command(subcommand)]
        command: Key,
    },
}

#[derive(Debug, Subcommand)]
enum Summit {
    /// Retry a failed task
    Retry {
        /// Task id
        task: u64,
    },
    /// Cancel an in progress task
    Cancel {
        /// Task id
        task: u64,
    },
    /// Refresh all projects
    Refresh {},
    /// Pause all projects
    Pause {},
    /// Resume all projects
    Resume {},
}

#[derive(Debug, Subcommand)]
enum Vessel {
    /// Update a stream to link to a new version
    UpdateStream {
        /// Channel name
        channel: String,
        /// Stream name
        stream: ChannelStream,
        /// Version slug
        ///
        /// Examples: ["history/<identifier>", "tag/<identifier>"]
        version: String,
    },
    /// Add a tag linked to history
    AddTag {
        /// Channel name
        channel: String,
        /// Tag identifier
        tag: String,
        /// History identifier
        history: String,
    },
    /// Remove a tag
    RemoveTag {
        /// Channel name
        channel: String,
        /// Tag identifier
        tag: String,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ChannelStream {
    Volatile,
    Unstable,
}

#[derive(Debug, Subcommand)]
enum Key {
    /// Generate a private key
    Generate {
        /// Output format
        #[arg(short, long, default_value = "pem")]
        format: KeyFormat,
        /// Output destination or stdout if omitted
        output: Option<PathBuf>,
    },
    /// Print a keys url safe base64 public key
    Encode {
        /// Key format
        #[arg(short, long, default_value = "pem")]
        format: KeyFormat,
        /// Path to the private key or stdin if omitted
        key: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum, Default)]
enum KeyFormat {
    Bytes,
    #[default]
    Pem,
    Der,
}
