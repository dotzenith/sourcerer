//! sourcerer — send and receive files peer-to-peer over iroh, addressed by key.

mod config;
mod proto;
mod receive;
mod send;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{EnvFilter, fmt};

use crate::config::Instance;

/// Basic leveled, timestamped log output on stderr. Our own messages log at
/// `info`; dependencies stay quiet unless they hit an `error` (or `RUST_LOG`
/// asks for more, e.g. `RUST_LOG=iroh=debug`).
fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("error,sr=info"));
    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}

#[derive(Debug, Parser)]
#[command(name = "sr", version, about = "Send files peer-to-peer over iroh")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Generate an identity if needed and print your endpoint id.
    Init,
    /// Print your endpoint id (the address you share with peers).
    Id,
    /// Listen for incoming files from allowlisted peers.
    Receive {
        /// Only accept from these peer aliases (default: all configured peers).
        #[arg(long)]
        from: Vec<String>,
    },
    /// Send a file to a configured peer.
    Send {
        /// The peer alias to send to.
        peer: String,
        /// The file to send.
        path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let cli = Cli::parse();
    let instance = Instance::load()?;

    match cli.command {
        Command::Init => {
            let key = instance.secret_key()?;
            tracing::info!("home: {}", instance.home.display());
            tracing::info!("config: {}", instance.config_path().display());
            tracing::info!("endpoint id: {}", key.public());
            tracing::info!("share that endpoint id with peers so they can add you under [peers]");
        }
        // Raw stdout: this is machine-readable output meant to be captured, e.g.
        // `id = "$(sr id)"`, so it deliberately stays out of the log stream.
        Command::Id => {
            println!("{}", instance.secret_key()?.public());
        }
        Command::Receive { from } => {
            receive::run(instance, from).await?;
        }
        Command::Send { peer, path } => {
            send::run(instance, peer, path).await?;
        }
    }
    Ok(())
}
