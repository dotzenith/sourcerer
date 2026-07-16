//! sender — send and receive files peer-to-peer over iroh, addressed by key.

mod config;
mod proto;
mod receive;
mod send;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::config::Instance;

#[derive(Debug, Parser)]
#[command(name = "sender", version, about = "Send files peer-to-peer over iroh")]
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
    let cli = Cli::parse();
    let instance = Instance::load()?;

    match cli.command {
        Command::Init => {
            let key = instance.secret_key()?;
            println!("home:        {}", instance.home.display());
            println!("config:      {}", instance.config_path().display());
            println!("endpoint id: {}", key.public());
            println!("\nShare that endpoint id with peers so they can add you under [peers].");
        }
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
