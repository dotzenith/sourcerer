//! `receive [--from <alias>...]`: listen for transfers from allowlisted peers.
//!
//! A sender connects to us on the control channel and announces a file. We check
//! their endpoint id against the allowlist, then pull the blob from them with
//! iroh-blobs' downloader (which only completes once the whole file has arrived
//! and been verified) and export it to the download directory.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Result, bail};
use futures_lite::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use iroh::{
    Endpoint, EndpointId,
    endpoint::{Connection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_blobs::{api::downloader::DownloadProgressItem, store::fs::FsStore};

use crate::{
    config::Instance,
    proto::{self, CTRL_ALPN, Header, OK},
};

pub async fn run(instance: Instance, only: Vec<String>) -> Result<()> {
    let allow = Arc::new(instance.allowlist(&only)?);
    if allow.is_empty() {
        bail!(
            "no peers configured to receive from; add some to {}",
            instance.config_path().display()
        );
    }
    let download_dir = Arc::new(instance.download_dir()?);
    std::fs::create_dir_all(download_dir.as_ref())?;

    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(instance.secret_key()?)
        .bind()
        .await?;
    endpoint.online().await;

    // On-disk store that pulled blobs land in before we export them.
    let store = FsStore::load(instance.spool_dir()).await?;

    let ctrl = CtrlHandler {
        allow: allow.clone(),
        store,
        download_dir: download_dir.clone(),
        endpoint: endpoint.clone(),
    };
    let router = Router::builder(endpoint.clone())
        .accept(CTRL_ALPN, ctrl)
        .spawn();

    let peers = allow.values().cloned().collect::<Vec<_>>().join(", ");
    tracing::info!("listening as {}", endpoint.id());
    tracing::info!("saving to {}", download_dir.display());
    tracing::info!("accepting from: {peers}");
    tracing::info!("ready (ctrl-c to stop)");

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    router.shutdown().await?;
    Ok(())
}

#[derive(Debug, Clone)]
struct CtrlHandler {
    allow: Arc<HashMap<EndpointId, String>>,
    store: FsStore,
    download_dir: Arc<PathBuf>,
    endpoint: Endpoint,
}

impl ProtocolHandler for CtrlHandler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        self.handle(conn).await.map_err(|e| {
            tracing::warn!("transfer failed: {e:#}");
            AcceptError::from_err(std::io::Error::other(e.to_string()))
        })
    }
}

impl CtrlHandler {
    async fn handle(&self, conn: Connection) -> Result<()> {
        // The consent gate: only allowlisted peers get past this point.
        let remote = conn.remote_id();
        let Some(alias) = self.allow.get(&remote).cloned() else {
            conn.close(0u32.into(), b"not authorized");
            tracing::warn!("rejected connection from unknown peer {remote}");
            return Ok(());
        };

        let (mut send, mut recv) = conn.accept_bi().await?;
        let header: Header = proto::read_frame(&mut recv).await?;
        tracing::info!("incoming: {} ({} bytes) from {alias}", header.name, header.size);
        // Ack -> the sender keeps serving while we pull the blob from them.
        proto::write_byte(&mut send, OK).await?;

        // Pull the blob. The downloader connects back to the sender and only
        // finishes once the whole, verified file is in our store.
        let downloader = self.store.downloader(&self.endpoint);
        let pb = progress_bar(header.size);
        let mut stream = downloader.download(header.hash, Some(remote)).stream().await?;
        while let Some(item) = stream.next().await {
            match item {
                DownloadProgressItem::Progress(n) => pb.set_position(n),
                DownloadProgressItem::Error(e) => {
                    pb.abandon();
                    bail!("download failed: {e}");
                }
                DownloadProgressItem::DownloadError => {
                    pb.abandon();
                    bail!("download failed");
                }
                _ => {}
            }
        }
        pb.finish_and_clear();

        let dest = unique_path(&self.download_dir, &header.name);
        self.store.blobs().export(header.hash, &dest).await?;
        // Confirm to the sender that the file is safely written.
        proto::write_byte(&mut send, OK).await?;
        send.finish().ok();

        tracing::info!("received {} -> {}", header.name, dest.display());
        conn.closed().await;
        Ok(())
    }
}

fn progress_bar(size: u64) -> ProgressBar {
    let pb = ProgressBar::new(size);
    pb.set_style(
        ProgressStyle::with_template(
            "{bar:40.cyan/blue} {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
        )
        .unwrap()
        .progress_chars("=>-"),
    );
    pb
}

/// Avoid clobbering an existing file: `name.ext` -> `name (1).ext`, etc.
fn unique_path(dir: &std::path::Path, name: &str) -> PathBuf {
    let candidate = dir.join(name);
    if !candidate.exists() {
        return candidate;
    }
    let path = std::path::Path::new(name);
    let stem = path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
    let ext = path.extension().map(|s| s.to_string_lossy().into_owned());
    for n in 1.. {
        let fname = match &ext {
            Some(ext) => format!("{stem} ({n}).{ext}"),
            None => format!("{stem} ({n})"),
        };
        let candidate = dir.join(fname);
        if !candidate.exists() {
            return candidate;
        }
    }
    unreachable!()
}
