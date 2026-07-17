//! `send <peer> <path>`: make a file available to a configured peer and wait
//! for them to pull it.
//!
//! We use iroh-blobs' pull path (the well-tested one): this side hashes the file
//! and serves it as a provider, restricted to the target peer, then announces it
//! over the control channel. The receiver downloads it and, only once it has the
//! whole file, signals us so we can stop serving. This is what makes large
//! transfers reliable — we keep serving until the data has actually landed.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use indicatif::{ProgressBar, ProgressStyle};
use iroh::{
    Endpoint, EndpointId,
    endpoint::presets,
    protocol::Router,
};
use iroh_blobs::{
    BlobsProtocol,
    provider::events::{
        AbortReason, ConnectMode, EventMask, EventSender, ProviderMessage, RequestMode,
        RequestUpdate,
    },
    store::mem::MemStore,
};

use crate::{
    config::Instance,
    proto::{self, CTRL_ALPN, Header, OK},
};

pub async fn run(instance: Instance, peer_alias: String, path: PathBuf) -> Result<()> {
    let abs = std::path::absolute(&path)
        .with_context(|| format!("resolving path {}", path.display()))?;
    if !abs.is_file() {
        bail!("{} is not a file", abs.display());
    }
    let name = abs
        .file_name()
        .context("path has no file name")?
        .to_string_lossy()
        .into_owned();
    let size = std::fs::metadata(&abs)?.len();

    let peer_id = instance.peer_id(&peer_alias)?;
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(instance.secret_key()?)
        .bind()
        .await?;
    // Wait until discovery is ready so the peer can dial us back to pull the data.
    endpoint.online().await;

    // Hash the file into an in-memory store; this gives us the blob's hash.
    let store = MemStore::new();
    tracing::info!("hashing {name}");
    let tag = store.blobs().add_path(&abs).await?;

    // Serve the blob, but only to the intended receiver.
    let blobs = BlobsProtocol::new(&store, Some(serve_events(peer_id)));
    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, blobs)
        .spawn();

    // Announce over the control channel. If the peer doesn't have us in their
    // allowlist, this is where it fails.
    let ctrl = endpoint
        .connect(peer_id, CTRL_ALPN)
        .await
        .with_context(|| format!("connecting to peer '{peer_alias}'"))?;
    let (mut send, mut recv) = ctrl.open_bi().await?;
    proto::write_frame(&mut send, &Header { name: name.clone(), size, hash: tag.hash }).await?;

    // First byte: the peer accepted (allowlist passed) and is downloading. The
    // progress bar is rendered from provider transfer events (see serve_events).
    if proto::read_byte(&mut recv).await.context("peer rejected the transfer")? != OK {
        bail!("peer rejected the transfer");
    }

    // Second byte: the peer has received and saved the whole file, so we can
    // stop serving.
    if proto::read_byte(&mut recv).await.context("waiting for the peer to finish")? != OK {
        bail!("peer failed to save the file");
    }

    tracing::info!("sent {name} ({size} bytes) to {peer_alias}");
    router.shutdown().await?;
    endpoint.close().await;
    Ok(())
}

/// Serve blobs to the one peer we're sending to (the hash is only shared with
/// them over the control channel, but this gates connections regardless), and
/// render a progress bar from the transfer's provider events.
fn serve_events(allowed: EndpointId) -> EventSender {
    let mask = EventMask {
        connected: ConnectMode::Intercept,
        get: RequestMode::InterceptLog, // gives us per-transfer progress events
        ..EventMask::DEFAULT
    };
    let (tx, mut rx) = EventSender::channel(32, mask);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                ProviderMessage::ClientConnected(msg) => {
                    let res = if msg.endpoint_id == Some(allowed) {
                        Ok(())
                    } else {
                        Err(AbortReason::Permission)
                    };
                    msg.tx.send(res).await.ok();
                }
                ProviderMessage::GetRequestReceived(msg) => {
                    msg.tx.send(Ok(())).await.ok(); // accept
                    let mut updates = msg.rx;
                    tokio::spawn(async move {
                        let mut pb: Option<ProgressBar> = None;
                        while let Ok(Some(update)) = updates.recv().await {
                            match update {
                                RequestUpdate::Started(s) => pb = Some(progress_bar(s.size)),
                                RequestUpdate::Progress(p) => {
                                    if let Some(pb) = &pb {
                                        pb.set_position(p.end_offset);
                                    }
                                }
                                RequestUpdate::Completed(_) => {
                                    if let Some(pb) = pb.take() {
                                        pb.finish_and_clear();
                                    }
                                    break;
                                }
                                RequestUpdate::Aborted(_) => {
                                    if let Some(pb) = pb.take() {
                                        pb.abandon();
                                    }
                                    break;
                                }
                            }
                        }
                    });
                }
                _ => {}
            }
        }
    });
    tx
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
