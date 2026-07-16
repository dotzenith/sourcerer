//! `send <peer> <path>`: push a single file to a configured peer.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use futures_lite::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use iroh::{Endpoint, endpoint::presets};
use iroh_blobs::{
    api::remote::PushProgressItem,
    protocol::{GetRequest, PushRequest},
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
    // Wait until discovery is ready so dialing the peer by id resolves reliably.
    endpoint.online().await;

    // Hash the file into an in-memory store; this gives us the blob's hash.
    let store = MemStore::new();
    println!("hashing {name}...");
    let tag = store.blobs().add_path(&abs).await?;

    // Open the control channel and announce what we're about to push. If the
    // peer doesn't have us in their allowlist, this is where it fails.
    let ctrl = endpoint
        .connect(peer_id, CTRL_ALPN)
        .await
        .with_context(|| format!("connecting to peer '{peer_alias}'"))?;
    let (mut send, mut recv) = ctrl.open_bi().await?;
    let header = Header { name: name.clone(), size, hash: tag.hash };
    proto::write_frame(&mut send, &header).await?;
    if proto::read_byte(&mut recv).await.context("peer rejected the transfer")? != OK {
        bail!("peer rejected the transfer");
    }

    // Push the blob over the standard blobs protocol, driving a progress bar.
    let blob_conn = endpoint.connect(peer_id, iroh_blobs::ALPN).await?;
    let request = PushRequest::from(GetRequest::blob(tag.hash));
    let pb = progress_bar(size);
    let mut stream = store.remote().execute_push(blob_conn, request).stream();
    while let Some(item) = stream.next().await {
        match item {
            PushProgressItem::Progress(n) => pb.set_position(n),
            PushProgressItem::Done(_) => pb.finish_and_clear(),
            PushProgressItem::Error(e) => {
                pb.abandon();
                return Err(anyhow::anyhow!("push failed: {e}"));
            }
        }
    }

    // Tell the receiver the blob is fully transferred so it can export, then
    // wait for its confirmation.
    proto::write_byte(&mut send, OK).await?;
    send.finish().ok();
    if proto::read_byte(&mut recv).await.context("waiting for confirmation")? != OK {
        bail!("peer failed to save the file");
    }

    println!("sent {name} ({size} bytes) to {peer_alias}");
    endpoint.close().await;
    Ok(())
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
