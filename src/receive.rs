//! `receive [--from <alias>...]`: listen for pushes from allowlisted peers.

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Result, bail};
use iroh::{
    Endpoint, EndpointId,
    endpoint::{Connection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_blobs::{
    BlobsProtocol,
    provider::events::{
        AbortReason, ConnectMode, EventMask, EventSender, ProviderMessage, RequestMode,
    },
    store::fs::FsStore,
};

use crate::{
    config::Instance,
    proto::{self, CTRL_ALPN, Header, OK},
};

pub async fn run(instance: Instance, only: Vec<String>) -> Result<()> {
    let allow = Arc::new(instance.allowlist(&only)?);
    if allow.is_empty() {
        bail!("no peers configured to receive from; add some to {}", instance.config_path().display());
    }
    let download_dir = Arc::new(instance.download_dir()?);
    std::fs::create_dir_all(download_dir.as_ref())?;

    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(instance.secret_key()?)
        .bind()
        .await?;
    endpoint.online().await;

    // On-disk store the pushes land in before we export them.
    let store = FsStore::load(instance.spool_dir()).await?;

    // Gate blobs connections by endpoint id, and allow pushes (disabled by default).
    let allowed_ids: HashSet<EndpointId> = allow.keys().copied().collect();
    let events = connection_gate(allowed_ids);
    let blobs = BlobsProtocol::new(&store, Some(events));

    let ctrl = CtrlHandler {
        allow: allow.clone(),
        store: store.clone(),
        download_dir: download_dir.clone(),
    };

    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, blobs)
        .accept(CTRL_ALPN, ctrl)
        .spawn();

    println!("listening as {}", endpoint.id());
    println!("saving to {}", download_dir.display());
    println!("accepting from:");
    for alias in allow.values() {
        println!("  - {alias}");
    }
    println!("(ctrl-c to stop)");

    tokio::signal::ctrl_c().await?;
    println!("\nshutting down");
    router.shutdown().await?;
    Ok(())
}

/// Reject any blobs connection whose peer isn't in the allowlist. Pushes are
/// enabled here (they're disabled in the default mask because they write to the
/// local store); the connection gate is what keeps that safe.
fn connection_gate(allowed: HashSet<EndpointId>) -> EventSender {
    let mask = EventMask {
        connected: ConnectMode::Intercept,
        push: RequestMode::None, // None = process normally (accept), vs the default Disabled
        ..EventMask::DEFAULT
    };
    let (tx, mut rx) = EventSender::channel(32, mask);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let ProviderMessage::ClientConnected(msg) = msg {
                let permitted = matches!(msg.endpoint_id, Some(id) if allowed.contains(&id));
                let res = if permitted { Ok(()) } else { Err(AbortReason::Permission) };
                msg.tx.send(res).await.ok();
            }
        }
    });
    tx
}

#[derive(Debug, Clone)]
struct CtrlHandler {
    allow: Arc<HashMap<EndpointId, String>>,
    store: FsStore,
    download_dir: Arc<PathBuf>,
}

impl ProtocolHandler for CtrlHandler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        self.handle(conn).await.map_err(|e| {
            eprintln!("transfer failed: {e:#}");
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
            println!("rejected connection from unknown peer {remote}");
            return Ok(());
        };

        let (mut send, mut recv) = conn.accept_bi().await?;
        let header: Header = proto::read_frame(&mut recv).await?;
        println!("incoming: {} ({} bytes) from {alias}", header.name, header.size);
        proto::write_byte(&mut send, OK).await?;

        // Block until the sender signals the push is done...
        if proto::read_byte(&mut recv).await? != OK {
            bail!("sender aborted");
        }
        // ...then wait for our own store to actually have the complete blob
        // (the push lands on a separate connection and may still be flushing).
        tokio::time::timeout(
            std::time::Duration::from_secs(60),
            self.store.blobs().observe(header.hash).await_completion(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for blob to complete"))??;

        let dest = unique_path(&self.download_dir, &header.name);
        self.store.blobs().export(header.hash, &dest).await?;
        proto::write_byte(&mut send, OK).await?;
        send.finish().ok();

        println!("received {} -> {}", header.name, dest.display());
        conn.closed().await;
        Ok(())
    }
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
