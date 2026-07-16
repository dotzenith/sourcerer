//! The tiny control protocol that rides alongside the blobs transfer.
//!
//! blobs is content-addressed, so a push only carries a hash. This side channel
//! carries the human-facing metadata (filename, size) and doubles as the consent
//! gate: the receiver checks the connecting peer's id before reading anything.

use anyhow::Result;
use iroh::endpoint::{RecvStream, SendStream};
use iroh_blobs::Hash;
use serde::{Serialize, de::DeserializeOwned};

/// ALPN for our control protocol. Bump the suffix on any wire-breaking change.
pub const CTRL_ALPN: &[u8] = b"sender/ctrl/1";

/// Announced by the sender before the blob is pushed.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct Header {
    /// The file name to write on the receiving side (base name only).
    pub name: String,
    /// Total size in bytes, used to render the progress bar.
    pub size: u64,
    /// BLAKE3 hash of the file; the receiver exports this hash once it lands.
    pub hash: Hash,
}

/// A one-byte signal. We only ever need a single "go ahead" token in each
/// direction, so a length-prefixed frame would be overkill here.
pub const OK: u8 = 1;

/// Write a length-prefixed postcard frame.
pub async fn write_frame<T: Serialize>(send: &mut SendStream, msg: &T) -> Result<()> {
    let bytes = postcard::to_allocvec(msg)?;
    let len = u32::try_from(bytes.len())?;
    send.write_all(&len.to_le_bytes()).await?;
    send.write_all(&bytes).await?;
    Ok(())
}

/// Read a length-prefixed postcard frame.
pub async fn read_frame<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<T> {
    let mut len = [0u8; 4];
    recv.read_exact(&mut len).await?;
    let len = u32::from_le_bytes(len) as usize;
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await?;
    Ok(postcard::from_bytes(&buf)?)
}

/// Write a single signal byte.
pub async fn write_byte(send: &mut SendStream, b: u8) -> Result<()> {
    send.write_all(&[b]).await?;
    Ok(())
}

/// Read a single signal byte.
pub async fn read_byte(recv: &mut RecvStream) -> Result<u8> {
    let mut b = [0u8; 1];
    recv.read_exact(&mut b).await?;
    Ok(b[0])
}
