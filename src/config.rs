//! Config + identity, modelled loosely on an SSH setup.
//!
//! Everything lives under one "home" directory (overridable with `SENDER_HOME`,
//! which makes it easy to run two instances on one machine for testing):
//!   - `identity.key` — 32 raw secret-key bytes (your stable address lives here)
//!   - `config.toml`  — the peer list + optional download dir
//!   - `spool/`       — the on-disk blob store the receiver writes into

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context, Result, bail};
use iroh::{EndpointId, SecretKey};
use serde::{Deserialize, Serialize};

/// A single known peer, keyed by alias in the config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    /// The peer's `EndpointId` (public key) as a string.
    pub id: String,
}

/// On-disk config file (`config.toml`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    /// Where received files are written. Defaults to the current directory.
    #[serde(default)]
    pub download_dir: Option<PathBuf>,
    /// Known peers, `alias -> Peer`. Used as an address book when sending and as
    /// an allowlist when receiving.
    #[serde(default)]
    pub peers: HashMap<String, Peer>,
}

/// Resolved paths + loaded config for a single instance.
pub struct Instance {
    pub home: PathBuf,
    pub config: Config,
}

impl Instance {
    /// Load the instance, creating the home dir and a default config if needed.
    pub fn load() -> Result<Self> {
        let home = home_dir()?;
        std::fs::create_dir_all(&home)
            .with_context(|| format!("creating home dir {}", home.display()))?;
        let config = load_config(&home.join("config.toml"))?;
        Ok(Self { home, config })
    }

    pub fn config_path(&self) -> PathBuf {
        self.home.join("config.toml")
    }

    pub fn spool_dir(&self) -> PathBuf {
        self.home.join("spool")
    }

    /// Load the persistent secret key, generating and saving one on first use.
    pub fn secret_key(&self) -> Result<SecretKey> {
        load_or_create_identity(&self.home.join("identity.key"))
    }

    /// The directory received files are written to.
    pub fn download_dir(&self) -> Result<PathBuf> {
        match &self.config.download_dir {
            Some(dir) => Ok(expand_tilde(dir)),
            None => std::env::current_dir().context("resolving current dir"),
        }
    }

    /// Look up a peer's `EndpointId` by alias.
    pub fn peer_id(&self, alias: &str) -> Result<EndpointId> {
        let peer = self
            .config
            .peers
            .get(alias)
            .with_context(|| format!("no peer named '{alias}' in {}", self.config_path().display()))?;
        EndpointId::from_str(&peer.id)
            .with_context(|| format!("invalid endpoint id for peer '{alias}'"))
    }

    /// Build the allowlist as `EndpointId -> alias`, optionally narrowed to a
    /// subset of aliases (the `--from` filter).
    pub fn allowlist(&self, only: &[String]) -> Result<HashMap<EndpointId, String>> {
        let mut map = HashMap::new();
        for (alias, peer) in &self.config.peers {
            if !only.is_empty() && !only.iter().any(|a| a == alias) {
                continue;
            }
            let id = EndpointId::from_str(&peer.id)
                .with_context(|| format!("invalid endpoint id for peer '{alias}'"))?;
            map.insert(id, alias.clone());
        }
        if !only.is_empty() {
            for a in only {
                if !self.config.peers.contains_key(a) {
                    bail!("--from '{a}' is not a configured peer");
                }
            }
        }
        Ok(map)
    }
}

fn home_dir() -> Result<PathBuf> {
    if let Ok(h) = std::env::var("SENDER_HOME") {
        return Ok(PathBuf::from(h));
    }
    // Always use an XDG-style `~/.config/sender`, including on macOS (where
    // ProjectDirs would otherwise pick `~/Library/Application Support`).
    let config_home = match std::env::var_os("XDG_CONFIG_HOME") {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => {
            let base = directories::BaseDirs::new()
                .context("could not determine your home directory; set SENDER_HOME")?;
            base.home_dir().join(".config")
        }
    };
    Ok(config_home.join("sender"))
}

fn load_config(path: &Path) -> Result<Config> {
    if !path.exists() {
        let default = Config::default();
        save_config(path, &default)?;
        return Ok(default);
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

fn save_config(path: &Path, config: &Config) -> Result<()> {
    let text = toml::to_string_pretty(config)?;
    std::fs::write(path, text).with_context(|| format!("writing {}", path.display()))
}

fn load_or_create_identity(path: &Path) -> Result<SecretKey> {
    if path.exists() {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading identity {}", path.display()))?;
        let bytes: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("identity file {} is not 32 bytes", path.display()))?;
        Ok(SecretKey::from_bytes(&bytes))
    } else {
        let key = SecretKey::generate();
        write_private(path, &key.to_bytes())
            .with_context(|| format!("writing identity {}", path.display()))?;
        Ok(key)
    }
}

/// Write a file with owner-only permissions (0o600).
fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)?;
    Ok(())
}

fn expand_tilde(path: &Path) -> PathBuf {
    if let Ok(rest) = path.strip_prefix("~")
        && let Some(dirs) = directories::UserDirs::new()
    {
        return dirs.home_dir().join(rest);
    }
    path.to_path_buf()
}
