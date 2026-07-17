# Sourcerer

Send and receive files peer-to-peer over the internet, addressed by a cryptographic key instead of an IP. Built on [iroh](https://github.com/n0-computer/iroh) and [iroh-blobs](https://github.com/n0-computer/iroh-blobs), so you get QUIC, NAT traversal, and BLAKE3-verified transfers without port forwarding or knowing anyone's IP.

The trust model is SSH-config-like and mutual: every machine has a persistent identity, and its public key — the **endpoint id** — is the address you share. A config file lists known peers by alias, and that list works both ways: an address book when sending, an allowlist when receiving. A receiver only accepts files from peers it has listed, and a sender only dials peers it has listed.

The binary is `sr`.

## Build

#### Shell
```sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/dotzenith/sourcerer/releases/latest/download/sourcerer-installer.sh | sh
```

#### Powershell
```sh
powershell -ExecutionPolicy ByPass -c "irm https://github.com/dotzenith/sourcerer/releases/latest/download/sourcerer-installer.ps1 | iex"
```

#### Binaries
Pre-Compiled binaries for linux, mac, and windows are available in [Releases](https://github.com/dotzenith/sourcerer/releases)

#### Source
```sh
git clone https://github.com/dotzenith/sourcerer
cd sourcerer
cargo build --release
./target/release/sr
```

## Usage

```
Send files peer-to-peer over iroh

Usage: sr <COMMAND>

Commands:
  init     Generate an identity if needed and print your endpoint id
  id       Print your endpoint id (the address you share with peers)
  receive  Listen for incoming files from allowlisted peers
  send     Send a file to a configured peer
  help     Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

### Examples

#### Set up your identity (once per machine)
```sh
sr init
# prints your endpoint id — share it with the peers you want to talk to
```

#### Receive
```sh
sr receive                 # wait for files from any configured peer
sr receive --from laptop   # only accept from specific peers this session
```

#### Send
```sh
sr send laptop ./report.pdf
```

Both sides show a live progress bar. The receiver auto-accepts files from allowlisted peers and rejects everyone else.

## Configuration

`sr` reads a config file at `~/.config/sourcerer/config.toml` (override the whole home with `SR_HOME`). After `sr init`, add the endpoint ids your peers shared with you.

### Example `config.toml`

```toml
# where received files land; defaults to the current directory
download_dir = "~/Downloads"

[peers.laptop]
id = "the-endpoint-id-your-peer-shared"

[peers.phone]
id = "another-endpoint-id"
```

The same `[peers]` table is used in both directions — add anyone you want to send to or receive from.

## Logging

Status messages are basic timestamped logs on stderr. By default you see `sr`'s own `info` messages; dependencies stay quiet unless they error. Set `RUST_LOG` for more:

```sh
RUST_LOG=iroh=debug sr receive
```

## License

MIT
