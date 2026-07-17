# sender

Send and receive files peer-to-peer over the internet, addressed by cryptographic
key instead of IP. Built on [iroh](https://github.com/n0-computer/iroh) 1.0 (QUIC +
NAT traversal + discovery) and [iroh-blobs](https://github.com/n0-computer/iroh-blobs)
for BLAKE3-verified, resumable transfers. No port forwarding, no knowing anyone's IP.

The trust model is SSH-config-like and mutual: every machine has a persistent
identity (a keypair on disk), and its public key — the **endpoint id** — is the
address you share. A config file lists known peers by alias. That list is an address
book when sending and an allowlist when receiving: a receiver only accepts files from
peers it has listed, and a sender only dials peers it has listed.

## Install

```sh
cargo build --release
# binary at target/release/sender
```

## Setup (do this once per machine)

```sh
sender init            # generates your identity and prints your endpoint id
```

Exchange endpoint ids with your peers, then edit your config (path printed by
`init`, e.g. `~/.config/sender/config.toml` or `$SENDER_HOME/config.toml`):

```toml
# optional; defaults to the current directory
download_dir = "~/Downloads"

[peers.laptop]
id = "the-endpoint-id-your-peer-shared"

[peers.phone]
id = "another-endpoint-id"
```

The same `[peers]` table is used in both directions — add anyone you want to send to
or receive from.

## Usage

On the receiving machine:

```sh
sender receive                 # wait for files from any configured peer
sender receive --from laptop   # only accept from specific peers this session
```

On the sending machine:

```sh
sender send laptop ./report.pdf
```

The receiver auto-accepts files from allowlisted peers and rejects everyone else.
The sender shows a live progress bar.

Other commands:

```sh
sender id      # print your endpoint id
```

## How it works

1. The sender hashes the file and starts serving it as an iroh-blobs provider,
   restricted to the target peer. It then opens a small **control** connection to
   the receiver announcing the file name, size, and hash. The receiver checks the
   sender's endpoint id against its allowlist here — unknown peers are dropped
   before any data moves.
2. The receiver **pulls** the blob from the sender over the standard iroh-blobs
   protocol, showing a live progress bar. The download only finishes once the
   whole file has arrived and been BLAKE3-verified, and the sender keeps serving
   until the receiver confirms — so large transfers can't be truncated.
3. The receiver exports the blob to `download_dir` under the original file name
   (with ` (1)`, ` (2)`… added if a file by that name already exists) and signals
   the sender, which then stops serving.

## Testing two instances on one machine

Set `SENDER_HOME` to give each instance its own identity, config, and store:

```sh
SENDER_HOME=/tmp/recv sender init
SENDER_HOME=/tmp/send sender init
# cross-add the printed ids into each other's config.toml, then:
SENDER_HOME=/tmp/recv sender receive &
SENDER_HOME=/tmp/send sender send <alias> ./file
```
