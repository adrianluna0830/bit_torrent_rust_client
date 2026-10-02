# BitTorrent Client in Rust

A minimal command-line BitTorrent client built to learn Rust, asynchronous networking, and the BitTorrent protocol. It downloads single-file torrents from `.torrent` files.

## Features

- Discover IPv4 peers through HTTP/HTTPS trackers, UDP trackers, and DHT.
- Coordinate up to eight active peers with separate peer workers and up to sixteen simultaneous connection attempts. Successful connections wait for a worker slot when the active limit is reached.
- Download blocks with at most eight pending requests per peer.
- Retry pending block requests after a peer unchokes, preserving blocks already received.
- Verify each completed piece against its SHA-1 hash.
- Resume downloads by checking the pieces already stored on disk.
- Rename the incomplete file after every piece has been verified.
- Upload verified pieces to connected peers.
- Configure logging with `log`, `env_logger`, and `RUST_LOG`.

## Requirements

- Rust and Cargo with support for Rust edition 2024.
- An internet connection.
- A single-file `.torrent` and an existing download directory.

## Run

From the project directory:

```bash
cargo run
```

The client asks for:

1. The path to the `.torrent` file.
2. The directory where the downloaded file should be saved.

For an optimized build:

```bash
cargo run --release
```

Peer discovery happens before downloading starts and can take some time when trackers or DHT nodes do not respond.

## Resume and seed

While downloading, the file has a leading dot, such as `.example.iso`. Once all pieces have been verified, it is renamed to `example.iso`.

To resume, run the client again with the same torrent and directory. It verifies the existing file and downloads the missing pieces. A partially downloaded piece is downloaded again.

If the completed file already exists, the client verifies it and can upload its pieces to connected peers. Workers can also continue uploading after a download completes.

The client rejects files whose size differs from the torrent and reports an error if both the incomplete and completed filenames exist in the download directory.

## Logging

By default, the client shows `info`, `warn`, and `error` logs. Other libraries show warnings and errors.

The following commands use Bash syntax.

Show peer messages sent and received:

```bash
RUST_LOG=warn,bit_torrent_rust_client=debug cargo run
```

Show additional details, including saved blocks:

```bash
RUST_LOG=warn,bit_torrent_rust_client=trace cargo run
```

Show only warnings and errors:

```bash
RUST_LOG=warn cargo run
```

Disable logs while keeping the input prompts:

```bash
RUST_LOG=off cargo run
```

| Level | Client output |
| --- | --- |
| `error` | Errors that prevent an operation from completing |
| `warn` | Connection failures, timeouts, and invalid requests |
| `info` | Connections, completed pieces, and download progress |
| `debug` | Peer messages, assignments, and discovery details |
| `trace` | Saved blocks and individual local piece checks |

Each level also includes the more severe levels. For example, `debug` includes `info`, `warn`, and `error`.

Logs are written to standard error in English, limited to one line and 300 message characters. Peer messages show their type, piece index, offset, or payload size instead of raw byte arrays. Progress is reported approximately once per second during peer coordination.

Logging levels also work with `cargo run --release`. The build mode and logging level are independent.

## Current scope

- Only single-file `.torrent` downloads are supported.
- Connections are outgoing only; incoming peer connections are not accepted.
- Peer discovery runs once at startup. The client does not refresh the peer list when it runs out of peers.
- Downloads depend on the connected peers having the missing pieces and allowing requests. If no peers remain before completion, the client exits with an error; the incomplete file can be used on the next run.

## Source layout

| File | Responsibility |
| --- | --- |
| `src/main.rs` | Startup, local verification, and coordination loop |
| `src/cli.rs` | Torrent and download directory input |
| `src/torrent.rs` | Torrent metadata decoding |
| `src/torrent_info.rs` | Extracting the original info dictionary |
| `src/tracker.rs` | HTTP/HTTPS and UDP tracker requests |
| `src/dht.rs` | Peer discovery through DHT |
| `src/peer.rs` | Connections, handshake, and piece assignments |
| `src/peer_message.rs` | Peer message encoding and nonblocking reading |
| `src/peer_worker.rs` | Downloading and uploading with an individual peer |
| `src/peer_orchestrator.rs` | Worker events, piece assignment, and peer removal |
| `src/logging.rs` | Logging initialization and output format |
