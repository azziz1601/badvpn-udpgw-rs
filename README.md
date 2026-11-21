# badvpn-udpgw-rs

High-performance Rust rewrite of **`badvpn-udpgw`** (UDP-over-TCP gateway), 100% wire-protocol and CLI drop-in compatible with the original C `badvpn-udpgw` and client apps such as **HTTP Custom**, **Dark Tunnel**, **HTTP Injector**, **NapsternetV**, and **`badvpn-tun2socks`**.

## Optimizations (Low-Spec 1-vCPU & Multi-Core VPS)

- **Auto `current_thread` vs `multi_thread` runtime**: Automatically uses Tokio's zero-stealing `current_thread` runtime on 1-vCPU VPS (eliminating cross-thread scheduling overhead) and scales to `multi_thread` on multi-core servers.
- **Shared on-demand UDP receive buffers**: Idle UDP sockets hold **0 bytes** of receive buffer; a small per-client buffer pool is borrowed only when datagrams arrive (drastically reducing RAM usage under hundreds of connections).
- **Zero-allocation outbound frame pool + `write_vectored` batching**: Recycles outbound packet buffers and coalesces bursts of up to 32 frames into a single `writev` syscall.
- **UDP idle timeout auto-GC (`--udp-idle-timeout`, default 90s)**: Automatically closes stale UDP sockets and releases file descriptors/ports without waiting for LRU eviction.
- **100% Drop-in CLI & Wire Compatible**: Supports `PacketProto`, `KEEPALIVE`, `REBIND`, `DNS`, `IPV6`, `--listen-addr`, `--max-clients`, `--max-connections-for-client`, `--udp-mtu`, `--client-socket-sndbuf`, `--local-udp-addrs`, `--local-udp-ip6-addrs`, `--unique-local-ports`, `--loglevel`, `--logger`.

## Build & Test

Install Rust (if not installed yet):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
```

Run tests and compile with CPU-native optimizations on your VPS:

```bash
cd badvpn-udpgw-rs
cargo test
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

The compiled binary will be at:

```text
target/release/badvpn-udpgw
```

## CI artifacts

GitHub Actions runs `cargo test` and publishes three Linux binaries as workflow artifacts:

- `badvpn-udpgw-linux-amd64-gnu` — x86_64 glibc build, compiled on Ubuntu 22.04 (glibc 2.35). Compatible with Debian 12 x86_64 VPS using glibc 2.36 or newer.
- `badvpn-udpgw-linux-amd64` — x86_64 static musl build with no shared-library dependency.
- `badvpn-udpgw-linux-arm64` — aarch64 static musl build.

Each artifact includes the raw binary, a `.tar.gz` bundle with `badvpn-udpgw.service` and this README, plus a `SHA256SUMS-*.txt` file. Tagged `v*` builds also attach all artifacts to the GitHub release.

## Run on VPS (Drop-in Replacement)

```bash
./target/release/badvpn-udpgw \
  --listen-addr 127.0.0.1:7300 \
  --max-clients 1000 \
  --max-connections-for-client 10
```

### Install as systemd service (Linux VPS)

```bash
sudo cp target/release/badvpn-udpgw /usr/local/bin/badvpn-udpgw
sudo chmod +x /usr/local/bin/badvpn-udpgw
sudo cp badvpn-udpgw.service /etc/systemd/system/badvpn-udpgw.service
sudo systemctl daemon-reload
sudo systemctl enable --now badvpn-udpgw
sudo systemctl status badvpn-udpgw
```
