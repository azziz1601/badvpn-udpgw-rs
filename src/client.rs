use std::collections::HashMap;
use std::io::{self, IoSlice};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use socket2::SockRef;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{timeout, timeout_at, Instant as TokioInstant};

use crate::cli::Config;
use crate::dns::SystemDns;
use crate::port_pool::{PortLease, PortPool};
use crate::protocol::{
    compute_udpgw_mtu, decode_client_packet, encode_server_packet_into, ClientPacket,
    UDPGW_CLIENT_FLAG_DNS, UDPGW_CLIENT_FLAG_REBIND,
};
use crate::{log_debug, log_error, log_info, log_warn};

/// Bounded per-client outbound TCP packet queue capacity.
const CLIENT_OUTBOUND_QUEUE_CAP: usize = 256;
/// Maximum number of frames coalesced into a single `write_vectored` syscall.
const WRITE_BATCH_MAX: usize = 32;
/// Maximum recycled outbound frame buffers kept per client.
const FRAME_POOL_MAX_ITEMS: usize = 64;
/// Avoid retaining unusually large buffers in the frame pool.
const FRAME_POOL_MAX_CAP: usize = 8192;
/// Maximum shared UDP receive buffers kept per client (usually 1 is enough).
const RECV_POOL_MAX_ITEMS: usize = 4;
/// Max datagrams drained per turn in a UDP receiver before yielding (prevents 1-vCPU starvation).
const UDP_RECV_BUDGET: usize = 64;

/// Lightweight per-client buffer pool to eliminate per-packet heap allocations.
struct BufPool {
    bufs: Mutex<Vec<Vec<u8>>>,
    max_items: usize,
    max_retain_cap: usize,
}

impl BufPool {
    fn new(max_items: usize, max_retain_cap: usize) -> Arc<Self> {
        Arc::new(Self {
            bufs: Mutex::new(Vec::with_capacity(max_items.min(16))),
            max_items,
            max_retain_cap,
        })
    }

    fn checkout_empty(&self, default_cap: usize) -> Vec<u8> {
        let mut guard = self.bufs.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(mut buf) = guard.pop() {
            buf.clear();
            buf
        } else {
            Vec::with_capacity(default_cap)
        }
    }

    fn checkout_zeroed(&self, len: usize) -> Vec<u8> {
        let mut guard = self.bufs.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(mut buf) = guard.pop() {
            if buf.len() < len {
                buf.resize(len, 0);
            }
            buf
        } else {
            vec![0u8; len]
        }
    }

    fn checkin(&self, buf: Vec<u8>) {
        if buf.capacity() > self.max_retain_cap {
            return;
        }
        let mut guard = self.bufs.lock().unwrap_or_else(|e| e.into_inner());
        if guard.len() < self.max_items {
            guard.push(buf);
        }
    }
}

struct UdpConnEntry {
    gen_id: u64,
    orig_addr: SocketAddr,
    remote_addr: SocketAddr,
    socket: Arc<UdpSocket>,
    last_used_ms: Arc<AtomicU64>,
    recv_task: JoinHandle<()>,
    _port_lease: Option<PortLease>,
}

impl Drop for UdpConnEntry {
    fn drop(&mut self) {
        self.recv_task.abort();
    }
}

pub async fn handle_client(
    stream: TcpStream,
    peer_addr: SocketAddr,
    cfg: Arc<Config>,
    dns: Arc<SystemDns>,
    port_pool: Arc<PortPool>,
    mut shutdown: watch::Receiver<bool>,
) {
    if let Err(e) = stream.set_nodelay(true) {
        log_warn!("client ({}): failed to set TCP_NODELAY: {}", peer_addr, e);
    }

    if cfg.client_socket_sndbuf > 0 {
        let sock_ref = SockRef::from(&stream);
        if let Err(e) = sock_ref.set_send_buffer_size(cfg.client_socket_sndbuf) {
            log_warn!(
                "client ({}): failed to set SO_SNDBUF={}: {}",
                peer_addr,
                cfg.client_socket_sndbuf,
                e
            );
        }
    }

    log_info!("client ({}): connected", peer_addr);

    let udpgw_mtu = compute_udpgw_mtu(cfg.udp_mtu);
    let client_timeout = Duration::from_millis(cfg.client_timeout_ms);
    let udp_idle_timeout = if cfg.udp_idle_timeout_secs > 0 {
        Some(Duration::from_secs(cfg.udp_idle_timeout_secs))
    } else {
        None
    };

    let session_start = Instant::now();
    let frame_pool = BufPool::new(FRAME_POOL_MAX_ITEMS, FRAME_POOL_MAX_CAP);
    let recv_pool = BufPool::new(RECV_POOL_MAX_ITEMS, usize::MAX);

    let (mut reader, mut writer) = stream.into_split();
    let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(CLIENT_OUTBOUND_QUEUE_CAP);
    let expired_cap = cfg.max_connections_for_client.saturating_mul(2).max(32);
    let (expired_tx, mut expired_rx) = mpsc::channel::<(u16, u64)>(expired_cap);

    // Dedicated writer task with vectored syscall batching and buffer recycling.
    let writer_peer = peer_addr;
    let writer_frame_pool = Arc::clone(&frame_pool);
    let writer_task = tokio::spawn(async move {
        let mut batch: Vec<Vec<u8>> = Vec::with_capacity(WRITE_BATCH_MAX);
        while let Some(first) = out_rx.recv().await {
            batch.push(first);
            while batch.len() < WRITE_BATCH_MAX {
                match out_rx.try_recv() {
                    Ok(next) => batch.push(next),
                    Err(_) => break,
                }
            }

            let write_res = if batch.len() == 1 {
                writer.write_all(&batch[0]).await
            } else {
                write_all_vectored(&mut writer, &batch).await
            };

            for buf in batch.drain(..) {
                writer_frame_pool.checkin(buf);
            }

            if let Err(e) = write_res {
                log_debug!("client ({}): TCP write error: {}", writer_peer, e);
                break;
            }
        }
    });

    let mut connections: HashMap<u16, UdpConnEntry> =
        HashMap::with_capacity(cfg.max_connections_for_client.min(64));
    let mut next_gen_id: u64 = 1;

    // Reusable TCP frame buffer (zero per-packet heap allocation on TCP receive).
    let mut len_buf = [0u8; 2];
    let mut body_buf = vec![0u8; udpgw_mtu];

    loop {
        // Reap any idle-expired UDP connections before waiting on TCP.
        while let Ok((exp_conid, exp_gen)) = expired_rx.try_recv() {
            reap_expired(&mut connections, peer_addr, exp_conid, exp_gen);
        }

        // Single strict deadline for the entire frame (2-byte header + body).
        let deadline = TokioInstant::now() + client_timeout;
        let mut len_read = 0usize;
        let mut disconnected = false;

        // Cancel-safe 2-byte PacketProto length read.
        while len_read < 2 {
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    log_debug!("client ({}): shutting down", peer_addr);
                    disconnected = true;
                    break;
                }
                _ = out_tx.closed() => {
                    log_info!("client ({}): writer closed, disconnecting", peer_addr);
                    disconnected = true;
                    break;
                }
                Some((exp_conid, exp_gen)) = expired_rx.recv() => {
                    reap_expired(&mut connections, peer_addr, exp_conid, exp_gen);
                }
                res = timeout_at(deadline, reader.read(&mut len_buf[len_read..])) => {
                    match res {
                        Ok(Ok(0)) => {
                            log_info!("client ({}): closed", peer_addr);
                            disconnected = true;
                            break;
                        }
                        Ok(Ok(n)) => {
                            len_read += n;
                        }
                        Ok(Err(e)) => {
                            log_info!("client ({}): read error: {}", peer_addr, e);
                            disconnected = true;
                            break;
                        }
                        Err(_) => {
                            log_info!("client ({}): timed out, disconnecting", peer_addr);
                            disconnected = true;
                            break;
                        }
                    }
                }
            }
        }

        if disconnected {
            break;
        }

        let frame_len = u16::from_le_bytes(len_buf) as usize;
        if frame_len > udpgw_mtu {
            log_error!(
                "client ({}): decoder error (frame length {} > mtu {})",
                peer_addr,
                frame_len,
                udpgw_mtu
            );
            break;
        }

        if frame_len > 0 {
            let read_body_res = tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    log_debug!("client ({}): shutting down", peer_addr);
                    break;
                }
                _ = out_tx.closed() => {
                    log_info!("client ({}): writer closed mid-frame, disconnecting", peer_addr);
                    break;
                }
                res = timeout_at(deadline, reader.read_exact(&mut body_buf[..frame_len])) => res,
            };

            match read_body_res {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        log_info!("client ({}): closed mid-frame", peer_addr);
                    } else {
                        log_info!("client ({}): read error: {}", peer_addr, e);
                    }
                    break;
                }
                Err(_) => {
                    log_info!("client ({}): timed out mid-frame, disconnecting", peer_addr);
                    break;
                }
            }
        }

        let packet = match decode_client_packet(&body_buf[..frame_len], cfg.udp_mtu) {
            Ok(pkt) => pkt,
            Err(err) => {
                log_error!("client ({}): {}", peer_addr, err);
                continue;
            }
        };

        match packet {
            ClientPacket::Keepalive { .. } => {
                log_debug!("client ({}): received keepalive", peer_addr);
            }
            ClientPacket::Data {
                flags,
                conid,
                orig_addr,
                payload,
            } => {
                let need_recreate = match connections.get(&conid) {
                    Some(existing) => {
                        (flags & UDPGW_CLIENT_FLAG_REBIND) != 0 || existing.orig_addr != orig_addr
                    }
                    None => true,
                };

                let now_ms = elapsed_ms(session_start);

                if need_recreate {
                    if connections.remove(&conid).is_some() {
                        log_debug!("client ({}): connection {}: closed old", peer_addr, conid);
                    }

                    if connections.len() >= cfg.max_connections_for_client {
                        if let Some(evict_id) = find_lru_conid(&connections) {
                            connections.remove(&evict_id);
                            log_debug!(
                                "client ({}): evicted LRU connection {}",
                                peer_addr,
                                evict_id
                            );
                        }
                    }

                    let mut remote_addr = orig_addr;
                    if (flags & UDPGW_CLIENT_FLAG_DNS) != 0 {
                        if let Some(dns_server) = dns.get_server() {
                            log_debug!(
                                "client ({}): redirecting DNS packet to {}",
                                peer_addr,
                                dns_server
                            );
                            remote_addr = dns_server;
                        } else {
                            log_warn!(
                                "client ({}): received DNS packet, but no DNS server available",
                                peer_addr
                            );
                        }
                    }

                    match port_pool.create_udp_socket(remote_addr) {
                        Ok((udp_sock, port_lease)) => {
                            let gen_id = next_gen_id;
                            next_gen_id = next_gen_id.wrapping_add(1);

                            let socket = Arc::new(udp_sock);
                            let last_used_ms = Arc::new(AtomicU64::new(now_ms));

                            let recv_task = spawn_udp_receiver(
                                Arc::clone(&socket),
                                peer_addr,
                                conid,
                                gen_id,
                                orig_addr,
                                cfg.udp_mtu,
                                udpgw_mtu,
                                udp_idle_timeout,
                                session_start,
                                out_tx.clone(),
                                expired_tx.clone(),
                                Arc::clone(&frame_pool),
                                Arc::clone(&recv_pool),
                                Arc::clone(&last_used_ms),
                            );

                            let entry = UdpConnEntry {
                                gen_id,
                                orig_addr,
                                remote_addr,
                                socket,
                                last_used_ms,
                                recv_task,
                                _port_lease: port_lease,
                            };
                            connections.insert(conid, entry);
                            log_debug!("client ({}): connection {}: initialized", peer_addr, conid);
                        }
                        Err(e) => {
                            log_error!(
                                "client ({}): failed to create UDP socket for {}: {}",
                                peer_addr,
                                remote_addr,
                                e
                            );
                            continue;
                        }
                    }
                }

                if let Some(entry) = connections.get(&conid) {
                    entry.last_used_ms.store(now_ms, Ordering::Relaxed);
                    log_debug!(
                        "client ({}): connection {}: from client {} bytes -> {}",
                        peer_addr,
                        conid,
                        payload.len(),
                        entry.remote_addr
                    );
                    // Fast-path non-blocking send; on WouldBlock (including fresh Tokio sockets
                    // whose readiness bit is not yet initialized), fall back to a bounded send_to.
                    let send_res = match entry.socket.try_send_to(payload, entry.remote_addr) {
                        Ok(n) => Ok(Ok(n)),
                        Err(ref e)
                            if e.kind() == io::ErrorKind::WouldBlock
                                || e.kind() == io::ErrorKind::Interrupted =>
                        {
                            timeout(
                                Duration::from_millis(50),
                                entry.socket.send_to(payload, entry.remote_addr),
                            )
                            .await
                        }
                        Err(e) => Ok(Err(e)),
                    };

                    match send_res {
                        Ok(Ok(_)) => {}
                        Err(_) => {
                            log_warn!(
                                "client ({}): connection {}: out of UDP send buffer, dropping packet",
                                peer_addr,
                                conid
                            );
                        }
                        Ok(Err(e)) => {
                            log_info!(
                                "client ({}): connection {}: UDP send error: {}",
                                peer_addr,
                                conid,
                                e
                            );
                            connections.remove(&conid);
                        }
                    }
                }
            }
        }
    }

    drop(connections);
    drop(out_tx);
    writer_task.abort();
}

fn reap_expired(
    connections: &mut HashMap<u16, UdpConnEntry>,
    peer_addr: SocketAddr,
    conid: u16,
    gen_id: u64,
) {
    if connections
        .get(&conid)
        .map(|c| c.gen_id == gen_id)
        .unwrap_or(false)
    {
        connections.remove(&conid);
        log_debug!(
            "client ({}): connection {}: closed due to UDP idle timeout",
            peer_addr,
            conid
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_udp_receiver(
    socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
    conid: u16,
    gen_id: u64,
    orig_addr: SocketAddr,
    udp_mtu: usize,
    udpgw_mtu: usize,
    idle_timeout: Option<Duration>,
    session_start: Instant,
    out_tx: mpsc::Sender<Vec<u8>>,
    expired_tx: mpsc::Sender<(u16, u64)>,
    frame_pool: Arc<BufPool>,
    recv_pool: Arc<BufPool>,
    last_used_ms: Arc<AtomicU64>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let buf_len = udp_mtu.max(1);
        let mut wait_dur = idle_timeout;

        loop {
            // Hold 0 bytes of receive buffer while waiting for kernel readiness.
            let ready_res = match wait_dur {
                Some(dur) => match timeout(dur, socket.readable()).await {
                    Ok(res) => res,
                    Err(_) => {
                        let full_dur = idle_timeout.unwrap_or(dur);
                        let now_ms = elapsed_ms(session_start);
                        let last = last_used_ms.load(Ordering::Relaxed);
                        let elapsed = Duration::from_millis(now_ms.saturating_sub(last));
                        if elapsed >= full_dur {
                            let _ = expired_tx.try_send((conid, gen_id));
                            break;
                        }
                        wait_dur = Some(full_dur.saturating_sub(elapsed));
                        continue;
                    }
                },
                None => socket.readable().await,
            };

            // Reset remaining idle window after readability wakeup.
            wait_dur = idle_timeout;

            if let Err(e) = ready_res {
                log_info!(
                    "client ({}): connection {}: UDP readability error: {}",
                    peer_addr,
                    conid,
                    e
                );
                let _ = expired_tx.try_send((conid, gen_id));
                break;
            }

            // Borrow a shared receive buffer only while draining ready datagrams (budget-capped).
            let mut recv_buf = recv_pool.checkout_zeroed(buf_len);
            let mut fatal = false;
            let mut drained = 0usize;

            while drained < UDP_RECV_BUDGET {
                match socket.try_recv_from(&mut recv_buf) {
                    Ok((n, _src)) => {
                        drained += 1;
                        let now_ms = elapsed_ms(session_start);
                        last_used_ms.store(now_ms, Ordering::Relaxed);
                        log_debug!(
                            "client ({}): connection {}: from UDP {} bytes",
                            peer_addr,
                            conid,
                            n
                        );

                        let mut frame = frame_pool.checkout_empty(n + 32);
                        if encode_server_packet_into(
                            &mut frame,
                            0,
                            conid,
                            orig_addr,
                            &recv_buf[..n],
                            udpgw_mtu,
                        ) {
                            if let Err(err) = out_tx.try_send(frame) {
                                match err {
                                    mpsc::error::TrySendError::Full(returned) => {
                                        frame_pool.checkin(returned);
                                        log_warn!(
                                            "client ({}): connection {}: out of client buffer, dropping packet",
                                            peer_addr,
                                            conid
                                        );
                                    }
                                    mpsc::error::TrySendError::Closed(returned) => {
                                        frame_pool.checkin(returned);
                                        fatal = true;
                                        break;
                                    }
                                }
                            }
                        } else {
                            frame_pool.checkin(frame);
                            log_warn!(
                                "client ({}): connection {}: packet too large ({} bytes), dropping",
                                peer_addr,
                                conid,
                                n
                            );
                        }
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        break;
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
                        continue;
                    }
                    Err(e) => {
                        log_info!(
                            "client ({}): connection {}: UDP recv error: {}",
                            peer_addr,
                            conid,
                            e
                        );
                        fatal = true;
                        break;
                    }
                }
            }

            recv_pool.checkin(recv_buf);

            if fatal {
                let _ = expired_tx.try_send((conid, gen_id));
                break;
            }

            // Yield cooperatively under UDP flood so other tasks on 1-vCPU are never starved.
            if drained >= UDP_RECV_BUDGET {
                tokio::task::yield_now().await;
            }
        }
    })
}

async fn write_all_vectored(writer: &mut OwnedWriteHalf, batch: &[Vec<u8>]) -> io::Result<()> {
    let mut idx = 0usize;
    let mut offset = 0usize;

    while idx < batch.len() {
        let mut slices: [IoSlice<'_>; WRITE_BATCH_MAX] = [IoSlice::new(&[]); WRITE_BATCH_MAX];
        let mut count = 0usize;

        for (i, buf) in batch[idx..].iter().enumerate() {
            if count >= WRITE_BATCH_MAX {
                break;
            }
            let slice = if i == 0 { &buf[offset..] } else { &buf[..] };
            if !slice.is_empty() {
                slices[count] = IoSlice::new(slice);
                count += 1;
            }
        }

        if count == 0 {
            break;
        }

        let mut written = writer.write_vectored(&slices[..count]).await?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write whole vectored batch",
            ));
        }

        while written > 0 && idx < batch.len() {
            let rem = batch[idx].len() - offset;
            if written >= rem {
                written -= rem;
                idx += 1;
                offset = 0;
            } else {
                offset += written;
                written = 0;
            }
        }
    }

    Ok(())
}

#[inline]
fn elapsed_ms(start: Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

fn find_lru_conid(connections: &HashMap<u16, UdpConnEntry>) -> Option<u16> {
    connections
        .iter()
        .min_by_key(|(_, entry)| entry.last_used_ms.load(Ordering::Relaxed))
        .map(|(&conid, _)| conid)
}
