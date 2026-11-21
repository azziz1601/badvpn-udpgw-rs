use std::collections::HashSet;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::{Arc, Mutex};

use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use tokio::net::UdpSocket;

use crate::cli::LocalUdpRange;
use crate::log_warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RemoteKey {
    Addr(SocketAddr),
    Ip(IpAddr),
}

struct PoolInner {
    v4_range: Option<LocalUdpRange<SocketAddrV4>>,
    v6_range: Option<LocalUdpRange<SocketAddrV6>>,
    unique_local_ports: bool,
    used_v4: HashSet<(RemoteKey, u16)>,
    used_v6: HashSet<(RemoteKey, u16)>,
}

/// Manages optional `--local-udp-addrs` / `--local-udp-ip6-addrs` port ranges across all clients.
pub struct PortPool {
    inner: Mutex<PoolInner>,
}

/// RAII lease that automatically releases the reserved `(remote_key, port_index)` when dropped.
pub struct PortLease {
    pool: Arc<PortPool>,
    is_v6: bool,
    key: RemoteKey,
    port_index: u16,
}

impl Drop for PortLease {
    fn drop(&mut self) {
        let mut inner = self.pool.inner.lock().unwrap_or_else(|e| e.into_inner());
        if self.is_v6 {
            inner.used_v6.remove(&(self.key, self.port_index));
        } else {
            inner.used_v4.remove(&(self.key, self.port_index));
        }
    }
}

impl PortPool {
    pub fn new(
        v4_range: Option<LocalUdpRange<SocketAddrV4>>,
        v6_range: Option<LocalUdpRange<SocketAddrV6>>,
        unique_local_ports: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(PoolInner {
                v4_range,
                v6_range,
                unique_local_ports,
                used_v4: HashSet::new(),
                used_v6: HashSet::new(),
            }),
        })
    }

    /// Creates a non-blocking Tokio `UdpSocket` matching the address family of `remote_addr`,
    /// binding to a configured local UDP port range if specified (falling back to ephemeral port).
    pub fn create_udp_socket(
        self: &Arc<Self>,
        remote_addr: SocketAddr,
    ) -> io::Result<(UdpSocket, Option<PortLease>)> {
        let is_v6 = remote_addr.is_ipv6();
        let candidate = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let key = if inner.unique_local_ports {
                RemoteKey::Ip(remote_addr.ip())
            } else {
                RemoteKey::Addr(remote_addr)
            };

            if is_v6 {
                inner.v6_range.clone().and_then(|range| {
                    find_free_port(&mut inner.used_v6, key, range.num_ports).map(|idx| {
                        let port = range.base_addr.port().wrapping_add(idx);
                        let bind_addr = SocketAddr::V6(SocketAddrV6::new(
                            *range.base_addr.ip(),
                            port,
                            range.base_addr.flowinfo(),
                            range.base_addr.scope_id(),
                        ));
                        (bind_addr, key, idx)
                    })
                })
            } else {
                inner.v4_range.clone().and_then(|range| {
                    find_free_port(&mut inner.used_v4, key, range.num_ports).map(|idx| {
                        let port = range.base_addr.port().wrapping_add(idx);
                        let bind_addr =
                            SocketAddr::V4(SocketAddrV4::new(*range.base_addr.ip(), port));
                        (bind_addr, key, idx)
                    })
                })
            }
        };

        if let Some((bind_addr, key, idx)) = candidate {
            match bind_udp_socket(bind_addr, true) {
                Ok(udp) => {
                    let lease = PortLease {
                        pool: Arc::clone(self),
                        is_v6,
                        key,
                        port_index: idx,
                    };
                    return Ok((udp, Some(lease)));
                }
                Err(err) => {
                    // Release reservation and fall back to ephemeral port (matches badvpn-udpgw).
                    let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                    if is_v6 {
                        inner.used_v6.remove(&(key, idx));
                    } else {
                        inner.used_v4.remove(&(key, idx));
                    }
                    log_warn!(
                        "failed to bind local UDP address {}: {}; falling back to ephemeral port",
                        bind_addr,
                        err
                    );
                }
            }
        }

        let ephemeral = if is_v6 {
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0))
        } else {
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
        };
        let udp = bind_udp_socket(ephemeral, false)?;
        Ok((udp, None))
    }
}

fn find_free_port(
    used: &mut HashSet<(RemoteKey, u16)>,
    key: RemoteKey,
    num_ports: u16,
) -> Option<u16> {
    for idx in 0..num_ports {
        if !used.contains(&(key, idx)) {
            used.insert((key, idx));
            return Some(idx);
        }
    }
    None
}

fn bind_udp_socket(bind_addr: SocketAddr, reuse_addr: bool) -> io::Result<UdpSocket> {
    let domain = match bind_addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let sock = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    if reuse_addr {
        sock.set_reuse_address(true)?;
    }
    sock.set_nonblocking(true)?;
    sock.bind(&SockAddr::from(bind_addr))?;
    let std_udp: std::net::UdpSocket = sock.into();
    UdpSocket::from_std(std_udp)
}
