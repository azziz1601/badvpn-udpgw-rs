pub mod cli;
pub mod client;
pub mod dns;
pub mod logger;
pub mod port_pool;
pub mod protocol;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use tokio::net::TcpListener;
use tokio::sync::{watch, Semaphore};

use crate::cli::Config;
use crate::dns::SystemDns;
use crate::port_pool::PortPool;

/// Backoff duration after transient `accept()` errors (e.g. EMFILE/ENFILE) to prevent 100% CPU spin.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(50);

/// Binds a non-blocking TCP listener with `SO_REUSEADDR` enabled.
pub fn bind_tcp_listener(addr: SocketAddr) -> io::Result<TcpListener> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let sock = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    sock.set_reuse_address(true)?;
    sock.set_nonblocking(true)?;
    sock.bind(&SockAddr::from(addr))?;
    sock.listen(1024)?;
    let std_listener: std::net::TcpListener = sock.into();
    TcpListener::from_std(std_listener)
}

/// Runs the UDPGW server across all configured listeners until `shutdown_rx` is triggered.
pub async fn run_server(
    cfg: Arc<Config>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut listeners = Vec::with_capacity(cfg.listen_addrs.len());
    for &addr in &cfg.listen_addrs {
        let listener = bind_tcp_listener(addr)?;
        log_notice!("listening on {}", listener.local_addr()?);
        listeners.push(listener);
    }

    let dns = Arc::new(SystemDns::new());
    let port_pool = PortPool::new(
        cfg.local_udp_v4.clone(),
        cfg.local_udp_v6.clone(),
        cfg.unique_local_ports,
    );
    let client_slots = Arc::new(Semaphore::new(cfg.max_clients));

    log_notice!("entering event loop");

    let mut accept_tasks = Vec::with_capacity(listeners.len());
    for listener in listeners {
        let cfg = Arc::clone(&cfg);
        let dns = Arc::clone(&dns);
        let port_pool = Arc::clone(&port_pool);
        let client_slots = Arc::clone(&client_slots);
        let mut task_shutdown = shutdown_rx.clone();

        let task = tokio::spawn(async move {
            loop {
                let accept_res = tokio::select! {
                    biased;
                    _ = task_shutdown.changed() => break,
                    res = listener.accept() => res,
                };

                match accept_res {
                    Ok((stream, peer_addr)) => {
                        match Arc::clone(&client_slots).try_acquire_owned() {
                            Ok(permit) => {
                                let c_cfg = Arc::clone(&cfg);
                                let c_dns = Arc::clone(&dns);
                                let c_pool = Arc::clone(&port_pool);
                                let c_shutdown = task_shutdown.clone();
                                tokio::spawn(async move {
                                    let _permit = permit;
                                    client::handle_client(
                                        stream, peer_addr, c_cfg, c_dns, c_pool, c_shutdown,
                                    )
                                    .await;
                                });
                            }
                            Err(_) => {
                                log_error!(
                                    "maximum number of clients ({}) reached; rejecting {}",
                                    cfg.max_clients,
                                    peer_addr
                                );
                                drop(stream);
                            }
                        }
                    }
                    Err(e) => {
                        log_error!("accept error: {} (backing off 50ms)", e);
                        tokio::select! {
                            biased;
                            _ = task_shutdown.changed() => break,
                            _ = tokio::time::sleep(ACCEPT_ERROR_BACKOFF) => {}
                        }
                    }
                }
            }
        });
        accept_tasks.push(task);
    }

    let _ = shutdown_rx.changed().await;
    for task in accept_tasks {
        let _ = task.await;
    }

    Ok(())
}
