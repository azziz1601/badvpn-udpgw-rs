use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};

use crate::logger::LogLevel;
use crate::protocol::PACKETPROTO_MAX_PAYLOAD;

pub const PROGRAM_NAME: &str = "badvpn-udpgw";
pub const VERSION_STR: &str = env!("CARGO_PKG_VERSION");

pub const MAX_LISTEN_ADDRS: usize = 16;
pub const DEFAULT_UDP_MTU: usize = 65520;
pub const DEFAULT_MAX_CLIENTS: usize = 3;
pub const DEFAULT_MAX_CONNECTIONS_FOR_CLIENT: usize = 256;
pub const DEFAULT_CLIENT_SOCKET_SNDBUF: usize = 1_048_576;
pub const DEFAULT_CLIENT_DISCONNECT_TIMEOUT_MS: u64 = 20_000;
pub const DEFAULT_UDP_IDLE_TIMEOUT_SECS: u64 = 90;

#[derive(Debug, Clone)]
pub struct LocalUdpRange<A> {
    pub base_addr: A,
    pub num_ports: u16,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub show_help: bool,
    pub show_version: bool,
    pub loglevel: LogLevel,
    pub listen_addrs: Vec<SocketAddr>,
    pub udp_mtu: usize,
    pub max_clients: usize,
    pub max_connections_for_client: usize,
    pub client_socket_sndbuf: usize,
    pub local_udp_v4: Option<LocalUdpRange<SocketAddrV4>>,
    pub local_udp_v6: Option<LocalUdpRange<SocketAddrV6>>,
    pub unique_local_ports: bool,
    pub client_timeout_ms: u64,
    pub udp_idle_timeout_secs: u64,
    pub worker_threads: Option<usize>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            show_help: false,
            show_version: false,
            loglevel: LogLevel::Notice,
            listen_addrs: Vec::new(),
            udp_mtu: DEFAULT_UDP_MTU,
            max_clients: DEFAULT_MAX_CLIENTS,
            max_connections_for_client: DEFAULT_MAX_CONNECTIONS_FOR_CLIENT,
            client_socket_sndbuf: DEFAULT_CLIENT_SOCKET_SNDBUF,
            local_udp_v4: None,
            local_udp_v6: None,
            unique_local_ports: false,
            client_timeout_ms: DEFAULT_CLIENT_DISCONNECT_TIMEOUT_MS,
            udp_idle_timeout_secs: DEFAULT_UDP_IDLE_TIMEOUT_SECS,
            worker_threads: None,
        }
    }
}

pub fn print_version() {
    println!(
        "badvpn-udpgw-rs {} (Rust high-performance rewrite of BadVPN udpgw)",
        VERSION_STR
    );
}

pub fn print_help(program: &str) {
    println!(
        "Usage:\n\
         \x20   {program}\n\
         \x20       [--help]\n\
         \x20       [--version]\n\
         \x20       [--logger <stdout|syslog>]\n\
         \x20       [--syslog-facility <string>]\n\
         \x20       [--syslog-ident <string>]\n\
         \x20       [--loglevel <0-5/none/error/warning/notice/info/debug>]\n\
         \x20       [--channel-loglevel <channel-name> <0-5/none/error/warning/notice/info/debug>] ...\n\
         \x20       [--listen-addr <addr>] ...\n\
         \x20       [--udp-mtu <bytes>]\n\
         \x20       [--max-clients <number>]\n\
         \x20       [--max-connections-for-client <number>]\n\
         \x20       [--client-socket-sndbuf <bytes / 0>]\n\
         \x20       [--local-udp-addrs <addr> <num_ports>]\n\
         \x20       [--local-udp-ip6-addrs <addr> <num_ports>]\n\
         \x20       [--unique-local-ports]\n\
         \x20       [--client-timeout <ms>]          (Rust extension, default: 20000)\n\
         \x20       [--udp-idle-timeout <secs / 0>]  (Rust extension, default: 90, 0=disabled)\n\
         \x20       [--threads <number>]             (Rust extension, default: CPU cores; 1 uses current_thread)\n\
         Address format is a.b.c.d:port (IPv4) or [addr]:port (IPv6)."
    );
}

pub fn parse_args<I, S>(iter: I) -> Result<Config, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args: Vec<String> = iter.into_iter().map(|s| s.as_ref().to_string()).collect();
    let mut cfg = Config::default();

    let mut i = 1;
    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "--help" | "-h" => {
                cfg.show_help = true;
            }
            "--version" | "-v" => {
                cfg.show_version = true;
            }
            "--logger" => {
                let val = next_arg(&args, &mut i, arg)?;
                if val != "stdout" && val != "syslog" {
                    return Err(format!(
                        "{arg}: wrong argument '{val}' (expected stdout or syslog)"
                    ));
                }
            }
            "--syslog-facility" | "--syslog-ident" => {
                let _ = next_arg(&args, &mut i, arg)?;
            }
            "--loglevel" => {
                let val = next_arg(&args, &mut i, arg)?;
                cfg.loglevel = LogLevel::parse(&val)
                    .ok_or_else(|| format!("{arg}: invalid loglevel '{val}'"))?;
            }
            "--channel-loglevel" => {
                let _channel = next_arg(&args, &mut i, arg)?;
                let lvl_str = next_arg(&args, &mut i, arg)?;
                let lvl = LogLevel::parse(&lvl_str)
                    .ok_or_else(|| format!("{arg}: invalid loglevel '{lvl_str}'"))?;
                cfg.loglevel = lvl;
            }
            "--listen-addr" => {
                let val = next_arg(&args, &mut i, arg)?;
                if cfg.listen_addrs.len() >= MAX_LISTEN_ADDRS {
                    return Err(format!(
                        "{arg}: too many listen addresses (max {MAX_LISTEN_ADDRS})"
                    ));
                }
                let addr: SocketAddr = val
                    .parse()
                    .map_err(|e| format!("{arg}: invalid address '{val}': {e}"))?;
                cfg.listen_addrs.push(addr);
            }
            "--udp-mtu" => {
                let val = next_arg(&args, &mut i, arg)?;
                let mtu: usize = val
                    .parse()
                    .map_err(|_| format!("{arg}: invalid number '{val}'"))?;
                if mtu > PACKETPROTO_MAX_PAYLOAD {
                    return Err(format!(
                        "{arg}: value {mtu} exceeds maximum {PACKETPROTO_MAX_PAYLOAD}"
                    ));
                }
                cfg.udp_mtu = mtu;
            }
            "--max-clients" => {
                let val = next_arg(&args, &mut i, arg)?;
                let n: usize = val
                    .parse()
                    .map_err(|_| format!("{arg}: invalid number '{val}'"))?;
                if n == 0 {
                    return Err(format!("{arg}: must be > 0"));
                }
                cfg.max_clients = n;
            }
            "--max-connections-for-client" => {
                let val = next_arg(&args, &mut i, arg)?;
                let n: usize = val
                    .parse()
                    .map_err(|_| format!("{arg}: invalid number '{val}'"))?;
                if n == 0 {
                    return Err(format!("{arg}: must be > 0"));
                }
                cfg.max_connections_for_client = n;
            }
            "--client-socket-sndbuf" => {
                let val = next_arg(&args, &mut i, arg)?;
                let n: usize = val
                    .parse()
                    .map_err(|_| format!("{arg}: invalid number '{val}'"))?;
                cfg.client_socket_sndbuf = n;
            }
            "--local-udp-addrs" => {
                let addr_str = next_arg(&args, &mut i, arg)?;
                let num_str = next_arg(&args, &mut i, arg)?;
                let base_addr: SocketAddrV4 = addr_str
                    .parse()
                    .map_err(|e| format!("{arg}: must be IPv4 addr:port '{addr_str}': {e}"))?;
                let num_ports: u16 = num_str
                    .parse()
                    .map_err(|_| format!("{arg}: invalid num_ports '{num_str}'"))?;
                validate_port_range(arg, base_addr.port(), num_ports)?;
                cfg.local_udp_v4 = Some(LocalUdpRange {
                    base_addr,
                    num_ports,
                });
            }
            "--local-udp-ip6-addrs" => {
                let addr_str = next_arg(&args, &mut i, arg)?;
                let num_str = next_arg(&args, &mut i, arg)?;
                let base_addr: SocketAddrV6 = addr_str
                    .parse()
                    .map_err(|e| format!("{arg}: must be IPv6 [addr]:port '{addr_str}': {e}"))?;
                let num_ports: u16 = num_str
                    .parse()
                    .map_err(|_| format!("{arg}: invalid num_ports '{num_str}'"))?;
                validate_port_range(arg, base_addr.port(), num_ports)?;
                cfg.local_udp_v6 = Some(LocalUdpRange {
                    base_addr,
                    num_ports,
                });
            }
            "--unique-local-ports" => {
                cfg.unique_local_ports = true;
            }
            "--client-timeout" => {
                let val = next_arg(&args, &mut i, arg)?;
                let ms: u64 = val
                    .parse()
                    .map_err(|_| format!("{arg}: invalid milliseconds '{val}'"))?;
                if ms == 0 {
                    return Err(format!("{arg}: must be > 0"));
                }
                cfg.client_timeout_ms = ms;
            }
            "--udp-idle-timeout" => {
                let val = next_arg(&args, &mut i, arg)?;
                let secs: u64 = val
                    .parse()
                    .map_err(|_| format!("{arg}: invalid seconds '{val}'"))?;
                cfg.udp_idle_timeout_secs = secs;
            }
            "--threads" => {
                let val = next_arg(&args, &mut i, arg)?;
                let n: usize = val
                    .parse()
                    .map_err(|_| format!("{arg}: invalid thread count '{val}'"))?;
                if n == 0 {
                    return Err(format!("{arg}: must be > 0"));
                }
                cfg.worker_threads = Some(n);
            }
            other => {
                return Err(format!("unknown option: {other}"));
            }
        }
        i += 1;
    }

    Ok(cfg)
}

fn validate_port_range(flag: &str, base_port: u16, num_ports: u16) -> Result<(), String> {
    if num_ports == 0 {
        return Ok(());
    }
    if base_port == 0 {
        return Err(format!("{flag}: base port must be > 0 when num_ports > 0"));
    }
    let end_port = (base_port as u32) + (num_ports as u32) - 1;
    if end_port > u16::MAX as u32 {
        return Err(format!(
            "{flag}: port range {base_port}..={end_port} exceeds 65535"
        ));
    }
    Ok(())
}

fn next_arg(args: &[String], idx: &mut usize, flag: &str) -> Result<String, String> {
    if *idx + 1 >= args.len() {
        return Err(format!("{flag}: requires an argument"));
    }
    *idx += 1;
    Ok(args[*idx].clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_typical_vps_command() {
        let cfg = parse_args([
            "badvpn-udpgw",
            "--listen-addr",
            "127.0.0.1:7300",
            "--max-clients",
            "1000",
            "--max-connections-for-client",
            "10",
            "--udp-idle-timeout",
            "60",
            "--loglevel",
            "info",
        ])
        .unwrap();

        assert_eq!(cfg.listen_addrs.len(), 1);
        assert_eq!(cfg.listen_addrs[0].to_string(), "127.0.0.1:7300");
        assert_eq!(cfg.max_clients, 1000);
        assert_eq!(cfg.max_connections_for_client, 10);
        assert_eq!(cfg.udp_idle_timeout_secs, 60);
        assert_eq!(cfg.loglevel, LogLevel::Info);
    }

    #[test]
    fn parse_ipv6_and_local_ports() {
        let cfg = parse_args([
            "badvpn-udpgw",
            "--listen-addr",
            "[::1]:7300",
            "--local-udp-addrs",
            "0.0.0.0:20000",
            "100",
            "--unique-local-ports",
        ])
        .unwrap();

        assert_eq!(cfg.listen_addrs[0].to_string(), "[::1]:7300");
        let r = cfg.local_udp_v4.unwrap();
        assert_eq!(r.base_addr.port(), 20000);
        assert_eq!(r.num_ports, 100);
        assert!(cfg.unique_local_ports);
    }

    #[test]
    fn reject_port_range_overflow_and_oversized_mtu() {
        assert!(parse_args([
            "badvpn-udpgw",
            "--local-udp-addrs",
            "0.0.0.0:65500",
            "100",
        ])
        .is_err());

        assert!(parse_args(["badvpn-udpgw", "--udp-mtu", "999999"]).is_err());
    }

    #[test]
    fn reject_unknown_flag() {
        let err = parse_args(["badvpn-udpgw", "--nonexistent"]).unwrap_err();
        assert!(err.contains("unknown option"));
    }
}
