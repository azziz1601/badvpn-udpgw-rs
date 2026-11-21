use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

/// PacketProto max payload (uint16_t max).
pub const PACKETPROTO_MAX_PAYLOAD: usize = u16::MAX as usize;
pub const PACKETPROTO_HEADER_LEN: usize = 2;

/// UDPGW header: flags (u8) + conid (u16 LE).
pub const UDPGW_HEADER_LEN: usize = 3;
/// IPv4 address: 4-byte IP + 2-byte big-endian port.
pub const UDPGW_ADDR_IPV4_LEN: usize = 6;
/// IPv6 address: 16-byte IP + 2-byte big-endian port.
pub const UDPGW_ADDR_IPV6_LEN: usize = 18;

pub const UDPGW_CLIENT_FLAG_KEEPALIVE: u8 = 1 << 0;
pub const UDPGW_CLIENT_FLAG_REBIND: u8 = 1 << 1;
pub const UDPGW_CLIENT_FLAG_DNS: u8 = 1 << 2;
pub const UDPGW_CLIENT_FLAG_IPV6: u8 = 1 << 3;

/// Computes the maximum UDPGW packet payload size (excluding the 2-byte PacketProto length),
/// matching `udpgw_compute_mtu` clamped to `PACKETPROTO_MAX_PAYLOAD`.
pub fn compute_udpgw_mtu(udp_mtu: usize) -> usize {
    let raw = UDPGW_HEADER_LEN
        .saturating_add(UDPGW_ADDR_IPV6_LEN)
        .saturating_add(udp_mtu);
    raw.min(PACKETPROTO_MAX_PAYLOAD)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientPacket<'a> {
    Keepalive {
        conid: u16,
    },
    Data {
        flags: u8,
        conid: u16,
        orig_addr: SocketAddr,
        payload: &'a [u8],
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    MissingHeader,
    MissingIpv4Addr,
    MissingIpv6Addr,
    PayloadTooLarge { len: usize, max: usize },
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingHeader => write!(f, "missing udpgw header"),
            Self::MissingIpv4Addr => write!(f, "missing ipv4 address"),
            Self::MissingIpv6Addr => write!(f, "missing ipv6 address"),
            Self::PayloadTooLarge { len, max } => {
                write!(f, "payload too large ({len} > {max})")
            }
        }
    }
}

/// Decodes a single UDPGW frame body (after the 2-byte PacketProto length prefix has been read).
pub fn decode_client_packet(buf: &[u8], udp_mtu: usize) -> Result<ClientPacket<'_>, DecodeError> {
    if buf.len() < UDPGW_HEADER_LEN {
        return Err(DecodeError::MissingHeader);
    }

    let flags = buf[0];
    let conid = u16::from_le_bytes([buf[1], buf[2]]);

    if (flags & UDPGW_CLIENT_FLAG_KEEPALIVE) != 0 {
        return Ok(ClientPacket::Keepalive { conid });
    }

    let rest = &buf[UDPGW_HEADER_LEN..];

    let (orig_addr, payload) = if (flags & UDPGW_CLIENT_FLAG_IPV6) != 0 {
        if rest.len() < UDPGW_ADDR_IPV6_LEN {
            return Err(DecodeError::MissingIpv6Addr);
        }
        let mut ip_bytes = [0u8; 16];
        ip_bytes.copy_from_slice(&rest[..16]);
        let port = u16::from_be_bytes([rest[16], rest[17]]);
        let addr = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(ip_bytes), port, 0, 0));
        (addr, &rest[UDPGW_ADDR_IPV6_LEN..])
    } else {
        if rest.len() < UDPGW_ADDR_IPV4_LEN {
            return Err(DecodeError::MissingIpv4Addr);
        }
        let ip = Ipv4Addr::new(rest[0], rest[1], rest[2], rest[3]);
        let port = u16::from_be_bytes([rest[4], rest[5]]);
        let addr = SocketAddr::V4(SocketAddrV4::new(ip, port));
        (addr, &rest[UDPGW_ADDR_IPV4_LEN..])
    };

    if payload.len() > udp_mtu {
        return Err(DecodeError::PayloadTooLarge {
            len: payload.len(),
            max: udp_mtu,
        });
    }

    Ok(ClientPacket::Data {
        flags,
        conid,
        orig_addr,
        payload,
    })
}

/// Encodes a server->client UDPGW packet WITH the 2-byte little-endian PacketProto length prefix
/// directly into a reusable `out` buffer (zero heap allocation when `out` has enough capacity).
/// Returns `false` if the encoded UDPGW body exceeds `udpgw_mtu`.
pub fn encode_server_packet_into(
    out: &mut Vec<u8>,
    flags: u8,
    conid: u16,
    orig_addr: SocketAddr,
    payload: &[u8],
    udpgw_mtu: usize,
) -> bool {
    let addr_len = match orig_addr {
        SocketAddr::V4(_) => UDPGW_ADDR_IPV4_LEN,
        SocketAddr::V6(_) => UDPGW_ADDR_IPV6_LEN,
    };

    let body_len = match UDPGW_HEADER_LEN
        .checked_add(addr_len)
        .and_then(|v| v.checked_add(payload.len()))
    {
        Some(len) => len,
        None => return false,
    };

    if body_len > udpgw_mtu || body_len > PACKETPROTO_MAX_PAYLOAD {
        return false;
    }

    let mut wire_flags = flags;
    if orig_addr.is_ipv6() {
        wire_flags |= UDPGW_CLIENT_FLAG_IPV6;
    }

    out.clear();
    out.reserve(PACKETPROTO_HEADER_LEN + body_len);
    out.extend_from_slice(&(body_len as u16).to_le_bytes());
    out.push(wire_flags);
    out.extend_from_slice(&conid.to_le_bytes());

    match orig_addr {
        SocketAddr::V4(v4) => {
            out.extend_from_slice(&v4.ip().octets());
            out.extend_from_slice(&v4.port().to_be_bytes());
        }
        SocketAddr::V6(v6) => {
            out.extend_from_slice(&v6.ip().octets());
            out.extend_from_slice(&v6.port().to_be_bytes());
        }
    }

    out.extend_from_slice(payload);
    true
}

/// Convenience wrapper that allocates a new `Vec<u8>` and encodes a server->client frame.
pub fn encode_server_packet(
    flags: u8,
    conid: u16,
    orig_addr: SocketAddr,
    payload: &[u8],
    udpgw_mtu: usize,
) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    if encode_server_packet_into(&mut out, flags, conid, orig_addr, payload, udpgw_mtu) {
        Some(out)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_ipv4_packet() {
        let addr: SocketAddr = "8.8.8.8:53".parse().unwrap();
        let payload = b"hello-udp";
        let mtu = compute_udpgw_mtu(65520);

        let framed = encode_server_packet(0, 0x1234, addr, payload, mtu).unwrap();
        let body_len = u16::from_le_bytes([framed[0], framed[1]]) as usize;
        assert_eq!(body_len, framed.len() - 2);
        assert_eq!(body_len, 3 + 6 + payload.len());

        let decoded = decode_client_packet(&framed[2..], 65520).unwrap();
        assert_eq!(
            decoded,
            ClientPacket::Data {
                flags: 0,
                conid: 0x1234,
                orig_addr: addr,
                payload,
            }
        );
    }

    #[test]
    fn roundtrip_ipv6_packet() {
        let addr: SocketAddr = "[2001:4860:4860::8888]:53".parse().unwrap();
        let payload = b"dns-v6";
        let mtu = compute_udpgw_mtu(65520);

        let framed = encode_server_packet(0, 42, addr, payload, mtu).unwrap();
        let decoded = decode_client_packet(&framed[2..], 65520).unwrap();
        assert_eq!(
            decoded,
            ClientPacket::Data {
                flags: UDPGW_CLIENT_FLAG_IPV6,
                conid: 42,
                orig_addr: addr,
                payload,
            }
        );
    }

    #[test]
    fn decode_keepalive_ignores_payload() {
        let raw = [UDPGW_CLIENT_FLAG_KEEPALIVE, 0x05, 0x00, 0xAA, 0xBB];
        let decoded = decode_client_packet(&raw, 65520).unwrap();
        assert_eq!(decoded, ClientPacket::Keepalive { conid: 5 });
    }
}
