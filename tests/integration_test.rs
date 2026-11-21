use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::watch;

use badvpn_udpgw_rs::cli::Config;
use badvpn_udpgw_rs::client::handle_client;
use badvpn_udpgw_rs::dns::SystemDns;
use badvpn_udpgw_rs::port_pool::PortPool;
use badvpn_udpgw_rs::protocol::{
    compute_udpgw_mtu, decode_client_packet, encode_server_packet, ClientPacket,
    UDPGW_CLIENT_FLAG_KEEPALIVE, UDPGW_CLIENT_FLAG_REBIND,
};
use badvpn_udpgw_rs::bind_tcp_listener;

#[tokio::test]
async fn end_to_end_udp_echo_and_keepalive() {
    // 1. Start a local UDP echo server on an ephemeral port.
    let udp_echo = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let echo_addr = udp_echo.local_addr().unwrap();

    let echo_task = tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        for _ in 0..2 {
            let (n, peer) = udp_echo.recv_from(&mut buf).await.unwrap();
            udp_echo.send_to(&buf[..n], peer).await.unwrap();
        }
    });

    // 2. Start badvpn-udpgw TCP listener on an ephemeral port.
    let listener = bind_tcp_listener("127.0.0.1:0".parse().unwrap()).unwrap();
    let gw_addr = listener.local_addr().unwrap();

    let mut cfg = Config::default();
    cfg.max_clients = 16;
    cfg.max_connections_for_client = 16;
    let cfg = Arc::new(cfg);
    let dns = Arc::new(SystemDns::new());
    let port_pool = PortPool::new(None, None, false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let server_task = tokio::spawn(async move {
        let (stream, peer_addr) = listener.accept().await.unwrap();
        handle_client(stream, peer_addr, cfg, dns, port_pool, shutdown_rx).await;
    });

    // 3. Connect as a UDPGW client (like HTTP Custom / Dark Tunnel / badvpn-tun2socks).
    let mut client = TcpStream::connect(gw_addr).await.unwrap();
    let udpgw_mtu = compute_udpgw_mtu(65520);

    // 3a. Send a keepalive packet (must not disconnect or error).
    let keepalive_body = [UDPGW_CLIENT_FLAG_KEEPALIVE, 0x00, 0x00];
    let mut ka_frame = Vec::with_capacity(5);
    ka_frame.extend_from_slice(&(keepalive_body.len() as u16).to_le_bytes());
    ka_frame.extend_from_slice(&keepalive_body);
    client.write_all(&ka_frame).await.unwrap();

    // 3b. Send first UDP payload on conid = 7.
    let payload1 = b"ping-badvpn-rust-1";
    let frame1 = encode_server_packet(0, 7, echo_addr, payload1, udpgw_mtu).unwrap();
    client.write_all(&frame1).await.unwrap();

    let mut len_buf = [0u8; 2];
    client.read_exact(&mut len_buf).await.unwrap();
    let resp_len = u16::from_le_bytes(len_buf) as usize;
    let mut resp_body = vec![0u8; resp_len];
    client.read_exact(&mut resp_body).await.unwrap();

    let decoded1 = decode_client_packet(&resp_body, 65520).unwrap();
    assert_eq!(
        decoded1,
        ClientPacket::Data {
            flags: 0,
            conid: 7,
            orig_addr: echo_addr,
            payload: payload1,
        }
    );

    // 3c. Send second UDP payload with REBIND flag on same conid = 7.
    let payload2 = b"ping-badvpn-rust-rebind";
    let frame2 =
        encode_server_packet(UDPGW_CLIENT_FLAG_REBIND, 7, echo_addr, payload2, udpgw_mtu).unwrap();
    client.write_all(&frame2).await.unwrap();

    client.read_exact(&mut len_buf).await.unwrap();
    let resp_len2 = u16::from_le_bytes(len_buf) as usize;
    let mut resp_body2 = vec![0u8; resp_len2];
    client.read_exact(&mut resp_body2).await.unwrap();

    let decoded2 = decode_client_packet(&resp_body2, 65520).unwrap();
    assert_eq!(
        decoded2,
        ClientPacket::Data {
            flags: 0,
            conid: 7,
            orig_addr: echo_addr,
            payload: payload2,
        }
    );

    // 4. Clean shutdown.
    let _ = shutdown_tx.send(true);
    drop(client);
    echo_task.await.unwrap();
    server_task.await.unwrap();
}
