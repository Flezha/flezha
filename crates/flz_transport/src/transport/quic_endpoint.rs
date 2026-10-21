//! The quinn-proto plumbing both ends of a connection share: the transport
//! settings, the timer, and the datagrams between the socket and the endpoint.

use std::io;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use mio::net::UdpSocket;
use quinn_proto::{ConnectionHandle, DatagramEvent, IdleTimeout, Transmit};

/// The transport settings both ends of a Flezha QUIC connection run: a 5 s
/// keep-alive and an idle timeout aligned to the control-liveness window, so
/// a wedged peer is declared dead in the same window on QUIC as on UDS
/// instead of inheriting the quinn default (keep-alive << idle, so a live
/// connection never idles out), plus the receive/send windows. The server
/// adds its stream limit on top.
pub fn transport_config() -> quinn_proto::TransportConfig {
    let mut transport = quinn_proto::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(5)));
    if let Ok(idle) = IdleTimeout::try_from(crate::transport::liveness::control_idle_timeout()) {
        transport.max_idle_timeout(Some(idle));
    }
    transport.stream_receive_window(8_388_608u32.into());
    transport.receive_window(16_777_216u32.into());
    transport.send_window(8_388_608);
    transport
}

/// How long to poll: until the connection's next timer, 100 ms at most.
pub(super) fn poll_timeout(connection: &mut quinn_proto::Connection) -> Duration {
    connection
        .poll_timeout()
        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
        .unwrap_or(Duration::from_millis(100))
}

/// Fire the connection's timer if it has come due.
pub fn expire_timer(connection: &mut quinn_proto::Connection, now: Instant) {
    if let Some(deadline) = connection.poll_timeout() {
        if now >= deadline {
            connection.handle_timeout(now);
        }
    }
}

/// Read every datagram the socket holds into the endpoint, feeding this
/// connection's events to it and answering stateless responses on the spot.
/// Shared by the handshake and the steady-state loop.
pub fn pump_udp(
    udp: &UdpSocket,
    udp_buf: &mut [u8],
    endpoint: &mut quinn_proto::Endpoint,
    conn_handle: ConnectionHandle,
    connection: &mut quinn_proto::Connection,
    now: Instant,
) -> io::Result<()> {
    loop {
        match udp.recv_from(udp_buf) {
            Ok((n, remote)) => {
                let data = BytesMut::from(&udp_buf[..n]);
                let mut resp_buf = Vec::new();
                if let Some(event) = endpoint.handle(now, remote, None, None, data, &mut resp_buf) {
                    match event {
                        DatagramEvent::ConnectionEvent(ch, event) => {
                            if ch == conn_handle {
                                connection.handle_event(event);
                            }
                        }
                        DatagramEvent::Response(transmit) => {
                            send_transmit(udp, &transmit, &resp_buf)?;
                        }
                        DatagramEvent::NewConnection(_) => {}
                    }
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

/// Pump endpoint events back and forth between endpoint and connection.
pub(super) fn pump_endpoint_events(
    endpoint: &mut quinn_proto::Endpoint,
    conn_handle: ConnectionHandle,
    connection: &mut quinn_proto::Connection,
) {
    while let Some(endpoint_event) = connection.poll_endpoint_events() {
        if let Some(conn_event) = endpoint.handle_event(conn_handle, endpoint_event) {
            connection.handle_event(conn_event);
        }
    }
}

/// Flush all pending transmits from the connection to the UDP socket.
pub fn flush_transmits(
    connection: &mut quinn_proto::Connection,
    udp: &UdpSocket,
    buf: &mut Vec<u8>,
) -> io::Result<()> {
    let now = Instant::now();
    while let Some(transmit) = connection.poll_transmit(now, 1, buf) {
        send_transmit(udp, &transmit, buf)?;
        buf.clear();
    }
    Ok(())
}

/// Send a single transmit (datagram) on the UDP socket.
pub fn send_transmit(udp: &UdpSocket, transmit: &Transmit, buf: &[u8]) -> io::Result<()> {
    let data = &buf[..transmit.size];
    match udp.send_to(data, transmit.destination) {
        Ok(_) => Ok(()),
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(()),
        Err(e) => Err(e),
    }
}
