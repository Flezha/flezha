use std::env;
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq)]
pub enum TransportType {
    Uds,
    Quic,
}

/// Default maximum accepted wire-frame payload length (64 MiB) — far above any
/// legitimate frame, small enough that a hostile length prefix cannot force an
/// OOM. Override with `FLEZHA_MAX_FRAME_BYTES`.
const DEFAULT_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// Default per-stream send-buffer cap (8 MiB) — a few seconds of peak
/// per-stream throughput, so a slow peer cannot grow the buffered-write `Vec`
/// without bound. Override with `FLEZHA_MAX_WRITE_BUFFER_BYTES`.
const DEFAULT_MAX_WRITE_BUFFER_BYTES: usize = 8 * 1024 * 1024;

/// The bound every frame decoder enforces on a declared length prefix BEFORE
/// allocating for it. Cached after the first read: the check sits on the
/// per-frame hot path, so the knob is boot-time configuration.
pub fn max_frame_bytes() -> usize {
    static CACHE: OnceLock<usize> = OnceLock::new();
    *CACHE.get_or_init(|| {
        parse_positive_bytes(
            env::var("FLEZHA_MAX_FRAME_BYTES").ok().as_deref(),
            DEFAULT_MAX_FRAME_BYTES,
        )
    })
}

/// Per-stream send-buffer cap shared by the UDS and QUIC drivers.
pub fn max_write_buffer_bytes() -> usize {
    parse_positive_bytes(
        env::var("FLEZHA_MAX_WRITE_BUFFER_BYTES").ok().as_deref(),
        DEFAULT_MAX_WRITE_BUFFER_BYTES,
    )
}

fn parse_positive_bytes(raw: Option<&str>, default: usize) -> usize {
    raw.and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

pub fn get_transport_type() -> TransportType {
    match env::var("FLEZHA_TRANSPORT").as_deref() {
        Ok("quic") => TransportType::Quic,
        _ => TransportType::Uds,
    }
}

pub fn quic_address() -> String {
    env::var("FLEZHA_QUIC_ADDRESS").unwrap_or_else(|_| "127.0.0.1:4433".to_string())
}

pub fn ca_cert_path() -> String {
    env::var("FLEZHA_CA_CERT_PATH").unwrap_or_else(|_| "app_data/certs/ca.pem".to_string())
}

/// Client certificate presented to a QUIC server that requires peer
/// authentication. Defaults to the controller-generated pair so a local
/// runner sharing `app_data/` authenticates out of the box; remote runners
/// point these at operator-provisioned certs.
pub fn quic_client_cert_path() -> String {
    env::var("FLEZHA_QUIC_CLIENT_CERT").unwrap_or_else(|_| "app_data/certs/client.pem".to_string())
}

pub fn quic_client_key_path() -> String {
    env::var("FLEZHA_QUIC_CLIENT_KEY").unwrap_or_else(|_| "app_data/certs/client.key".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The pure parser is tested instead of the env accessors themselves: the
    // frame-limit accessor's OnceLock cache is process-global, so an
    // env-mutating test would race other tests in the same binary.
    #[test]
    fn byte_knob_override_honoured_and_default_otherwise() {
        assert_eq!(
            parse_positive_bytes(Some("1024"), DEFAULT_MAX_FRAME_BYTES),
            1024
        );
        assert_eq!(
            parse_positive_bytes(None, DEFAULT_MAX_FRAME_BYTES),
            DEFAULT_MAX_FRAME_BYTES
        );
        assert_eq!(
            parse_positive_bytes(Some("nonsense"), DEFAULT_MAX_FRAME_BYTES),
            DEFAULT_MAX_FRAME_BYTES
        );
        assert_eq!(
            parse_positive_bytes(Some("0"), DEFAULT_MAX_FRAME_BYTES),
            DEFAULT_MAX_FRAME_BYTES
        );
        assert_eq!(max_write_buffer_bytes(), DEFAULT_MAX_WRITE_BUFFER_BYTES);
    }
}
