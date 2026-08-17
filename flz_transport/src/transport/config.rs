use std::env;

#[derive(Debug, Clone, PartialEq)]
pub enum TransportType {
    Uds,
    Quic,
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
