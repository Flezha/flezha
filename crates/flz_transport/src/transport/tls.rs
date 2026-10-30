//! PEM loading and endpoint crypto shared by every TLS endpoint: the QUIC
//! client here and the controller-side QUIC server in `flz_nif` — one
//! definition of "read a cert chain / private key / trust anchor off disk" and
//! one of "assemble an endpoint config from them", so neither can drift.

use std::io;
use std::sync::Arc;

use rustls::pki_types::pem::{self, PemObject};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::RootCertStore;

/// Read a PEM certificate chain. Errors on unreadable/undecodable input;
/// an empty chain is the caller's policy question, not an IO error.
pub fn load_certs(path: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_file_iter(path)
        .and_then(Iterator::collect)
        .map_err(pem_to_io)
}

/// Read the first PEM private key.
pub fn load_private_key(path: &str) -> io::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path).map_err(|e| match e {
        pem::Error::NoItemsFound => io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no private key found in {}", path),
        ),
        e => pem_to_io(e),
    })
}

/// A read failure keeps its own kind; anything else is undecodable input.
fn pem_to_io(e: pem::Error) -> io::Error {
    match e {
        pem::Error::Io(e) => e,
        e => io::Error::new(io::ErrorKind::InvalidData, e),
    }
}

/// Read a PEM trust-anchor file into a `RootCertStore`. Errors when the file
/// yields no usable anchor — an empty store would silently trust nothing or,
/// worse, be treated as "verification off" by a careless caller.
pub fn load_root_store(path: &str) -> io::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();

    for cert in load_certs(path)? {
        roots
            .add(cert)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    }

    if roots.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no CA certificates found in {}", path),
        ));
    }

    Ok(roots)
}

/// Build a rustls ServerConfig and wrap it into a quinn-proto ServerConfig.
///
/// A non-empty `client_ca_path` makes the handshake require a client
/// certificate chaining to that trust anchor; an empty one is the explicit
/// wide-open opt-out the Elixir side warns about on every boot.
pub fn build_quic_server_config(
    cert_path: &str,
    key_path: &str,
    client_ca_path: &str,
) -> io::Result<quinn_proto::ServerConfig> {
    let certs = load_certs(cert_path)?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "No certificates found in cert file",
        ));
    }

    let key = load_private_key(key_path)?;

    let builder = rustls::ServerConfig::builder();
    let builder = if client_ca_path.is_empty() {
        builder.with_no_client_auth()
    } else {
        let roots = load_root_store(client_ca_path)?;
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        builder.with_client_cert_verifier(verifier)
    };
    let mut server_crypto = builder
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    server_crypto.alpn_protocols = vec![b"flezha".to_vec()];

    let quic_server_config = quinn_proto::crypto::rustls::QuicServerConfig::try_from(server_crypto)
        .map_err(io::Error::other)?;
    let mut server_config = quinn_proto::ServerConfig::with_crypto(Arc::new(quic_server_config));

    // The client's transport settings, plus enough bidi streams for control +
    // data streams.
    let mut transport = super::quic::transport_config();
    transport.max_concurrent_bidi_streams(128u32.into());
    server_config.transport_config(Arc::new(transport));

    Ok(server_config)
}

/// Load the client identity for mutual TLS. `Ok(None)` when the pair is not
/// on disk (the caller falls back to no client auth and the server's posture
/// decides); a present-but-unreadable pair is an error, not a silent
/// downgrade.
fn load_client_identity(
    cert_path: &str,
    key_path: &str,
) -> io::Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    if !std::path::Path::new(cert_path).exists() || !std::path::Path::new(key_path).exists() {
        return Ok(None);
    }

    let chain = load_certs(cert_path)?;
    if chain.is_empty() {
        return Ok(None);
    }

    Ok(Some((chain, load_private_key(key_path)?)))
}

/// The client's TLS posture, and the mirror of [`build_quic_server_config`]:
/// the CA the server is verified against, the client identity for mutual TLS
/// when its pair is on disk (the server requires it by default), and the
/// `flezha` ALPN.
pub(crate) fn build_quic_client_crypto(ca_cert_path: &str) -> io::Result<rustls::ClientConfig> {
    let root_store = load_root_store(ca_cert_path)?;
    let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

    let client_cert_path = super::config::quic_client_cert_path();
    let client_key_path = super::config::quic_client_key_path();

    let mut crypto = match load_client_identity(&client_cert_path, &client_key_path)? {
        Some((chain, key)) => builder
            .with_client_auth_cert(chain, key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        None => builder.with_no_client_auth(),
    };
    crypto.alpn_protocols = vec![b"flezha".to_vec()];
    Ok(crypto)
}
