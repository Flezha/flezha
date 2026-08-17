//! PEM loading shared by every TLS endpoint: the QUIC client here and the
//! controller-side QUIC server in `flz_nif` — one definition of "read a cert
//! chain / private key / trust anchor off disk", so the loaders cannot drift.

use std::io;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::RootCertStore;

/// Read a PEM certificate chain. Errors on unreadable/undecodable input;
/// an empty chain is the caller's policy question, not an IO error.
pub fn load_certs(path: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    let pem = std::fs::read(path)?;
    let mut reader = io::BufReader::new(&pem[..]);

    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Read the first PEM private key.
pub fn load_private_key(path: &str) -> io::Result<PrivateKeyDer<'static>> {
    let pem = std::fs::read(path)?;
    let mut reader = io::BufReader::new(&pem[..]);

    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("no private key found in {}", path),
            )
        })
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
