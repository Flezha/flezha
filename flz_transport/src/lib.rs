pub mod env;
pub mod service;
pub mod transport;
pub mod utility;

use std::fmt;

use transport::config::QuicListError;
use utility::socket_dir::SocketDirError;

/// Why the transport refused: the socket ladder or the QUIC candidate list.
/// Each arm renders exactly as the refusal it wraps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    SocketDir(SocketDirError),
    QuicList(QuicListError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::SocketDir(refusal) => refusal.fmt(f),
            Error::QuicList(refusal) => refusal.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl From<SocketDirError> for Error {
    fn from(refusal: SocketDirError) -> Self {
        Error::SocketDir(refusal)
    }
}

impl From<QuicListError> for Error {
    fn from(refusal: QuicListError) -> Self {
        Error::QuicList(refusal)
    }
}
