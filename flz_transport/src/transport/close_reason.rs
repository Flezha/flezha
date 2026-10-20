//! Why a stream ended, as every Elixir-facing transport reports it: the
//! controller's server drivers and a runner's socket client.

use std::{fmt, io};

/// Why a stream ended, in the one vocabulary every transport reports through.
///
/// The same wire event is the same variant whichever transport observed it, so
/// what a controller's log says happened to a stream does not depend on which
/// transport it was configured with. A reason only one wire can produce is
/// still declared here rather than written as a literal where it is produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseReason {
    /// The peer ended the stream cleanly. Every transport produces it.
    ConnectionClosed,
    /// The peer reset the stream. QUIC only: a UDS peer has no way to say it.
    StreamReset,
    /// A read failed. Every transport produces it.
    ReadError,
    /// A write failed. Every transport produces it.
    WriteError,
    /// A frame declared a length above the configured maximum, so the byte
    /// stream cannot resync. Every transport produces it.
    FrameTooLarge,
    /// A frame's payload did not decode: an inner length overran it, or its
    /// metadata table was cut inside a pair. Every transport produces it.
    MalformedFrame,
    /// The whole connection died, carrying the wire's own account of it. QUIC
    /// only: a UDS connection IS its stream, so it closes rather than is lost.
    ConnectionLost(String),
}

impl From<io::ErrorKind> for CloseReason {
    /// How a byte-stream read failure reads as a close. The controller's server
    /// wire and a runner's client both answer through this, so a reason read
    /// from a log does not depend on which side of the connection produced it.
    fn from(kind: io::ErrorKind) -> Self {
        match kind {
            io::ErrorKind::UnexpectedEof => CloseReason::ConnectionClosed,
            io::ErrorKind::InvalidData => CloseReason::FrameTooLarge,
            _ => CloseReason::ReadError,
        }
    }
}

impl fmt::Display for CloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CloseReason::ConnectionClosed => f.write_str("connection_closed"),
            CloseReason::StreamReset => f.write_str("stream_reset"),
            CloseReason::ReadError => f.write_str("read_error"),
            CloseReason::WriteError => f.write_str("write_error"),
            CloseReason::FrameTooLarge => f.write_str("frame_too_large"),
            CloseReason::MalformedFrame => f.write_str("malformed_frame"),
            CloseReason::ConnectionLost(e) => write!(f, "connection_lost: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every reason is declared once and rendered once, including the two only one
    /// wire can produce. A word written as a literal at the site that produces it
    /// is what this set exists to prevent.
    #[test]
    fn the_close_vocabulary_is_one_declared_set() {
        let rendered: Vec<String> = [
            CloseReason::ConnectionClosed,
            CloseReason::StreamReset,
            CloseReason::ReadError,
            CloseReason::WriteError,
            CloseReason::FrameTooLarge,
            CloseReason::MalformedFrame,
            CloseReason::ConnectionLost("aborted by peer".into()),
        ]
        .iter()
        .map(CloseReason::to_string)
        .collect();

        assert_eq!(
            rendered,
            [
                "connection_closed",
                "stream_reset",
                "read_error",
                "write_error",
                "frame_too_large",
                "malformed_frame",
                "connection_lost: aborted by peer",
            ]
        );
    }
}
