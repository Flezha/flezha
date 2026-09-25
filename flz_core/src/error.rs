//! The crate's one refusal type, by the boundary that refused. Every
//! variant's `Display` is the text it replaced, byte for byte:
//! `CommandResponse.errors` carries these strings to the controller, which
//! matches token prefixes, and `flz_core_last_error` carries them to every
//! veneer suite, which asserts substrings.

use flz_transport::transport::config::QuicListError;
use flz_transport::utility::socket_dir::SocketDirError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A host class declaration the core cannot honour.
    #[error("{0}")]
    Declaration(String),
    /// A command activation the host did not answer as asked.
    #[error("{0}")]
    HostCrossing(String),
    /// A completion or abandon that does not fit its activation.
    #[error("{0}")]
    Answer(String),
    /// An edge value that could not reach its consumer.
    #[error("{0}")]
    Routing(String),
    /// A key this partition member does not own.
    #[error("{0}")]
    Partition(String),
    /// A state read or apply the operator cannot serve.
    #[error("{0}")]
    State(String),
    /// A control connection that could not be opened.
    #[error("{0}")]
    Connect(String),
    /// The transport's refusal: the socket ladder or the QUIC list.
    #[error(transparent)]
    Transport(#[from] flz_transport::Error),
    /// The per-tasklet `max_cells` cap is reached; the new-key input is
    /// dropped (`inputs_dropped_capped_total` bumped).
    #[error("max_cells reached: the new-key input is dropped")]
    Capped,
}

impl From<SocketDirError> for Error {
    fn from(refusal: SocketDirError) -> Self {
        Error::Transport(refusal.into())
    }
}

impl From<QuicListError> for Error {
    fn from(refusal: QuicListError) -> Self {
        Error::Transport(refusal.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // One representative text per variant, as its producer renders it, plus
    // the two `snapshot_frame` refusals the typed SDK produces from outside
    // this crate: the wrapping must add nothing, because the controller and
    // the veneer suites match on the text alone.
    #[test]
    fn every_variant_renders_the_text_it_replaced() {
        let declaration = "a host class declares at least one tasklet";
        let crossing = "snapshot_request: runner is shutting down";
        let answer = "completion carries more effects than items";
        let routing = "edge output names unknown channel 7";
        let partition = "key 'k' hashes to key group 3, which partition 0 of 2 does not own";
        let state = "cell_not_found: tasklet `p` holds no Cell keyed `k`";
        let connect = "Failed to connect to a QUIC controller — 127.0.0.1:4433: refused";
        let socket_dir = SocketDirError::Ambiguous { live: Vec::new() };
        let quic_list = QuicListError::NoEndpoint;

        let snapshot_state = "snapshot_frame: state type mismatch for tasklet";
        let snapshot_encode = "snapshot_frame: postcard state encode failed: bad";

        let rendered: Vec<(Error, String)> = vec![
            (Error::Declaration(declaration.into()), declaration.into()),
            (Error::State(snapshot_state.into()), snapshot_state.into()),
            (Error::State(snapshot_encode.into()), snapshot_encode.into()),
            (Error::HostCrossing(crossing.into()), crossing.into()),
            (Error::Answer(answer.into()), answer.into()),
            (Error::Routing(routing.into()), routing.into()),
            (Error::Partition(partition.into()), partition.into()),
            (Error::State(state.into()), state.into()),
            (Error::Connect(connect.into()), connect.into()),
            (Error::from(socket_dir.clone()), socket_dir.to_string()),
            (Error::from(quic_list.clone()), quic_list.to_string()),
            (
                Error::Capped,
                "max_cells reached: the new-key input is dropped".into(),
            ),
        ];
        for (error, text) in rendered {
            assert_eq!(error.to_string(), text, "{error:?}");
        }
    }
}
