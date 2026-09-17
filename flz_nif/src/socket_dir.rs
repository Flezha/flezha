//! Where this controller's sockets are: the resolved directory, the QUIC
//! candidate list and the socket file names, all asked of `flz_transport` so the
//! controller and every runner read one answer.

use std::path::Path;

use flz_transport::transport::config as transport_config;
use flz_transport::utility::socket_dir::{self, Role};
use rustler::{Encoder, Env, NifResult, Term};

use crate::atoms;

/// Where this controller's sockets are: the resolved absolute socket directory,
/// creating the first ladder candidate it can, and the QUIC candidate list, both
/// from `flz_transport`, which every runner asks too, so the ladder exists once.
///
/// `socket_dir` and `quic_addresses` carry the CALLER's reading of the two
/// variables: the BEAM keeps its own environment table, so a value set with
/// `System.put_env/2` is invisible to `std::env::var` here. Passing them keeps
/// the rule in one place while letting the caller's environment win.
#[rustler::nif(schedule = "DirtyIo")]
fn resolve_connect<'a>(
    env: Env<'a>,
    socket_dir: Option<String>,
    quic_addresses: Option<String>,
) -> NifResult<Term<'a>> {
    let resolved_dir = match socket_dir::resolve_socket_dir(Role::Controller, socket_dir.as_deref())
    {
        Ok(resolved) => resolved.path.display().to_string(),
        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
    };

    let entries = match transport_config::quic_addresses_with(quic_addresses.as_deref()) {
        Ok(entries) => entries.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
    };

    Ok((atoms::ok(), resolved_dir, entries).encode(env))
}

/// A runner's data-socket path in `socket_dir` — `flz_transport`'s own
/// composition, the one every runner reaches too. The controller asks for it
/// here rather than spelling it, so one runner cannot end up with two names;
/// the runner-id field is bounded there and a path a Unix domain socket address
/// cannot hold is refused by name and length.
#[rustler::nif]
fn stream_socket_path(
    socket_dir: String,
    runner_id: String,
    stream_type: String,
) -> Result<String, String> {
    socket_dir::stream_socket_path_in(Path::new(&socket_dir), &runner_id, &stream_type)
        .map_err(|refusal| refusal.to_string())
}

/// The control-socket path in `socket_dir` — the composer beside
/// `stream_socket_path/3`, so the controller that listens and every runner that
/// dials read one file name from `flz_transport` rather than each spelling it.
#[rustler::nif]
fn control_socket_path_in(socket_dir: String) -> Result<String, String> {
    socket_dir::control_socket_path_in(Path::new(&socket_dir))
        .map_err(|refusal| refusal.to_string())
}

/// The runner-id ceiling a data-socket name applies. Read by the controller so
/// the id it MINTS fits the socket it will open, rather than being shortened
/// into a name the controller never reports.
#[rustler::nif]
fn stream_socket_id_max() -> usize {
    socket_dir::stream_socket_id_max()
}
