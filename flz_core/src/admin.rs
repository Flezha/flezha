//! Administering the namespace this runner's connection proved.
//!
//! One request shape for every verb, because the verb is data: [`call`] ships
//! an `AdminRequest` naming a function of the controller's published consumer
//! surface and blocks for the reply. Nothing in the request names a namespace,
//! a principal or a role. The controller fills all three from the connection,
//! so a host that adds a verb adds a name and no plumbing.
//!
//! Every SDK over this kernel reaches the surface here rather than through a
//! shape of its own: `sdk/rust` calls it directly and `flz_core_ffi` exposes
//! it as the one C ABI entry the other five hosts share.

use std::time::Duration;

use crate::pb;

/// How long an administration call waits for the controller's answer: the
/// deadline every request over [`crate::shutdown::ask`] waits out.
pub const ADMIN_TIMEOUT: Duration = crate::shutdown::REPLY_TIMEOUT;

/// Why an administration call produced no answer, as distinct from the
/// controller's own refusal, which arrives as [`Refused`](AdminError::Refused).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdminError {
    /// The controller judged the call and said no: policy, quota, a name that
    /// resolves to nothing, or the effect's own failure.
    #[error("{0}")]
    Refused(String),

    /// No control plane to ask on: a transport-less test or bench, or a
    /// runner that has closed.
    #[error("no control plane to administer over")]
    Unavailable,

    /// The request went out and nothing came back within [`ADMIN_TIMEOUT`].
    #[error("no answer within {}s", ADMIN_TIMEOUT.as_secs())]
    Unanswered,
}

/// Run `verb` with `args`, a JSON array of the call's arguments other than the
/// namespace. Answers the published function's own result as JSON.
pub fn call(verb: &str, args: &str) -> Result<String, AdminError> {
    let reference_id = format!("admin-{}-{}", verb, crate::util::nonce_suffix());
    let command = pb::command_request::Command::AdminRequest(pb::AdminRequest {
        verb: verb.to_string(),
        args: args.to_string(),
    });
    let rx =
        crate::shutdown::ask(reference_id.clone(), command).map_err(|_| AdminError::Unavailable)?;

    match rx.recv_timeout(ADMIN_TIMEOUT) {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(refusal)) => Err(AdminError::Refused(refusal)),
        Err(_) => {
            crate::shutdown::drop_pending_ack(&reference_id);
            Err(AdminError::Unanswered)
        }
    }
}
