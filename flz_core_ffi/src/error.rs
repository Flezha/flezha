//! The C ABI's one refusal type. Every variant answers a catalogue code, and
//! `flz_core_last_error` carries it as the JSON error document, so a veneer
//! branches on the code and never on the text.

use flz_core::{Code, ErrorModel, FlezhaError};

#[derive(Debug)]
pub enum Error {
    /// The kernel's refusal.
    Core(flz_core::Error),
    /// An `ActivationResult` the host encoded wrongly.
    ResultDecode(postcard::Error),
    /// A completion or abandon for a token this context does not hold: the
    /// host answered twice, or answered one it never polled.
    UnknownToken(u64),
    /// An argument that is not UTF-8, named (`namespace`, `spawn arguments`).
    Utf8(&'static str),
    SpawnIfRunning(String),
    /// A spawn handle read after it resolved, or after the activation that
    /// made it completed.
    SpawnReleased(u64),
    /// A spawn that reached no instance: the controller's refusal, or the
    /// core's own timeout or missing control plane.
    Spawn(flz_core::spawn::SpawnError),
    AlreadyStarted,
    /// A local-mode call on a context that holds no local run.
    NoLocalRun,
    ConnectUnconfirmed,
    /// An administration call's refusal: the controller's own for a policy
    /// refusal, or the kernel's for a call that reached nothing.
    Admin(flz_core::admin::AdminError),
    /// A `ctx.effect` the core refused or could not keep.
    Effect(FlezhaError),
    /// A shim built against another activation schema, refused at creation.
    SchemaMismatch {
        shim: u32,
        core: u32,
    },
}

/// How long `flz_core_start` waits for the runner's connect confirmation.
pub const CONNECT_CONFIRM_SECS: u64 = 10;

impl ErrorModel for Error {
    fn to_error(&self) -> FlezhaError {
        match self {
            Error::Core(error) => error.to_error(),
            Error::ResultDecode(e) => {
                Code::ResultUndecodable.with([("reason", e.to_string().into())])
            }
            Error::UnknownToken(token) => {
                Code::UnknownActivationToken.with([("token", (*token).into())])
            }
            Error::Utf8(argument) => Code::ArgumentNotUtf8.with([("argument", (*argument).into())]),
            Error::SpawnIfRunning(value) => {
                Code::InvalidIfRunning.with([("value", value.as_str().into())])
            }
            Error::SpawnReleased(handle) => {
                Code::SpawnHandleReleased.with([("handle", (*handle).into())])
            }
            Error::Spawn(error) => error.to_error(),
            Error::AlreadyStarted => Code::RunnerAlreadyRunning.with([]),
            Error::NoLocalRun => Code::NoLocalRun.with([]),
            Error::ConnectUnconfirmed => {
                Code::ConnectUnconfirmed.with([("seconds", CONNECT_CONFIRM_SECS.into())])
            }
            Error::Admin(error) => error.to_error(),
            Error::Effect(error) => error.clone(),
            Error::SchemaMismatch { shim, core } => {
                Code::SchemaMismatch.with([("shim", (*shim).into()), ("core", (*core).into())])
            }
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_error().message)
    }
}

impl std::error::Error for Error {}

impl From<flz_core::Error> for Error {
    fn from(error: flz_core::Error) -> Self {
        Error::Core(error)
    }
}

impl From<flz_core::admin::AdminError> for Error {
    fn from(error: flz_core::admin::AdminError) -> Self {
        Error::Admin(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Every variant answers a catalogued code, and its document is the one
    // the controller renders for the same code.
    #[test]
    fn every_variant_answers_a_catalogued_code() {
        let truncated = flz_core::schema::decode_result(&[]).expect_err("no bytes");
        let refused = FlezhaError::new("authz.role_insufficient", Default::default());

        let coded: Vec<(Error, &str)> = vec![
            (
                Error::from(flz_core::Error::Declaration(Code::NoTasklets.with([]))),
                "topology.no_tasklets",
            ),
            (Error::ResultDecode(truncated), "runtime.result_undecodable"),
            (Error::UnknownToken(7), "runtime.unknown_activation_token"),
            (Error::Utf8("namespace"), "runtime.argument_not_utf8"),
            (
                Error::SpawnIfRunning("later".into()),
                "runtime.invalid_if_running",
            ),
            (Error::SpawnReleased(3), "runtime.spawn_handle_released"),
            (
                Error::Spawn(flz_core::spawn::SpawnError::Timeout),
                "spawn.timeout",
            ),
            (Error::AlreadyStarted, "runtime.runner_already_running"),
            (Error::NoLocalRun, "runtime.no_local_run"),
            (Error::ConnectUnconfirmed, "runtime.connect_unconfirmed"),
            (
                Error::from(flz_core::admin::AdminError::Refused(refused)),
                "authz.role_insufficient",
            ),
            (
                Error::from(flz_core::admin::AdminError::Unanswered),
                "control.timeout",
            ),
            (
                Error::SchemaMismatch { shim: 2, core: 3 },
                "runtime.schema_mismatch",
            ),
        ];
        for (error, code) in coded {
            let document = FlezhaError::from_document(&error.to_error().json())
                .expect("a failure is a JSON error document");
            assert_eq!(document.code, code, "{error:?}");
            assert!(
                flz_transport::errors::entry(code).is_some(),
                "{code} is catalogued"
            );
        }
    }
}
