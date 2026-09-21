//! The C ABI's one refusal type. Every variant's `Display` is the text the
//! entry point wrote to `flz_core_last_error` before it existed, byte for
//! byte: the veneer suites assert substrings of that string, and PHP reads it
//! as a C string with no Rust in between.

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The kernel's refusal, rendered exactly as the kernel renders it.
    #[error(transparent)]
    Core(#[from] flz_core::Error),
    /// An `ActivationResult` the host encoded wrongly.
    #[error("activation result failed to decode: {0}")]
    ResultDecode(postcard::Error),
    /// A completion or abandon for a token this context does not hold: the
    /// host answered twice, or answered one it never polled.
    #[error("unknown activation token {0}")]
    UnknownToken(u64),
    /// A C string argument that is not UTF-8, named (`namespace`,
    /// `instance id`).
    #[error("{0} is not valid UTF-8")]
    Utf8(&'static str),
    #[error("spawn arguments are not valid UTF-8")]
    SpawnArgsUtf8,
    #[error("if_running must be `deliver` or `error`, not `{0}`")]
    SpawnIfRunning(String),
    /// A spawn handle read after it resolved, or after the activation that
    /// made it completed.
    #[error("spawn handle {0} was released")]
    SpawnReleased(u64),
    #[error("context already started")]
    AlreadyStarted,
    /// A local-mode call on a context that holds no local run.
    #[error("no local run on this context: start one before feeding it")]
    NoLocalRun,
    #[error("controller connect not confirmed within 10s")]
    ConnectUnconfirmed,
    /// An administration call's refusal, rendered exactly as the kernel
    /// renders it: the controller's own words for a policy refusal, or the
    /// kernel's for a call that reached nothing.
    #[error(transparent)]
    Admin(#[from] flz_core::admin::AdminError),
    #[error("administration arguments are not valid UTF-8")]
    AdminArgsUtf8,
    /// A shim built against another activation schema, refused at creation.
    #[error("activation schema version mismatch: shim speaks {shim}, core speaks {core}")]
    SchemaMismatch { shim: u32, core: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;

    // One text per variant, as its producer renders it: the wrapping must add
    // nothing, because every veneer suite matches on the text alone.
    #[test]
    fn every_variant_renders_the_text_it_replaced() {
        let declaration = "a host class declares at least one tasklet";
        let truncated = flz_core::schema::decode_result(&[]).expect_err("no bytes");

        let rendered: Vec<(Error, String)> = vec![
            (
                Error::from(flz_core::Error::Declaration(declaration.into())),
                declaration.into(),
            ),
            (
                Error::ResultDecode(truncated),
                "activation result failed to decode: Hit the end of buffer, expected more data"
                    .into(),
            ),
            (Error::UnknownToken(7), "unknown activation token 7".into()),
            (
                Error::Utf8("namespace"),
                "namespace is not valid UTF-8".into(),
            ),
            (
                Error::Utf8("instance id"),
                "instance id is not valid UTF-8".into(),
            ),
            (
                Error::SpawnArgsUtf8,
                "spawn arguments are not valid UTF-8".into(),
            ),
            (
                Error::SpawnIfRunning("later".into()),
                "if_running must be `deliver` or `error`, not `later`".into(),
            ),
            (
                Error::SpawnReleased(3),
                "spawn handle 3 was released".into(),
            ),
            (Error::AlreadyStarted, "context already started".into()),
            (
                Error::ConnectUnconfirmed,
                "controller connect not confirmed within 10s".into(),
            ),
            (
                Error::from(flz_core::admin::AdminError::Refused(
                    "forbidden: role_insufficient".into(),
                )),
                "forbidden: role_insufficient".into(),
            ),
            (
                Error::AdminArgsUtf8,
                "administration arguments are not valid UTF-8".into(),
            ),
            (
                Error::SchemaMismatch { shim: 2, core: 3 },
                "activation schema version mismatch: shim speaks 2, core speaks 3".into(),
            ),
        ];
        for (error, text) in rendered {
            assert_eq!(error.to_string(), text, "{error:?}");
        }
    }
}
