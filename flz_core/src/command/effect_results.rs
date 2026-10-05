//! `EffectResults`: the effect results a bind ships, kept for the instance's
//! re-runs before its first tasklet runs.

use super::{CommandAction, Reply};
use crate::pb::EffectResults;
use crate::runner::RunnerNode;

impl CommandAction for EffectResults {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<crate::Error>,
    ) -> Reply {
        let count = crate::effect_result::landed(self.results.clone());
        // A steps relay's gate counts them with its Cell restores.
        if let Some(instance) = runner.get_relay(&self.relay_id) {
            crate::steps::results_landed(&instance, count);
        }
        Reply {
            payload_id: "effect_results",
            bytes: Vec::new(),
        }
    }
}
