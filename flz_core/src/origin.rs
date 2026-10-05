//! Where a tasklet's call came from, which names what it asks for across a
//! re-run of the invocation: the Cell, the input-log offset the invocation was
//! taking, and the call's own place in it. A crash re-run of an input that was
//! not durably consumed asks again with the same origin, so the controller
//! answers it with what the first run's call left. Spawn and effect both name
//! their requests from it.

use crate::relay::instance::{fnv1a128, FNV128_OFFSET_BASIS};

/// The invocation a call was made in: the requesting Cell and the input-log
/// offset it was taking, `None` when it took none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvocationOrigin<'a> {
    pub cell_identity: &'a [u8],
    pub offset: Option<u64>,
}

/// Which hook an effect ran in, so an undo's call never takes the run's
/// result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Run,
    Compensate,
}

impl Phase {
    fn byte(self) -> u8 {
        match self {
            Phase::Run => 0,
            Phase::Compensate => 1,
        }
    }
}

impl InvocationOrigin<'_> {
    /// A spawn's identity: the origin, the call's position among the
    /// invocation's spawns, the factory and the payload, which is what makes a
    /// re-run that asks for something else a new request.
    pub fn spawn_identity(&self, position: u32, factory_id: &str, payload: &[u8]) -> String {
        self.identity(&[&position.to_be_bytes(), factory_id.as_bytes(), payload])
    }

    /// An effect's identity: the origin, the phase, the name and how many
    /// earlier effects of that name the invocation made at this offset. An
    /// unrelated effect added before this one leaves it unchanged.
    pub fn effect_identity(&self, phase: Phase, name: &str, ordinal: u32) -> String {
        self.identity(&[
            b"effect",
            &[phase.byte()],
            name.as_bytes(),
            &ordinal.to_be_bytes(),
        ])
    }

    /// FNV-128 over the length-prefixed Cell, offset and `parts`. Empty for an
    /// invocation that took no input-log offset: nothing re-runs it, so
    /// nothing it asks for is named.
    fn identity(&self, parts: &[&[u8]]) -> String {
        let Some(offset) = self.offset else {
            return String::new();
        };
        let mut hash = FNV128_OFFSET_BASIS;
        for part in [self.cell_identity, &offset.to_be_bytes()]
            .into_iter()
            .chain(parts.iter().copied())
        {
            hash = fnv1a128(hash, &(part.len() as u64).to_be_bytes());
            hash = fnv1a128(hash, part);
        }
        format!("{hash:032x}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn origin(offset: Option<u64>) -> InvocationOrigin<'static> {
        InvocationOrigin {
            cell_identity: b"cell-a",
            offset,
        }
    }

    /// A re-run asks again with the same identity; anything that makes it a
    /// different request makes it a different identity.
    #[test]
    fn a_spawn_origin_names_its_request_and_nothing_else() {
        let first = origin(Some(7)).spawn_identity(0, "worker", b"p");
        assert_eq!(
            first, "ea4e9a44a7709f773c348f9751fccf29",
            "every core derives this"
        );
        assert_eq!(first, origin(Some(7)).spawn_identity(0, "worker", b"p"));
        for other in [
            origin(Some(8)).spawn_identity(0, "worker", b"p"),
            origin(Some(7)).spawn_identity(1, "worker", b"p"),
            origin(Some(7)).spawn_identity(0, "other", b"p"),
            origin(Some(7)).spawn_identity(0, "worker", b"q"),
        ] {
            assert_ne!(first, other);
        }
    }

    /// Nothing re-runs an invocation that took no offset, and two of its
    /// inputs with one payload are two requests.
    #[test]
    fn an_origin_without_an_offset_names_nothing() {
        assert_eq!(origin(None).spawn_identity(0, "worker", b"p"), "");
        assert_eq!(origin(None).effect_identity(Phase::Run, "model", 0), "");
    }

    #[test]
    fn an_effect_is_named_by_its_origin_phase_name_and_ordinal() {
        let model = origin(Some(7)).effect_identity(Phase::Run, "model", 0);
        assert_eq!(
            model, "8e020c11ce8c7e65014f0b83a666a054",
            "every core derives this"
        );
        assert_eq!(
            model,
            origin(Some(7)).effect_identity(Phase::Run, "model", 0)
        );
        for other in [
            origin(Some(7)).effect_identity(Phase::Run, "tool", 0),
            origin(Some(7)).effect_identity(Phase::Run, "model", 1),
            origin(Some(7)).effect_identity(Phase::Compensate, "model", 0),
            origin(Some(8)).effect_identity(Phase::Run, "model", 0),
            InvocationOrigin {
                cell_identity: b"cell-b",
                offset: Some(7),
            }
            .effect_identity(Phase::Run, "model", 0),
        ] {
            assert_ne!(model, other);
        }
    }

    /// The ordinal counts only earlier uses of the same name, so a call of
    /// another name made first leaves the identity where it was.
    #[test]
    fn an_added_effect_of_another_name_does_not_shift_an_identity() {
        let kept = origin(Some(7)).effect_identity(Phase::Run, "model", 0);
        let _lookup_first = origin(Some(7)).effect_identity(Phase::Run, "lookup", 0);
        assert_eq!(
            kept,
            origin(Some(7)).effect_identity(Phase::Run, "model", 0)
        );
    }

    /// An effect never takes a spawn's identity, whatever their parts.
    #[test]
    fn an_effect_and_a_spawn_never_share_an_identity() {
        assert_ne!(
            origin(Some(7)).effect_identity(Phase::Run, "worker", 0),
            origin(Some(7)).spawn_identity(0, "worker", b"")
        );
    }
}
