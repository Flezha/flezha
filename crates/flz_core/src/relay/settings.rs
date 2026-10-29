//! The one relay class's settings tree: how it runs and where its work is
//! kept. Every "one of" is an enum and every nested setting a member of its
//! parent, so a setting written under the wrong parent cannot be expressed.
//! A host declaration carries the same tree as JSON; [`parse_run`] and
//! [`parse_durable`] read it and name a misplaced setting with its parent.

use std::time::Duration;

use serde_json::{json, Map, Value};

use super::topology::{AckGranularity, BeyondWindowPolicy, CheckpointOutputs, CheckpointPolicy};

/// How long an idle durable steps instance waits before it sleeps when its
/// class says nothing. A flow instance whose class says nothing never sleeps.
pub const DEFAULT_SLEEP_AFTER: Duration = Duration::from_secs(6 * 60 * 60);

/// What a class promises, declared once in code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RelaySettings {
    pub description: &'static str,
    /// A wall-clock stop, for either run.
    pub max_duration: Option<Duration>,
    pub run: Run,
    pub durable: Durable,
}

/// How an instance runs: its commit point is an input or a checkpoint
/// (`Flow`), or a stage (`Steps`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Run {
    Flow { exactly: Exactly },
    Steps { each: Each, on_failure: OnFailure },
}

impl Default for Run {
    fn default() -> Self {
        Run::Flow {
            exactly: Exactly::AtLeastOnce,
        }
    }
}

impl Run {
    /// A steps run with `each` and the default failure mode.
    pub const fn steps(each: Each) -> Self {
        Run::Steps {
            each,
            on_failure: OnFailure::UndoAll,
        }
    }

    /// The run as the wire names it.
    pub fn as_str(&self) -> &'static str {
        match self {
            Run::Flow { .. } => "flow",
            Run::Steps { .. } => "steps",
        }
    }
}

/// A flow relay's duplicate rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Exactly {
    #[default]
    AtLeastOnce,
    /// The effectively-once tier: input log, effect record and outbox.
    EffectivelyOnce {
        ack: AckGranularity,
        /// The late-input correction window; `None` never corrects.
        correction_ms: Option<u64>,
        /// What an input later than the window becomes.
        beyond_window: BeyondWindowPolicy,
    },
}

impl Exactly {
    /// The tier with every member at its default.
    pub const fn effectively_once() -> Self {
        Exactly::EffectivelyOnce {
            ack: AckGranularity::PerInput,
            correction_ms: None,
            beyond_window: BeyondWindowPolicy::ProcessAsArrival,
        }
    }
}

/// How a steps relay repeats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Each {
    /// One pass over the first message, then done.
    #[default]
    Once,
    /// One iteration per message.
    PerInput,
    /// Every later pass seeded by the previous terminal handoff, ending on an
    /// empty batch.
    Chained,
}

impl Each {
    /// The name a class declares its loop shape by, on the wire and in a
    /// refusal.
    pub fn as_str(self) -> &'static str {
        match self {
            Each::Once => "once",
            Each::PerInput => "per_input",
            Each::Chained => "chained",
        }
    }

    /// Whether pass `iter` takes its seed from a message: every pass of a
    /// `PerInput` loop, the first pass of any loop.
    #[inline]
    pub fn message_fed(self, iter: u32) -> bool {
        match self {
            Each::PerInput => true,
            Each::Once | Each::Chained => iter == 0,
        }
    }

    /// Whether the instance ends after its first pass.
    #[inline]
    pub fn ends_after_first(self) -> bool {
        match self {
            Each::Once => true,
            Each::PerInput | Each::Chained => false,
        }
    }

    /// Whether a pass's leaf batch seeds the next pass, so a chain must end
    /// in one leaf that hands off.
    #[inline]
    pub fn carries_leaf(self) -> bool {
        match self {
            Each::Chained => true,
            Each::Once | Each::PerInput => false,
        }
    }

    /// Whether a tasklet may call another relay: only a turn per message can
    /// wait for its answer.
    #[inline]
    pub fn calls_allowed(self) -> bool {
        match self {
            Each::PerInput => true,
            Each::Once | Each::Chained => false,
        }
    }

    /// Whether the relay may run as several partitions, each its own loop.
    #[inline]
    pub fn partitionable(self) -> bool {
        match self {
            Each::PerInput => true,
            Each::Once | Each::Chained => false,
        }
    }
}

/// What a failed steps instance undoes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnFailure {
    /// Every completed tasklet is undone in reverse.
    #[default]
    UndoAll,
    /// Committed iterations are kept; only the in-flight one is undone. In a
    /// `PerInput` loop a failed turn is dead-lettered and the loop goes on.
    KeepProgress,
}

impl OnFailure {
    pub fn as_str(self) -> &'static str {
        match self {
            OnFailure::UndoAll => "undo_all",
            OnFailure::KeepProgress => "keep_progress",
        }
    }

    /// Whether a committed iteration stays done when the instance fails.
    #[inline]
    pub fn keeps_committed(self) -> bool {
        match self {
            OnFailure::KeepProgress => true,
            OnFailure::UndoAll => false,
        }
    }

    /// Whether a tasklet's failure ends only its turn, dead-lettered while
    /// the loop goes on, instead of the instance.
    #[inline]
    pub fn fails_turn(self, each: Each) -> bool {
        match self {
            OnFailure::KeepProgress => each == Each::PerInput,
            OnFailure::UndoAll => false,
        }
    }
}

/// How long an idle durable instance waits before it sleeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SleepAfter {
    /// The run's own: a flow instance never sleeps, a steps instance sleeps
    /// after [`DEFAULT_SLEEP_AFTER`]. A sleeping flow instance has its input
    /// journaled before it is forwarded, which a flow class pays only when it
    /// asks to sleep.
    #[default]
    RunDefault,
    Never,
    After(Duration),
}

impl SleepAfter {
    /// The interval for a class that runs in steps (`steps`) or as a flow;
    /// `None` never sleeps.
    pub fn interval(self, steps: bool) -> Option<Duration> {
        match self {
            SleepAfter::RunDefault => steps.then_some(DEFAULT_SLEEP_AFTER),
            SleepAfter::Never => None,
            SleepAfter::After(after) => Some(after),
        }
    }
}

/// How long a finished instance keeps its state: nothing by default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeepFinished {
    #[default]
    Nothing,
    /// The controller's window for kept runs (`keep_finished: true`).
    ControllerWindow,
    Hours(u64),
}

impl KeepFinished {
    /// The `keep_finished_h` metadata value; `None` when nothing is kept.
    pub fn metadata(self) -> Option<String> {
        match self {
            KeepFinished::Nothing => None,
            KeepFinished::ControllerWindow => Some("default".to_string()),
            KeepFinished::Hours(hours) => Some(hours.to_string()),
        }
    }
}

/// Where an instance's work is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durable {
    /// In memory only: a crash starts over.
    Memory,
    /// On the controller's disk.
    Local {
        sleep_after: SleepAfter,
        keep_finished: KeepFinished,
    },
    /// In the remote copy before the commit point is acknowledged.
    Remote {
        sleep_after: SleepAfter,
        keep_finished: KeepFinished,
    },
}

impl Default for Durable {
    fn default() -> Self {
        Durable::local()
    }
}

impl Durable {
    /// `Local`, sleeping after the run's default.
    pub const fn local() -> Self {
        Durable::Local {
            sleep_after: SleepAfter::RunDefault,
            keep_finished: KeepFinished::Nothing,
        }
    }

    /// `Remote`, sleeping after the run's default.
    pub const fn remote() -> Self {
        Durable::Remote {
            sleep_after: SleepAfter::RunDefault,
            keep_finished: KeepFinished::Nothing,
        }
    }

    /// Whether anything is kept on disk.
    pub fn keeps(&self) -> bool {
        !matches!(self, Durable::Memory)
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, Durable::Remote { .. })
    }

    /// The level's `sleep_after` as declared; `None` for `Memory`.
    pub fn declared_sleep_after(&self) -> Option<SleepAfter> {
        match self {
            Durable::Memory => None,
            Durable::Local { sleep_after, .. } | Durable::Remote { sleep_after, .. } => {
                Some(*sleep_after)
            }
        }
    }

    /// How long a finished instance keeps its state; `Nothing` for `Memory`.
    pub fn keep_finished(&self) -> KeepFinished {
        match self {
            Durable::Memory => KeepFinished::Nothing,
            Durable::Local { keep_finished, .. } | Durable::Remote { keep_finished, .. } => {
                *keep_finished
            }
        }
    }

    /// How long an idle instance of a class that runs in steps (`steps`) or
    /// as a flow waits before it sleeps; `None` never.
    pub fn sleep_after(&self, steps: bool) -> Option<Duration> {
        self.declared_sleep_after()?.interval(steps)
    }

    /// The level as the wire names it.
    pub fn as_str(&self) -> &'static str {
        match self {
            Durable::Memory => "memory",
            Durable::Local { .. } => "local",
            Durable::Remote { .. } => "remote",
        }
    }
}

/// Why a declared tree cannot be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingIssue {
    /// `fields` written under `declaration`, which does not carry them.
    UnknownField { declaration: String, fields: String },
    /// `setting` holds `value`, which is not `expected`.
    InvalidSetting {
        setting: String,
        value: String,
        expected: String,
    },
}

type Parsed<T> = Result<T, SettingIssue>;

/// A branch as the tree writes it: a bare name, or a one-key object whose
/// value holds the branch's members.
fn branch(setting: &str, value: &Value, names: &[&str]) -> Parsed<(String, Map<String, Value>)> {
    let expected = || names.join(" | ");
    let invalid = |shown: String| SettingIssue::InvalidSetting {
        setting: setting.to_string(),
        value: shown,
        expected: expected(),
    };
    let (name, members) = match value {
        Value::String(name) => (name.clone(), Map::new()),
        Value::Object(map) if map.len() == 1 => {
            let (name, members) = map.iter().next().expect("one key");
            let members = match members {
                Value::Object(members) => members.clone(),
                Value::Null => Map::new(),
                other => return Err(invalid(format!("{{{name}: {other}}}"))),
            };
            (name.clone(), members)
        }
        other => return Err(invalid(other.to_string())),
    };
    if names.contains(&name.as_str()) {
        Ok((name, members))
    } else {
        Err(invalid(name))
    }
}

/// Refuse any member of `declaration` outside `known`.
fn only(declaration: &str, members: &Map<String, Value>, known: &[&str]) -> Parsed<()> {
    let unknown: Vec<&str> = members
        .keys()
        .map(String::as_str)
        .filter(|key| !known.contains(key))
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    Err(SettingIssue::UnknownField {
        declaration: declaration.to_string(),
        fields: unknown.join(", "),
    })
}

fn named<T: Copy>(
    setting: &str,
    value: Option<&Value>,
    choices: &[(&str, T)],
    default: T,
) -> Parsed<T> {
    let Some(value) = value else {
        return Ok(default);
    };
    let names: Vec<&str> = choices.iter().map(|(name, _)| *name).collect();
    let (name, members) = branch(setting, value, &names)?;
    only(&name, &members, &[])?;
    Ok(choices
        .iter()
        .find(|(choice, _)| *choice == name)
        .map(|(_, v)| *v)
        .expect("branch checked the name"))
}

/// A non-negative integer member; `None` when absent.
fn integer(setting: &str, value: Option<&Value>) -> Parsed<Option<u64>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) if n.as_u64().is_some() => Ok(n.as_u64()),
        Some(other) => Err(SettingIssue::InvalidSetting {
            setting: setting.to_string(),
            value: other.to_string(),
            expected: "a non-negative integer".to_string(),
        }),
    }
}

/// `run` as the tree writes it; absent is `flow`.
pub fn parse_run(value: Option<&Value>) -> Parsed<Run> {
    let Some(value) = value else {
        return Ok(Run::default());
    };
    let (name, members) = branch("run", value, &["flow", "steps"])?;
    if name == "flow" {
        only("flow", &members, &["exactly"])?;
        return Ok(Run::Flow {
            exactly: parse_exactly(members.get("exactly"))?,
        });
    }
    only("steps", &members, &["each", "on_failure"])?;
    Ok(Run::Steps {
        each: named(
            "each",
            members.get("each"),
            &[
                ("once", Each::Once),
                ("per_input", Each::PerInput),
                ("chained", Each::Chained),
            ],
            Each::Once,
        )?,
        on_failure: named(
            "on_failure",
            members.get("on_failure"),
            &[
                ("undo_all", OnFailure::UndoAll),
                ("keep_progress", OnFailure::KeepProgress),
            ],
            OnFailure::UndoAll,
        )?,
    })
}

fn parse_exactly(value: Option<&Value>) -> Parsed<Exactly> {
    let Some(value) = value else {
        return Ok(Exactly::AtLeastOnce);
    };
    let (name, members) = branch("exactly", value, &["at_least_once", "effectively_once"])?;
    if name == "at_least_once" {
        only("at_least_once", &members, &[])?;
        return Ok(Exactly::AtLeastOnce);
    }
    only(
        "effectively_once",
        &members,
        &["ack", "correction_ms", "beyond_window"],
    )?;
    Ok(Exactly::EffectivelyOnce {
        ack: parse_ack(members.get("ack"))?,
        correction_ms: integer("correction_ms", members.get("correction_ms"))?,
        beyond_window: named(
            "beyond_window",
            members.get("beyond_window"),
            &[
                ("process_as_arrival", BeyondWindowPolicy::ProcessAsArrival),
                ("drop", BeyondWindowPolicy::Drop),
            ],
            BeyondWindowPolicy::ProcessAsArrival,
        )?,
    })
}

fn parse_ack(value: Option<&Value>) -> Parsed<AckGranularity> {
    let Some(value) = value else {
        return Ok(AckGranularity::PerInput);
    };
    let (name, members) = branch("ack", value, &["per_input", "checkpoint"])?;
    if name == "per_input" {
        only("per_input", &members, &[])?;
        return Ok(AckGranularity::PerInput);
    }
    only(
        "checkpoint",
        &members,
        &["every", "max_delay_ms", "outputs"],
    )?;
    let every = integer("every", members.get("every"))?;
    Ok(AckGranularity::Checkpoint(CheckpointPolicy {
        every: every.map(|n| u32::try_from(n).unwrap_or(u32::MAX)),
        max_delay_ms: integer("max_delay_ms", members.get("max_delay_ms"))?,
        outputs: named(
            "outputs",
            members.get("outputs"),
            &[
                ("as_produced", CheckpointOutputs::AsProduced),
                ("at_checkpoint", CheckpointOutputs::AtCheckpoint),
            ],
            CheckpointOutputs::AsProduced,
        )?,
    }))
}

/// `durable` as the tree writes it: `false`, a level, or a level with its
/// members; absent is `local`, and an absent `sleep_after` the run's default.
pub fn parse_durable(value: Option<&Value>) -> Parsed<Durable> {
    let Some(value) = value else {
        return Ok(Durable::default());
    };
    if value == &Value::Bool(false) {
        return Ok(Durable::Memory);
    }
    // `{"false": {...}}` is `false` written with members, which it has none of.
    let (name, members) = branch("durable", value, &["false", "local", "remote"])?;
    if name == "false" {
        only("false", &members, &[])?;
        return Ok(Durable::Memory);
    }
    only(&name, &members, &["sleep_after", "keep_finished"])?;
    let sleep_after = match members.get("sleep_after") {
        None => SleepAfter::RunDefault,
        Some(Value::Bool(false)) => SleepAfter::Never,
        Some(Value::Number(n)) if n.as_u64().is_some_and(|ms| ms > 0) => {
            SleepAfter::After(Duration::from_millis(n.as_u64().expect("checked")))
        }
        Some(other) => {
            return Err(SettingIssue::InvalidSetting {
                setting: "sleep_after".to_string(),
                value: other.to_string(),
                expected: "a positive number of milliseconds or false".to_string(),
            })
        }
    };
    let keep_finished = parse_keep_finished(members.get("keep_finished"))?;
    Ok(match name.as_str() {
        "local" => Durable::Local {
            sleep_after,
            keep_finished,
        },
        _ => Durable::Remote {
            sleep_after,
            keep_finished,
        },
    })
}

/// `durable.<level>.keep_finished`: hours, `true` for the controller's window,
/// or `false` (the default) for nothing.
fn parse_keep_finished(value: Option<&Value>) -> Parsed<KeepFinished> {
    match value {
        None | Some(Value::Bool(false)) => Ok(KeepFinished::Nothing),
        Some(Value::Bool(true)) => Ok(KeepFinished::ControllerWindow),
        Some(Value::Number(n)) if n.as_u64().is_some_and(|hours| hours > 0) => {
            Ok(KeepFinished::Hours(n.as_u64().expect("checked")))
        }
        Some(other) => Err(SettingIssue::InvalidSetting {
            setting: "keep_finished".to_string(),
            value: other.to_string(),
            expected: "a positive whole number of hours, true or false".to_string(),
        }),
    }
}

/// `run` as a host declaration writes it.
pub fn run_json(run: &Run) -> Value {
    match run {
        Run::Flow { exactly } => json!({ "flow": { "exactly": exactly_json(exactly) } }),
        Run::Steps { each, on_failure } => json!({
            "steps": { "each": each.as_str(), "on_failure": on_failure.as_str() }
        }),
    }
}

fn exactly_json(exactly: &Exactly) -> Value {
    let Exactly::EffectivelyOnce {
        ack,
        correction_ms,
        beyond_window,
    } = exactly
    else {
        return json!("at_least_once");
    };
    let ack = match ack {
        AckGranularity::PerInput => json!("per_input"),
        AckGranularity::Checkpoint(policy) => json!({ "checkpoint": {
            "every": policy.every,
            "max_delay_ms": policy.max_delay_ms,
            "outputs": match policy.outputs {
                CheckpointOutputs::AsProduced => "as_produced",
                CheckpointOutputs::AtCheckpoint => "at_checkpoint",
            },
        }}),
    };
    let beyond_window = match beyond_window {
        BeyondWindowPolicy::ProcessAsArrival => "process_as_arrival",
        BeyondWindowPolicy::Drop => "drop",
    };
    json!({ "effectively_once": {
        "ack": ack,
        "correction_ms": correction_ms,
        "beyond_window": beyond_window,
    }})
}

/// `durable` as a host declaration writes it; the run's default leaves
/// `sleep_after` out, and keeping nothing leaves `keep_finished` out.
pub fn durable_json(durable: &Durable) -> Value {
    let Some(sleep_after) = durable.declared_sleep_after() else {
        return Value::Bool(false);
    };
    let mut members = Map::new();
    match sleep_after {
        SleepAfter::RunDefault => {}
        SleepAfter::Never => {
            members.insert("sleep_after".into(), json!(false));
        }
        SleepAfter::After(after) => {
            members.insert("sleep_after".into(), json!(after.as_millis() as u64));
        }
    }
    match durable.keep_finished() {
        KeepFinished::Nothing => {}
        KeepFinished::ControllerWindow => {
            members.insert("keep_finished".into(), json!(true));
        }
        KeepFinished::Hours(hours) => {
            members.insert("keep_finished".into(), json!(hours));
        }
    }
    if members.is_empty() {
        json!(durable.as_str())
    } else {
        json!({ durable.as_str(): members })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_rules_per_variant() {
        // (variant, message_fed(0), message_fed(1), ends_after_first,
        //  carries_leaf, calls_allowed, partitionable)
        let table = [
            (Each::Once, true, false, true, false, false, false),
            (Each::PerInput, true, true, false, false, true, true),
            (Each::Chained, true, false, false, true, false, false),
        ];
        for (each, first, later, ends, carries, calls, partitions) in table {
            assert_eq!(each.message_fed(0), first, "{each:?}");
            assert_eq!(each.message_fed(1), later, "{each:?}");
            assert_eq!(each.ends_after_first(), ends, "{each:?}");
            assert_eq!(each.carries_leaf(), carries, "{each:?}");
            assert_eq!(each.calls_allowed(), calls, "{each:?}");
            assert_eq!(each.partitionable(), partitions, "{each:?}");
        }
    }

    #[test]
    fn keep_finished_reads_every_spelling_and_writes_it_back() {
        let table = [
            (json!("local"), KeepFinished::Nothing),
            (
                json!({"local": {"keep_finished": false}}),
                KeepFinished::Nothing,
            ),
            (
                json!({"local": {"keep_finished": true}}),
                KeepFinished::ControllerWindow,
            ),
            (
                json!({"remote": {"keep_finished": 24, "sleep_after": 2000}}),
                KeepFinished::Hours(24),
            ),
        ];
        for (tree, kept) in table {
            let durable = parse_durable(Some(&tree)).expect("a valid tree");
            assert_eq!(durable.keep_finished(), kept, "{tree}");
            assert_eq!(
                parse_durable(Some(&durable_json(&durable))),
                Ok(durable),
                "{tree}"
            );
        }
        assert_eq!(KeepFinished::Nothing.metadata(), None);
        assert_eq!(
            KeepFinished::ControllerWindow.metadata().as_deref(),
            Some("default")
        );
        assert_eq!(KeepFinished::Hours(6).metadata().as_deref(), Some("6"));
    }

    #[test]
    fn keep_finished_refuses_by_name() {
        for value in [json!(0), json!(-1), json!(1.5), json!("24")] {
            let tree = json!({ "local": { "keep_finished": value } });
            match parse_durable(Some(&tree)) {
                Err(SettingIssue::InvalidSetting { setting, .. }) => {
                    assert_eq!(setting, "keep_finished", "{tree}")
                }
                other => panic!("{tree}: {other:?}"),
            }
        }
        match parse_durable(Some(&json!({ "false": { "keep_finished": 24 } }))) {
            Err(SettingIssue::UnknownField { fields, .. }) => assert_eq!(fields, "keep_finished"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn on_failure_rules_per_variant() {
        // (variant, keeps_committed, fails_turn for Once, PerInput, Chained)
        let table = [
            (OnFailure::UndoAll, false, [false, false, false]),
            (OnFailure::KeepProgress, true, [false, true, false]),
        ];
        for (mode, keeps, turns) in table {
            assert_eq!(mode.keeps_committed(), keeps, "{mode:?}");
            for (each, turn) in [Each::Once, Each::PerInput, Each::Chained]
                .into_iter()
                .zip(turns)
            {
                assert_eq!(mode.fails_turn(each), turn, "{mode:?} {each:?}");
            }
        }
    }
}
