//! The activation and result shapes, once: the field keys in both spellings,
//! the vocabulary a discriminator carries, and the two mappings written
//! against a language's own containers rather than an intermediate tree.
//!
//! A binding supplies [`Sink`] — how its language builds a map, a list and a
//! scalar — and [`Source`] — how it reads one back. Which key carries which
//! field, in what order, and every refusal a malformed answer earns live
//! here, so the bindings cannot drift.

use flz_core::schema;

use super::Refusal;

/// One field key in both spellings. A binding picks with [`Key::spell`] over
/// its own `CAMEL`, so the JavaScript spelling costs nothing at runtime and
/// no binding carries a second key table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Key {
    pub snake: &'static str,
    pub camel: &'static str,
}

impl Key {
    const fn of(snake: &'static str, camel: &'static str) -> Self {
        Self { snake, camel }
    }

    /// A key whose two spellings are the same word.
    const fn flat(name: &'static str) -> Self {
        Self::of(name, name)
    }

    pub fn spell(self, camel: bool) -> &'static str {
        if camel {
            self.camel
        } else {
            self.snake
        }
    }
}

// --- the field keys --------------------------------------------------------

pub const TOKEN: Key = Key::flat("token");
pub const OPERATIVE: Key = Key::flat("operative");
pub const PROCEDURE: Key = Key::flat("procedure");
pub const ENTITY_KEY: Key = Key::of("entity_key", "entityKey");
pub const CHANGE_SEQ: Key = Key::of("change_seq", "changeSeq");
pub const FACTORY: Key = Key::flat("factory");
pub const KIND: Key = Key::flat("kind");
pub const ITEMS: Key = Key::flat("items");
pub const SNAPSHOT: Key = Key::flat("snapshot");
pub const CHANGES: Key = Key::flat("changes");
pub const STATE_JSON: Key = Key::of("state_json", "stateJson");
pub const KEY_REQUEST: Key = Key::of("key_request", "keyRequest");
pub const ARGS: Key = Key::flat("args");
pub const OUTPUTS: Key = Key::flat("outputs");
pub const CHANNEL: Key = Key::flat("channel");
pub const PAYLOAD: Key = Key::flat("payload");
pub const METADATA: Key = Key::flat("metadata");
pub const CONSUMED_OFFSET: Key = Key::of("consumed_offset", "consumedOffset");
pub const REPLAY: Key = Key::flat("replay");
pub const NOW: Key = Key::flat("now");
pub const TIMER_KIND: Key = Key::of("timer_kind", "timerKind");
pub const SCHEDULED_AT: Key = Key::of("scheduled_at", "scheduledAt");
pub const BYTES: Key = Key::flat("bytes");
pub const FROM_VERSION: Key = Key::of("from_version", "fromVersion");
pub const PAYLOADS: Key = Key::flat("payloads");
pub const WANT_KEYS: Key = Key::of("want_keys", "wantKeys");
pub const WANT_EVENT_TIME: Key = Key::of("want_event_time", "wantEventTime");
pub const OUTCOME: Key = Key::flat("outcome");
pub const EFFECTS: Key = Key::flat("effects");
pub const STATE: Key = Key::flat("state");
pub const JSON: Key = Key::flat("json");
pub const PREPARED: Key = Key::flat("prepared");
pub const ERROR: Key = Key::flat("error");
pub const NON_RETRYABLE: Key = Key::of("non_retryable", "nonRetryable");
pub const WORKFLOW_COMPLETED: Key = Key::of("workflow_completed", "workflowCompleted");
pub const WORKFLOW_RESULT: Key = Key::of("workflow_result", "workflowResult");
pub const VERDICT: Key = Key::flat("verdict");
pub const MESSAGE: Key = Key::flat("message");
pub const EVENT_TIME: Key = Key::of("event_time", "eventTime");
pub const TIMERS: Key = Key::flat("timers");
pub const CHECKPOINT: Key = Key::flat("checkpoint");
pub const DLQ: Key = Key::flat("dlq");
pub const REGISTER: Key = Key::flat("register");
pub const ATTEMPTS: Key = Key::flat("attempts");
pub const TARGET: Key = Key::flat("target");
pub const CONSUMER: Key = Key::flat("consumer");
pub const PLUGIN_CHANNEL: Key = Key::of("plugin_channel", "pluginChannel");
pub const KEY: Key = Key::flat("key");
pub const KEYS: Key = Key::flat("keys");
pub const VALUE: Key = Key::flat("value");

/// Every key a record carries, in the order the mapping writes it. The Node
/// SDK's `native-types.test.mjs` is pinned on these, so a field added to a
/// record without a spelling here fails that suite rather than reaching
/// JavaScript unnamed.
pub mod record {
    use super::*;

    pub const ACTIVATION: &[Key] = &[
        TOKEN,
        OPERATIVE,
        PROCEDURE,
        ENTITY_KEY,
        CHANGE_SEQ,
        FACTORY,
        KIND,
        ITEMS,
        SNAPSHOT,
        CHANGES,
        STATE_JSON,
        super::KEY_REQUEST,
        ARGS,
        OUTPUTS,
    ];
    pub const WORK_ITEM: &[Key] = &[
        KIND,
        CHANNEL,
        PAYLOAD,
        METADATA,
        CONSUMED_OFFSET,
        REPLAY,
        NOW,
        TIMER_KIND,
        SCHEDULED_AT,
    ];
    pub const RESTORE_BODY: &[Key] = &[BYTES, FROM_VERSION];
    pub const KEY_REQUEST: &[Key] = &[CHANNEL, PAYLOADS, WANT_KEYS, WANT_EVENT_TIME];
    pub const META: &[Key] = &[KEY, VALUE];
    pub const RESULT: &[Key] = &[
        TOKEN,
        OUTCOME,
        EFFECTS,
        STATE,
        JSON,
        PREPARED,
        ERROR,
        NON_RETRYABLE,
        WORKFLOW_COMPLETED,
        WORKFLOW_RESULT,
    ];
    pub const ITEM_EFFECT: &[Key] = &[
        VERDICT,
        MESSAGE,
        CHANGES,
        OUTPUTS,
        CONSUMED_OFFSET,
        EVENT_TIME,
        TIMERS,
        CHECKPOINT,
        DLQ,
    ];
    pub const OUTPUT: &[Key] = &[
        TARGET,
        CHANNEL,
        CONSUMER,
        PLUGIN_CHANNEL,
        KEY,
        EVENT_TIME,
        METADATA,
        PAYLOAD,
    ];
    pub const TIMER_OP: &[Key] = &[KIND, SCHEDULED_AT, REGISTER];
    pub const DLQ_ENTRY: &[Key] = &[CHANNEL, ERROR, ATTEMPTS, PAYLOAD];
    pub const PREPARE_ANSWER: &[Key] = &[KEYS, EVENT_TIME, ERROR, NON_RETRYABLE];
}

/// The vocabulary a discriminator carries. Spelled the same in every
/// language: only field keys take a camelCase form.
pub mod word {
    pub const WORK: &str = "work";
    pub const INPUT: &str = "input";
    pub const TIMER: &str = "timer";
    pub const RESTORE_FOLD: &str = "restore_fold";
    pub const SNAPSHOT_REQUEST: &str = "snapshot_request";
    pub const LIVE_STATE_READ: &str = "live_state_read";
    pub const APPLY_STATE: &str = "apply_state";
    pub const MIGRATION_SERIALIZE: &str = "migration_serialize";
    pub const KEY_REQUEST: &str = "key_request";
    pub const WORKFLOW_RUN: &str = "workflow_run";
    pub const COMPENSATE_RUN: &str = "compensate_run";

    pub const EDGE: &str = "edge";
    pub const PLUGIN: &str = "plugin";
    pub const OK: &str = "ok";
    pub const RETRYABLE: &str = "retryable";
    pub const NON_RETRYABLE: &str = "non_retryable";
    pub const EVENT_TIME: &str = "event_time";
    pub const PROCESSING_TIME: &str = "processing_time";

    pub const RESTORED: &str = "restored";
    pub const SNAPSHOT: &str = "snapshot";
    pub const STATE_JSON: &str = "state_json";
    pub const STATE_APPLIED: &str = "state_applied";
    pub const MIGRATION_STATE: &str = "migration_state";
    pub const PREPARED: &str = "prepared";
    pub const FAILED: &str = "failed";
    pub const WORKFLOW_DONE: &str = "workflow_done";
}

/// What a refusal calls the record it was reading.
mod what {
    use super::Key;

    pub const AN_OUTPUT: &str = "an output";
    pub const AN_ITEM_EFFECT: &str = "an item effect";
    pub const A_TIMER_OP: &str = "a timer op";
    pub const A_DLQ_ENTRY: &str = "a dlq entry";
    pub const A_PREPARE_ANSWER: &str = "a prepare answer";
    pub const OUTPUT: &str = "output";
    pub const EDGE_OUTPUT: &str = "edge output";
    pub const PLUGIN_OUTPUT: &str = "plugin output";
    pub const ITEM_EFFECT: &str = "item effect";
    pub const TIMER_OP: &str = "timer op";
    pub const DLQ_ENTRY: &str = "dlq entry";
    pub const ACTIVATION_RESULT: &str = "activation result";
    pub const CHANGE: &str = "change";

    /// Where a numeric refusal names more than the bare field key. Only a
    /// binding that reports one — napi — renders these.
    pub const TIMER_SCHEDULED_AT: Key = Key::of("timer scheduled_at", "timer scheduledAt");
    pub const OUTPUT_EVENT_TIME: Key = Key::of("output event_time", "output eventTime");
    pub const PREPARE_EVENT_TIME: Key = Key::of("prepare event_time", "prepare eventTime");
}

// --- the two halves a binding supplies -------------------------------------

/// How a language builds the value an activation crosses as.
pub trait Sink {
    type Value;
    type Map;
    type List;
    type Error: From<Refusal>;

    /// Whether this language spells a field key in camelCase.
    const CAMEL: bool = false;

    fn map(&self) -> Result<Self::Map, Self::Error>;
    fn set(&self, map: &mut Self::Map, key: Key, value: Self::Value) -> Result<(), Self::Error>;
    fn finish_map(&self, map: Self::Map) -> Result<Self::Value, Self::Error>;
    fn list(&self, capacity: usize) -> Result<Self::List, Self::Error>;
    fn push(&self, list: &mut Self::List, value: Self::Value) -> Result<(), Self::Error>;
    fn finish_list(&self, list: Self::List) -> Result<Self::Value, Self::Error>;

    /// A user payload: the language's borrowed-buffer type where it has one.
    fn bytes(&self, v: Vec<u8>) -> Result<Self::Value, Self::Error>;
    /// State JSON, which a language's own JSON reader may need as its own
    /// bytes type rather than as a buffer.
    fn json(&self, v: Vec<u8>) -> Result<Self::Value, Self::Error> {
        self.bytes(v)
    }
    fn text(&self, v: String) -> Result<Self::Value, Self::Error>;
    /// A vocabulary word: a Symbol where the language has one, else text.
    fn word(&self, v: &'static str) -> Result<Self::Value, Self::Error>;
    /// One metadata entry. A pair rather than a map entry: the schema allows
    /// a repeated key and the order is part of what the controller sees.
    fn pair(&self, key: String, value: String) -> Result<Self::Value, Self::Error>;
    fn u64(&self, v: u64) -> Result<Self::Value, Self::Error>;
    fn u32(&self, v: u32) -> Result<Self::Value, Self::Error>;
    fn bool(&self, v: bool) -> Result<Self::Value, Self::Error>;
}

/// How a language reads back the value a host answers with.
pub trait Source {
    type Value;
    type Map;
    type Error: From<Refusal>;

    const CAMEL: bool = false;

    /// The value under `key`, or `None` when the key is absent or the
    /// language's nil.
    fn get(&self, map: &Self::Map, key: Key) -> Result<Option<Self::Value>, Self::Error>;
    fn map(&self, v: &Self::Value, what: &str) -> Result<Self::Map, Self::Error>;
    fn text(&self, v: &Self::Value) -> Result<String, Self::Error>;
    fn bytes(&self, v: &Self::Value, what: &str) -> Result<Vec<u8>, Self::Error>;
    fn u64(&self, v: &Self::Value, what: &str) -> Result<u64, Self::Error>;
    fn u32(&self, v: &Self::Value) -> Result<u32, Self::Error>;
    fn bool(&self, v: &Self::Value) -> Result<bool, Self::Error>;
    fn items(&self, v: &Self::Value) -> Result<Vec<Self::Value>, Self::Error>;
    fn pair(&self, v: &Self::Value) -> Result<(String, String), Self::Error>;
}

// --- schema -> the language ------------------------------------------------

/// Build the activation the host's dispatcher branches on.
pub fn activation<S: Sink>(s: &S, act: schema::Activation) -> Result<S::Value, S::Error> {
    let mut out = s.map()?;
    let token = s.u64(act.token)?;
    s.set(&mut out, TOKEN, token)?;
    let operative = s.text(act.target.operative)?;
    s.set(&mut out, OPERATIVE, operative)?;
    let procedure = s.u32(act.target.procedure)?;
    s.set(&mut out, PROCEDURE, procedure)?;
    if let Some(key) = act.target.entity_key {
        let key = s.text(key)?;
        s.set(&mut out, ENTITY_KEY, key)?;
    }
    let change_seq = s.u64(act.change_seq)?;
    s.set(&mut out, CHANGE_SEQ, change_seq)?;
    let factory = s.text(act.factory)?;
    s.set(&mut out, FACTORY, factory)?;
    kind_out(s, &mut out, act.kind)?;
    s.finish_map(out)
}

/// The discriminator, and whatever that kind carries.
fn kind_out<S: Sink>(
    s: &S,
    out: &mut S::Map,
    kind: schema::ActivationKind,
) -> Result<(), S::Error> {
    use schema::ActivationKind as K;
    match kind {
        K::Work(items) => {
            set_word(s, out, KIND, word::WORK)?;
            let items = work_items(s, items)?;
            s.set(out, ITEMS, items)?;
        }
        K::RestoreFold(fold) => {
            set_word(s, out, KIND, word::RESTORE_FOLD)?;
            if let Some(snapshot) = fold.snapshot {
                let snapshot = restore_body(s, snapshot)?;
                s.set(out, SNAPSHOT, snapshot)?;
            }
            let mut changes = s.list(fold.changes.len())?;
            for body in fold.changes {
                let body = restore_body(s, body)?;
                s.push(&mut changes, body)?;
            }
            let changes = s.finish_list(changes)?;
            s.set(out, CHANGES, changes)?;
        }
        K::SnapshotRequest => set_word(s, out, KIND, word::SNAPSHOT_REQUEST)?,
        K::LiveStateRead => set_word(s, out, KIND, word::LIVE_STATE_READ)?,
        K::ApplyState { state_json } => {
            set_word(s, out, KIND, word::APPLY_STATE)?;
            let json = s.json(state_json)?;
            s.set(out, STATE_JSON, json)?;
        }
        K::MigrationSerialize => set_word(s, out, KIND, word::MIGRATION_SERIALIZE)?,
        K::KeyRequest(request) => {
            set_word(s, out, KIND, word::KEY_REQUEST)?;
            let request = key_request(s, request)?;
            s.set(out, KEY_REQUEST, request)?;
        }
        K::WorkflowRun { args } => {
            set_word(s, out, KIND, word::WORKFLOW_RUN)?;
            let args = s.bytes(args)?;
            s.set(out, ARGS, args)?;
        }
        K::CompensateRun { outputs } => {
            set_word(s, out, KIND, word::COMPENSATE_RUN)?;
            let list = byte_list(s, outputs)?;
            s.set(out, OUTPUTS, list)?;
        }
    }
    Ok(())
}

fn set_word<S: Sink>(s: &S, map: &mut S::Map, key: Key, w: &'static str) -> Result<(), S::Error> {
    let w = s.word(w)?;
    s.set(map, key, w)
}

fn byte_list<S: Sink>(s: &S, bodies: Vec<Vec<u8>>) -> Result<S::Value, S::Error> {
    let mut list = s.list(bodies.len())?;
    for body in bodies {
        let body = s.bytes(body)?;
        s.push(&mut list, body)?;
    }
    s.finish_list(list)
}

fn metadata_out<S: Sink>(s: &S, pairs: Vec<(String, String)>) -> Result<S::Value, S::Error> {
    let mut list = s.list(pairs.len())?;
    for (key, value) in pairs {
        let pair = s.pair(key, value)?;
        s.push(&mut list, pair)?;
    }
    s.finish_list(list)
}

fn restore_body<S: Sink>(s: &S, body: schema::RestoreBody) -> Result<S::Value, S::Error> {
    let mut out = s.map()?;
    let bytes = s.bytes(body.bytes)?;
    s.set(&mut out, BYTES, bytes)?;
    if let Some(version) = body.from_version {
        let version = s.u32(version)?;
        s.set(&mut out, FROM_VERSION, version)?;
    }
    s.finish_map(out)
}

fn key_request<S: Sink>(s: &S, request: schema::KeyRequest) -> Result<S::Value, S::Error> {
    let mut out = s.map()?;
    let channel = s.u32(request.channel)?;
    s.set(&mut out, CHANNEL, channel)?;
    let payloads = byte_list(s, request.payloads)?;
    s.set(&mut out, PAYLOADS, payloads)?;
    let want_keys = s.bool(request.want_keys)?;
    s.set(&mut out, WANT_KEYS, want_keys)?;
    let want_event_time = s.bool(request.want_event_time)?;
    s.set(&mut out, WANT_EVENT_TIME, want_event_time)?;
    s.finish_map(out)
}

fn work_items<S: Sink>(s: &S, items: Vec<schema::WorkItem>) -> Result<S::Value, S::Error> {
    let mut list = s.list(items.len())?;
    for item in items {
        let item = match item {
            schema::WorkItem::Input(i) => input_item(s, i)?,
            schema::WorkItem::Timer(t) => timer_item(s, t)?,
        };
        s.push(&mut list, item)?;
    }
    s.finish_list(list)
}

fn input_item<S: Sink>(s: &S, i: schema::InputItem) -> Result<S::Value, S::Error> {
    let mut out = s.map()?;
    set_word(s, &mut out, KIND, word::INPUT)?;
    let channel = s.u32(i.channel)?;
    s.set(&mut out, CHANNEL, channel)?;
    let payload = s.bytes(i.payload)?;
    s.set(&mut out, PAYLOAD, payload)?;
    let meta = metadata_out(s, i.metadata)?;
    s.set(&mut out, METADATA, meta)?;
    if let Some(offset) = i.consumed_offset {
        let offset = s.u64(offset)?;
        s.set(&mut out, CONSUMED_OFFSET, offset)?;
    }
    let replay = s.bool(i.replay)?;
    s.set(&mut out, REPLAY, replay)?;
    let now = s.u64(i.now)?;
    s.set(&mut out, NOW, now)?;
    s.finish_map(out)
}

fn timer_item<S: Sink>(s: &S, t: schema::TimerItem) -> Result<S::Value, S::Error> {
    let mut out = s.map()?;
    set_word(s, &mut out, KIND, word::TIMER)?;
    set_word(s, &mut out, TIMER_KIND, t.kind.as_str())?;
    let scheduled_at = s.u64(t.scheduled_at)?;
    s.set(&mut out, SCHEDULED_AT, scheduled_at)?;
    // A timer firing's `now` IS its scheduled_at, so a chained `timer_after`
    // anchors replay-stably.
    let now = s.u64(t.scheduled_at)?;
    s.set(&mut out, NOW, now)?;
    s.finish_map(out)
}

// --- the language -> schema ------------------------------------------------

fn missing<S: Source>(key: Key, what: &str) -> S::Error {
    Refusal::Shape(format!("{what} is missing `{}`", key.spell(S::CAMEL))).into()
}

fn unknown<S: Source>(what: &str, value: &str) -> S::Error {
    Refusal::Shape(format!("unknown {what} {value:?}")).into()
}

fn req<S: Source>(s: &S, m: &S::Map, key: Key, what: &str) -> Result<S::Value, S::Error> {
    s.get(m, key)?.ok_or_else(|| missing::<S>(key, what))
}

fn req_text<S: Source>(s: &S, m: &S::Map, key: Key, what: &str) -> Result<String, S::Error> {
    s.text(&req(s, m, key, what)?)
}

fn req_u64<S: Source>(s: &S, m: &S::Map, key: Key, what: &str) -> Result<u64, S::Error> {
    s.u64(&req(s, m, key, what)?, key.spell(S::CAMEL))
}

fn opt_text<S: Source>(s: &S, m: &S::Map, key: Key) -> Result<Option<String>, S::Error> {
    s.get(m, key)?.as_ref().map(|v| s.text(v)).transpose()
}

/// An optional integer; `what` is what a refusal calls it, which is the key
/// itself unless the caller says otherwise.
fn opt_u64_as<S: Source>(s: &S, m: &S::Map, key: Key, what: &str) -> Result<Option<u64>, S::Error> {
    s.get(m, key)?.as_ref().map(|v| s.u64(v, what)).transpose()
}

fn opt_u64<S: Source>(s: &S, m: &S::Map, key: Key) -> Result<Option<u64>, S::Error> {
    opt_u64_as(s, m, key, key.spell(S::CAMEL))
}

fn opt_bool<S: Source>(s: &S, m: &S::Map, key: Key) -> Result<bool, S::Error> {
    match s.get(m, key)? {
        None => Ok(false),
        Some(v) => s.bool(&v),
    }
}

fn opt_bytes<S: Source>(s: &S, m: &S::Map, key: Key) -> Result<Option<Vec<u8>>, S::Error> {
    s.get(m, key)?
        .as_ref()
        .map(|v| s.bytes(v, key.spell(S::CAMEL)))
        .transpose()
}

/// An absent or nil sequence reads as empty: every mapping treats a missing
/// list and an empty one alike.
fn opt_list<S: Source>(s: &S, m: &S::Map, key: Key) -> Result<Vec<S::Value>, S::Error> {
    match s.get(m, key)? {
        None => Ok(Vec::new()),
        Some(v) => s.items(&v),
    }
}

fn read_all<S: Source, T>(
    s: &S,
    values: Vec<S::Value>,
    read: fn(&S, &S::Value) -> Result<T, S::Error>,
) -> Result<Vec<T>, S::Error> {
    let mut out = Vec::with_capacity(values.len());
    for value in &values {
        out.push(read(s, value)?);
    }
    Ok(out)
}

/// Read the postcard `ActivationResult` out of what the host answered with.
/// Reading and building live together: one implementation of the wire
/// contract, in the crate that owns it.
pub fn result<S: Source>(s: &S, r: &S::Map) -> Result<schema::ActivationResult, S::Error> {
    use schema::ActivationOutcome as O;
    let outcome = match req_text(s, r, OUTCOME, what::ACTIVATION_RESULT)?.as_str() {
        word::WORK => O::Work(read_all(s, opt_list(s, r, EFFECTS)?, effect)?),
        word::RESTORED => O::Restored,
        word::SNAPSHOT => O::Snapshot {
            state: opt_bytes(s, r, STATE)?.unwrap_or_default(),
        },
        word::STATE_JSON => O::StateJson {
            json: opt_bytes(s, r, JSON)?.unwrap_or_default(),
        },
        word::STATE_APPLIED => O::StateApplied,
        word::MIGRATION_STATE => O::MigrationState {
            json: opt_bytes(s, r, JSON)?.unwrap_or_default(),
        },
        word::PREPARED => O::Prepared(read_all(s, opt_list(s, r, PREPARED)?, prepared)?),
        word::FAILED => O::Failed(schema::WorkError {
            message: opt_text(s, r, ERROR)?.unwrap_or_else(|| "activation failed".into()),
            non_retryable: opt_bool(s, r, NON_RETRYABLE)?,
        }),
        word::WORKFLOW_DONE => O::WorkflowDone {
            completed: opt_bool(s, r, WORKFLOW_COMPLETED)?,
            result: opt_bytes(s, r, WORKFLOW_RESULT)?.unwrap_or_default(),
        },
        other => return Err(unknown::<S>("activation outcome", other)),
    };
    Ok(schema::ActivationResult {
        token: req_u64(s, r, TOKEN, what::ACTIVATION_RESULT)?,
        outcome,
    })
}

fn effect<S: Source>(s: &S, value: &S::Value) -> Result<schema::ItemEffect, S::Error> {
    let h = s.map(value, what::AN_ITEM_EFFECT)?;
    let verdict = verdict(s, &h)?;
    let mut changes = Vec::new();
    for c in opt_list(s, &h, CHANGES)? {
        changes.push(s.bytes(&c, what::CHANGE)?);
    }
    let outputs = read_all(s, opt_list(s, &h, OUTPUTS)?, output)?;
    let timers = read_all(s, opt_list(s, &h, TIMERS)?, timer_op)?;
    let dlq = match s.get(&h, DLQ)? {
        None => None,
        Some(d) => Some(dlq_entry(s, &s.map(&d, what::A_DLQ_ENTRY)?)?),
    };
    Ok(schema::ItemEffect {
        verdict,
        changes,
        outputs,
        consumed_offset: opt_u64(s, &h, CONSUMED_OFFSET)?,
        event_time: opt_u64(s, &h, EVENT_TIME)?.unwrap_or(0),
        timers,
        checkpoint: opt_bool(s, &h, CHECKPOINT)?,
        dlq,
    })
}

fn verdict<S: Source>(s: &S, h: &S::Map) -> Result<schema::Verdict, S::Error> {
    let named = req_text(s, h, VERDICT, what::ITEM_EFFECT)?;
    let message = || opt_text(s, h, MESSAGE).map(Option::unwrap_or_default);
    Ok(match named.as_str() {
        word::OK => schema::Verdict::Ok,
        word::RETRYABLE => schema::Verdict::RetryableError {
            message: message()?,
        },
        word::NON_RETRYABLE => schema::Verdict::NonRetryableError {
            message: message()?,
        },
        other => return Err(unknown::<S>("verdict", other)),
    })
}

fn output<S: Source>(s: &S, value: &S::Value) -> Result<schema::Output, S::Error> {
    let h = s.map(value, what::AN_OUTPUT)?;
    let target = match req_text(s, &h, TARGET, what::OUTPUT)?.as_str() {
        word::EDGE => schema::OutputTarget::Edge {
            channel: s.u32(&req(s, &h, CHANNEL, what::EDGE_OUTPUT)?)?,
            consumer: s.u32(&req(s, &h, CONSUMER, what::EDGE_OUTPUT)?)?,
        },
        word::PLUGIN => schema::OutputTarget::Plugin {
            channel: req_text(s, &h, PLUGIN_CHANNEL, what::PLUGIN_OUTPUT)?,
        },
        other => return Err(unknown::<S>("output target", other)),
    };
    Ok(schema::Output {
        target,
        key: opt_text(s, &h, KEY)?,
        event_time: opt_u64_as(s, &h, EVENT_TIME, what::OUTPUT_EVENT_TIME.spell(S::CAMEL))?,
        metadata: metadata_in(s, &h)?,
        payload: opt_bytes(s, &h, PAYLOAD)?.unwrap_or_default(),
    })
}

fn metadata_in<S: Source>(s: &S, m: &S::Map) -> Result<Vec<(String, String)>, S::Error> {
    let mut out = Vec::new();
    for entry in opt_list(s, m, METADATA)? {
        out.push(s.pair(&entry)?);
    }
    Ok(out)
}

fn timer_op<S: Source>(s: &S, value: &S::Value) -> Result<schema::TimerOp, S::Error> {
    let t = s.map(value, what::A_TIMER_OP)?;
    let named = req_text(s, &t, KIND, what::TIMER_OP)?;
    let kind = match named.as_str() {
        word::EVENT_TIME => schema::TimerKind::EventTime,
        word::PROCESSING_TIME => schema::TimerKind::ProcessingTime,
        other => return Err(unknown::<S>("timer kind", other)),
    };
    Ok(schema::TimerOp {
        kind,
        scheduled_at: s.u64(
            &req(s, &t, SCHEDULED_AT, what::TIMER_OP)?,
            what::TIMER_SCHEDULED_AT.spell(S::CAMEL),
        )?,
        register: s.bool(&req(s, &t, REGISTER, what::TIMER_OP)?)?,
    })
}

fn dlq_entry<S: Source>(s: &S, d: &S::Map) -> Result<schema::DlqEntry, S::Error> {
    Ok(schema::DlqEntry {
        channel: req_text(s, d, CHANNEL, what::DLQ_ENTRY)?,
        error: req_text(s, d, ERROR, what::DLQ_ENTRY)?,
        attempts: s.u32(&req(s, d, ATTEMPTS, what::DLQ_ENTRY)?)?,
        payload: opt_bytes(s, d, PAYLOAD)?,
    })
}

/// One prepare answer per payload, in order. A per-payload refusal is a
/// `WorkError` inside the answer — the input's retry ladder takes it from
/// there — while a malformed answer refuses at the caller, so the two failure
/// kinds cannot be collected together.
fn prepared<S: Source>(
    s: &S,
    value: &S::Value,
) -> Result<Result<schema::PrepareAnswer, schema::WorkError>, S::Error> {
    let h = s.map(value, what::A_PREPARE_ANSWER)?;
    if let Some(message) = opt_text(s, &h, ERROR)? {
        return Ok(Err(schema::WorkError {
            message,
            non_retryable: opt_bool(s, &h, NON_RETRYABLE)?,
        }));
    }
    let mut keys = Vec::new();
    for key in opt_list(s, &h, KEYS)? {
        keys.push(s.text(&key)?);
    }
    Ok(Ok(schema::PrepareAnswer {
        keys,
        event_time: opt_u64_as(s, &h, EVENT_TIME, what::PREPARE_EVENT_TIME.spell(S::CAMEL))?,
    }))
}
