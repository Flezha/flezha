use std::io::{self, Read};
use std::time::Duration;
use std::{env, thread};

use flezha_executor::operation::{Operation, OperationCtx, OperationError};
use flezha_executor::state::{
    Change, CoalesceKey, NoChange, RestoreInitPayload, StateHandle, WallClock,
};
use flezha_executor::{
    register_bounded, register_continuous, BoundedCtx, BoundedError, BoundedOperative,
    ChannelBuilder, ContinuousOperative, OverflowPolicy,
};
use serde::{Deserialize, Serialize};

fn main() {
    register_continuous_all();
    register_bounded_all();

    let _executor = flezha_executor::new(namespace());

    std_keeper();
}

fn namespace() -> String {
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        return args[1].clone();
    }
    "default".to_string()
}

fn std_keeper() -> thread::JoinHandle<()> {
    let mut buf = [0u8; 1];
    loop {
        match io::stdin().read(&mut buf) {
            Ok(0) => std::process::exit(0),
            Ok(_) => {
                println!("Data available: {:?}", buf);
            }
            Err(_e) => {}
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn register_continuous_all() {
    register_continuous::<DoNothingOperative>();
    register_continuous::<StoreInputOperative>();
    register_continuous::<CompareFilterOperative>();
    register_continuous::<StaticSegmentOperative>();
    register_continuous::<SegmentingStressOperative>();
    register_continuous::<BoundedMailboxOperative>();
    register_continuous::<StatefulCounterOperative>();
    register_continuous::<CoalescingCounterOperative>();
    register_continuous::<PersistenceOffOperative>();
    register_continuous::<HybridChannelOperative>();
    register_continuous::<ResumeProbeOperative>();
    register_continuous::<DynamicSegmentOperative>();
    register_continuous::<MixedOperative>();
}

fn register_bounded_all() {
    register_bounded::<EchoBounded>();
}

// -- EchoBounded -----------------------------------------------------------------
//
// Run-once bounded operative exercised by the `bounded-kind` e2e: emits its args back as
// output once, then returns Ok so the SDK ships `TaskCompleted` and the
// controller deregisters it.
struct EchoBounded;

impl BoundedOperative for EchoBounded {
    fn factory_id() -> &'static str {
        "echo_bounded"
    }

    fn run(ctx: &mut BoundedCtx, args: &str) -> Result<(), BoundedError> {
        ctx.emit(format!("echo:{args}").into_bytes())?;
        Ok(())
    }
}

// -- DynamicSegmentOperative ----------------------------------------------------
//
// Dynamic-mode operative shared by the e2e correctness test and `mix bench
// --shape dynamic`. Key = text before the first ':' (`"drop"` → None, drop).
// Eviction TTL is read from `DYN_TEST_TTL_MS` (the e2e sets 200ms to exercise
// reap + delete; the bench leaves it unset → no eviction, pure routing).

#[derive(Serialize, Deserialize, Clone, Default)]
struct DynState {
    seen: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum DynChange {
    Push(String),
}
impl Change for DynChange {}

#[derive(Default)]
struct DynamicSegmentOperation;
impl Operation for DynamicSegmentOperation {
    type Input = String;
    type Output = Vec<u8>;
    type State = DynState;
    type Change = DynChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "dynamic_segment_operation"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> DynState {
        DynState::default()
    }
    fn apply_change(state: &mut DynState, change: &DynChange) {
        let DynChange::Push(s) = change;
        state.seen.push(s.clone());
    }
    const OPERATION_KEY_OVERRIDDEN: bool = true;
    fn operation_key(input: &String) -> Option<String> {
        (input != "drop").then(|| input.split(':').next().unwrap_or(input).to_string())
    }
    fn segment_idle_ttl() -> Option<Duration> {
        env::var("DYN_TEST_TTL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .map(Duration::from_millis)
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        state.apply(DynChange::Push(input.clone()));
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct DynamicSegmentOperative;
impl ContinuousOperative for DynamicSegmentOperative {
    fn factory_id() -> &'static str {
        "dynamic_segment"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input")
            .on_full(OverflowPolicy::Block)
            .into::<DynamicSegmentOperation>();
        c.channel::<Vec<u8>>("kafka")
            .from::<DynamicSegmentOperation>()
            .to_plugin("test_kafka");
    }
}

// -- DoNothingOperative -----------------------------------------------------------
//
// Echoes the input bytes on the plugin-bound `echo` channel. Stateless;
// inherits `Change = NoChange` so the runtime skips flush-buffer registration.

#[derive(Default)]
struct DoNothingOperation;
impl Operation for DoNothingOperation {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "do_nothing_operation"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct DoNothingOperative;
impl ContinuousOperative for DoNothingOperative {
    fn factory_id() -> &'static str {
        "do_nothing"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<DoNothingOperation>();
        c.channel::<Vec<u8>>("echo")
            .from::<DoNothingOperation>()
            .to_plugin("do_nothing_echo");
    }
}

// -- StoreInputOperative ----------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct StoreInputState {
    binary_inputs: Vec<Vec<u8>>,
    text_inputs: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum StoreInputChange {
    PushText(String),
}
impl Change for StoreInputChange {}

#[derive(Default)]
struct StoreInputOperation;
impl Operation for StoreInputOperation {
    type Input = String;
    type Output = String;
    type State = StoreInputState;
    type Change = StoreInputChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "store_input_operation"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> StoreInputState {
        StoreInputState::default()
    }
    fn apply_change(state: &mut StoreInputState, change: &StoreInputChange) {
        match change {
            StoreInputChange::PushText(s) => state.text_inputs.push(s.clone()),
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        state.apply(StoreInputChange::PushText(input.clone()));
        ctx.emit::<String>(input)
    }
}

struct StoreInputOperative;
impl ContinuousOperative for StoreInputOperative {
    fn factory_id() -> &'static str {
        "store_input"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<StoreInputOperation>();
        c.channel::<String>("sink")
            .from::<StoreInputOperation>()
            .to_plugin("store_input_sink");
    }
}

// -- ResumeProbeOperative ---------------------------------------------------------
//
// Stateful counter that EMITS its running total on every input, so a Kafka
// plugin on the output makes the persisted value externally observable. Used
// by chaos/tests/stateful-resume.sh to prove WAL-backed state survives a hard
// controller kill / executor restart: after recovery the operative resumes from
// the persisted total (replayed changes), not from zero. Input is a decimal
// delta string; output is the new running total as ASCII bytes.

#[derive(Serialize, Deserialize, Clone, Default)]
struct ResumeProbeState {
    total: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum ResumeProbeChange {
    Add(i64),
}
impl Change for ResumeProbeChange {}

#[derive(Default)]
struct ResumeProbeOperation;
impl Operation for ResumeProbeOperation {
    type Input = String;
    type Output = Vec<u8>;
    type State = ResumeProbeState;
    type Change = ResumeProbeChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "resume_probe_operation"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> ResumeProbeState {
        // Default + change replay rebuilds the total on restore (same pattern
        // as stateful_counter); no snapshot fast-path needed.
        ResumeProbeState::default()
    }
    fn apply_change(state: &mut ResumeProbeState, change: &ResumeProbeChange) {
        match change {
            ResumeProbeChange::Add(d) => state.total = state.total.saturating_add(*d),
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        let delta: i64 = input.parse().unwrap_or(0);
        let new_total = state.total.saturating_add(delta);
        state.apply(ResumeProbeChange::Add(delta));
        ctx.emit::<Vec<u8>>(new_total.to_string().into_bytes())
    }
}

struct ResumeProbeOperative;
impl ContinuousOperative for ResumeProbeOperative {
    fn factory_id() -> &'static str {
        "resume_probe"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<ResumeProbeOperation>();
        c.channel::<Vec<u8>>("total")
            .from::<ResumeProbeOperation>()
            .to_plugin("resume_probe_out");
    }
}

// -- CompareFilterOperative -------------------------------------------------------

#[derive(Serialize, Deserialize, Clone)]
struct CompareFilterState {
    comparable: String,
    inverted: bool,
}

impl Default for CompareFilterState {
    fn default() -> Self {
        Self {
            comparable: "default".to_string(),
            inverted: false,
        }
    }
}

#[derive(Default)]
struct CompareFilterOperation;
impl Operation for CompareFilterOperation {
    type Input = String;
    type Output = String;
    type State = CompareFilterState;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "compare_filter_operation"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> CompareFilterState {
        CompareFilterState::default()
    }
    fn run(
        &mut self,
        input: String,
        state: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        if state.comparable == input {
            ctx.emit::<String>(input)?;
        }
        Ok(())
    }
}

struct CompareFilterOperative;
impl ContinuousOperative for CompareFilterOperative {
    fn factory_id() -> &'static str {
        "compare_filter"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<CompareFilterOperation>();
        c.channel::<String>("sink")
            .from::<CompareFilterOperation>()
            .to_plugin("compare_filter_sink");
    }
}

// -- StaticSegmentOperative -----------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct StaticEntryState {
    seen: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum StaticEntryChange {
    Push(String),
}
impl Change for StaticEntryChange {}

#[derive(Default)]
struct StaticEntryOperation;
impl Operation for StaticEntryOperation {
    type Input = String;
    type Output = String;
    type State = StaticEntryState;
    type Change = StaticEntryChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "static_entry_operation"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> StaticEntryState {
        StaticEntryState::default()
    }
    fn apply_change(state: &mut StaticEntryState, change: &StaticEntryChange) {
        match change {
            StaticEntryChange::Push(s) => state.seen.push(s.clone()),
        }
    }
    fn operation_keys() -> Vec<&'static str> {
        vec!["a", "b", "c", "d"]
    }
    const SEGMENT_FOR_OVERRIDDEN: bool = true;
    fn segment_for(input: &String) -> Vec<&'static str> {
        match input.chars().next() {
            Some('a') => vec!["a"],
            Some('b') => vec!["b"],
            Some('c') => vec!["c"],
            _ => vec!["d"],
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        state.apply(StaticEntryChange::Push(input.clone()));
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StaticDownstreamOperation;
impl Operation for StaticDownstreamOperation {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "static_downstream_operation"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct StaticSegmentOperative;
impl ContinuousOperative for StaticSegmentOperative {
    fn factory_id() -> &'static str {
        "static_segment"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<StaticEntryOperation>();
        c.channel::<String>("hop")
            .from::<StaticEntryOperation>()
            .into::<StaticDownstreamOperation>();
        c.channel::<Vec<u8>>("kafka")
            .from::<StaticDownstreamOperation>()
            .to_plugin("test_kafka");
    }
}

// -- SegmentingStressOperative --------------------------------------------------

const STRESS_KEYS: &[&str] = &["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"];

#[derive(Default)]
struct StressOperationA;
impl Operation for StressOperationA {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_a"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn operation_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const SEGMENT_FOR_OVERRIDDEN: bool = true;
    fn segment_for(_: &String) -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StressOperationB;
impl Operation for StressOperationB {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_b"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn operation_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const SEGMENT_FOR_OVERRIDDEN: bool = true;
    fn segment_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StressOperationC;
impl Operation for StressOperationC {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_c"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn operation_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const SEGMENT_FOR_OVERRIDDEN: bool = true;
    fn segment_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StressOperationD;
impl Operation for StressOperationD {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_d"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn operation_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const SEGMENT_FOR_OVERRIDDEN: bool = true;
    fn segment_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct SegmentingStressOperative;
impl ContinuousOperative for SegmentingStressOperative {
    fn factory_id() -> &'static str {
        "segmenting_stress"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<StressOperationA>();
        c.channel::<String>("a_to_b")
            .from::<StressOperationA>()
            .into::<StressOperationB>();
        c.channel::<String>("b_to_c")
            .from::<StressOperationB>()
            .into::<StressOperationC>();
        c.channel::<String>("c_to_d")
            .from::<StressOperationC>()
            .into::<StressOperationD>();
        c.channel::<Vec<u8>>("echo")
            .from::<StressOperationD>()
            .to_plugin("segmenting_stress_echo");
    }
}

// -- BoundedMailboxOperative ------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct BoundedOperationState {
    pub processed: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum BoundedChange {
    Inc,
}
impl Change for BoundedChange {}

fn apply_inc(state: &mut BoundedOperationState, _: &BoundedChange) {
    state.processed += 1;
}

#[derive(Default)]
struct BlockTestOperation;
impl Operation for BlockTestOperation {
    type Input = String;
    type Output = Vec<u8>;
    type State = BoundedOperationState;
    type Change = BoundedChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "block_test_operation"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> BoundedOperationState {
        BoundedOperationState::default()
    }
    fn apply_change(state: &mut BoundedOperationState, change: &BoundedChange) {
        apply_inc(state, change);
    }
    fn mailbox_capacity() -> usize {
        4
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        state.apply(BoundedChange::Inc);
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

#[derive(Default)]
struct DropNewestTestOperation;
impl Operation for DropNewestTestOperation {
    type Input = String;
    type Output = Vec<u8>;
    type State = BoundedOperationState;
    type Change = BoundedChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "drop_newest_test_operation"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> BoundedOperationState {
        BoundedOperationState::default()
    }
    fn apply_change(state: &mut BoundedOperationState, change: &BoundedChange) {
        apply_inc(state, change);
    }
    fn mailbox_capacity() -> usize {
        4
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        state.apply(BoundedChange::Inc);
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

#[derive(Default)]
struct DropOldestTestOperation;
impl Operation for DropOldestTestOperation {
    type Input = String;
    type Output = Vec<u8>;
    type State = BoundedOperationState;
    type Change = BoundedChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "drop_oldest_test_operation"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> BoundedOperationState {
        BoundedOperationState::default()
    }
    fn apply_change(state: &mut BoundedOperationState, change: &BoundedChange) {
        apply_inc(state, change);
    }
    fn mailbox_capacity() -> usize {
        4
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        state.apply(BoundedChange::Inc);
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct BoundedMailboxOperative;
impl ContinuousOperative for BoundedMailboxOperative {
    fn factory_id() -> &'static str {
        "bounded_mailbox"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("block_input")
            .on_full(OverflowPolicy::Block)
            .into::<BlockTestOperation>();
        c.channel::<Vec<u8>>("block_echo")
            .from::<BlockTestOperation>()
            .to_plugin("bounded_mailbox_echo");

        c.input::<String>("drop_newest_input")
            .into::<DropNewestTestOperation>();
        c.channel::<Vec<u8>>("drop_newest_echo")
            .from::<DropNewestTestOperation>()
            .to_plugin("bounded_mailbox_echo");

        c.input::<String>("drop_oldest_input")
            .on_full(OverflowPolicy::DropOldest)
            .into::<DropOldestTestOperation>();
        c.channel::<Vec<u8>>("drop_oldest_echo")
            .from::<DropOldestTestOperation>()
            .to_plugin("bounded_mailbox_echo");
    }
}

// -- StatefulCounterOperative (Section 6.1) --------------------------------------
//
// One Operation `CounterOperation` with `Change = CounterChange { delta: i64 }`,
// `apply_change` mutating `state.value += change.delta`, `coalesce_key()`
// returning `None` (verbatim retention).

#[derive(Serialize, Deserialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct CounterState {
    pub value: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CounterChange {
    pub delta: i64,
}
impl Change for CounterChange {}

#[derive(Default)]
struct CounterOperation;
impl Operation for CounterOperation {
    type Input = i64;
    type Output = ();
    type State = CounterState;
    type Change = CounterChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "counter_operation"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) -> CounterState {
        CounterState::default()
    }
    fn apply_change(state: &mut CounterState, change: &CounterChange) {
        state.value = state.value.saturating_add(change.delta);
    }
    fn run(
        &mut self,
        input: i64,
        mut state: StateHandle<'_, Self>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        state.apply(CounterChange { delta: input });
        Ok(())
    }
}

struct StatefulCounterOperative;
impl ContinuousOperative for StatefulCounterOperative {
    fn factory_id() -> &'static str {
        "stateful_counter"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<CounterEntryOperation>();
        c.channel::<i64>("ticks")
            .from::<CounterEntryOperation>()
            .into::<CounterOperation>();
    }
}

#[derive(Default)]
struct CounterEntryOperation;
impl Operation for CounterEntryOperation {
    type Input = String;
    type Output = i64;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "counter_entry_operation"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        let delta: i64 = input.parse().unwrap_or(0);
        ctx.emit::<i64>(delta)
    }
}

// -- CoalescingCounterOperative (Section 6.2) ------------------------------------
//
// `Change = TickChange { interval: u8, ts: u64, delta: i64 }` with
// `coalesce_key` returning `Some(CoalesceKey::from_hash(&(interval, ts)))`.

#[derive(Serialize, Deserialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct TickState {
    pub last: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TickChange {
    pub interval: u8,
    pub ts: u64,
    pub delta: i64,
}
impl Change for TickChange {
    fn coalesce_key(&self) -> Option<CoalesceKey> {
        Some(CoalesceKey::from_hash(&(self.interval, self.ts)))
    }
}

#[derive(Default)]
struct TickOperation;
impl Operation for TickOperation {
    type Input = TickInput;
    type Output = ();
    type State = TickState;
    type Change = TickChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tick_operation"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) -> TickState {
        TickState::default()
    }
    fn apply_change(state: &mut TickState, change: &TickChange) {
        state.last = change.delta;
    }
    fn run(
        &mut self,
        input: TickInput,
        mut state: StateHandle<'_, Self>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        state.apply(TickChange {
            interval: input.interval,
            ts: input.ts,
            delta: input.delta,
        });
        Ok(())
    }
}

#[derive(Clone)]
pub struct TickInput {
    pub interval: u8,
    pub ts: u64,
    pub delta: i64,
}

#[derive(Default)]
struct TickEntryOperation;
impl Operation for TickEntryOperation {
    type Input = String;
    type Output = TickInput;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tick_entry_operation"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        // Wire format: `<interval>:<ts>:<delta>`.
        let mut parts = input.splitn(3, ':');
        let interval = parts.next().and_then(|s| s.parse::<u8>().ok()).unwrap_or(0);
        let ts = parts
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let delta = parts
            .next()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        ctx.emit::<TickInput>(TickInput {
            interval,
            ts,
            delta,
        })
    }
}

struct CoalescingCounterOperative;
impl ContinuousOperative for CoalescingCounterOperative {
    fn factory_id() -> &'static str {
        "coalescing_counter"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<TickEntryOperation>();
        c.channel::<TickInput>("ticks")
            .from::<TickEntryOperation>()
            .into::<TickOperation>();
    }
}

// -- PersistenceOffOperative (Section 6.3) ---------------------------------------
//
// Same Operation topology as `StatefulCounterOperative` but `PERSISTENCE_ENABLED`
// is `false`. Used by integration tests to verify the toggle-off drain
// path discards frames without invoking the wire stub.

struct PersistenceOffOperative;
impl ContinuousOperative for PersistenceOffOperative {
    fn factory_id() -> &'static str {
        "persistence_off_counter"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input")
            .into::<PersistenceOffEntryOperation>();
        c.channel::<i64>("ticks")
            .from::<PersistenceOffEntryOperation>()
            .into::<PersistenceOffCounterOperation>();
    }
    const PERSISTENCE_ENABLED: bool = false;
}

#[derive(Default)]
struct PersistenceOffEntryOperation;
impl Operation for PersistenceOffEntryOperation {
    type Input = String;
    type Output = i64;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "persistence_off_entry_operation"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        let delta: i64 = input.parse().unwrap_or(0);
        ctx.emit::<i64>(delta)
    }
}

#[derive(Default)]
struct PersistenceOffCounterOperation;
impl Operation for PersistenceOffCounterOperation {
    type Input = i64;
    type Output = ();
    type State = CounterState;
    type Change = CounterChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "persistence_off_counter_operation"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) -> CounterState {
        CounterState::default()
    }
    fn apply_change(state: &mut CounterState, change: &CounterChange) {
        state.value = state.value.saturating_add(change.delta);
    }
    fn run(
        &mut self,
        input: i64,
        mut state: StateHandle<'_, Self>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        state.apply(CounterChange { delta: input });
        Ok(())
    }
}

// -- HybridChannelOperative -------------------------------------------------------
//
// Demonstrates `also_to_plugin_with(...)`: the producer Operation emits a
// `u32` value that fans out to an inter-operation consumer and is also
// serialized to bytes for the `hybrid_indicators` plugin. The
// serializer panics on the sentinel `0xDEADBEEF` so an operator can
// observe the panic-isolation guarantee end-to-end.

#[derive(Default)]
struct HybridProducerOperation;
impl Operation for HybridProducerOperation {
    type Input = String;
    type Output = u32;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "hybrid_producer_operation"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        let n: u32 = input.parse().unwrap_or(0);
        match ctx.emit::<u32>(n) {
            Ok(()) => Ok(()),
            Err(OperationError::PluginSerializerPanic { channel, message }) => {
                eprintln!("hybrid_producer_operation: serializer panic on `{channel}`: {message}");
                Ok(())
            }
            Err(other) => Err(other),
        }
    }
}

#[derive(Default)]
struct HybridConsumerOperation;
impl Operation for HybridConsumerOperation {
    type Input = u32;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "hybrid_consumer_operation"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: u32,
        _: StateHandle<'_, Self>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        println!("hybrid_consumer_operation: received {input}");
        Ok(())
    }
}

struct HybridChannelOperative;
impl ContinuousOperative for HybridChannelOperative {
    fn factory_id() -> &'static str {
        "hybrid_channel"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<HybridProducerOperation>();
        c.channel::<u32>("hybrid_indicator")
            .from::<HybridProducerOperation>()
            .also_to_plugin_with("hybrid_indicators", |v: &u32| {
                if *v == 0xDEAD_BEEF {
                    panic!("hybrid_indicators: serializer rejected sentinel 0xDEADBEEF");
                }
                v.to_be_bytes().to_vec()
            })
            .into::<HybridConsumerOperation>();
    }
}

// ---------------------------------------------------------------------------
// Authoring-sugar twins (`simplify-sdk-authoring`). Each mirrors a
// hand-written operative above to prove the macros expand to the same trait
// surface and interoperate with raw operations. See
// `docs/sdk_authoring_contract.md`.
// ---------------------------------------------------------------------------

// Single-operation stateless sugar — twin of `DoNothingOperation` / `DoNothingOperative`.
// Auto-wires input -> operation -> plugin and auto-registers (no `register_continuous`).
#[flezha_executor::operation(id = "sugar_do_nothing", sink = "sugar_do_nothing_echo")]
fn sugar_do_nothing(input: String) -> Vec<u8> {
    input.into_bytes()
}

// Single-operation stateful sugar with `#[derive(flezha::State)]` — twin of
// `StoreInputOperation` / `StoreInputOperative`.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_executor::State)]
struct SugarStoreState {
    text_inputs: Vec<String>,
}

#[flezha_executor::operation(id = "sugar_store_input", sink = "sugar_store_sink")]
fn sugar_store_input(input: String, state: &mut SugarStoreState) -> String {
    state.push_text_inputs(input.clone());
    input
}

// `operative!` linear-pipeline twin of `StaticSegmentOperative`.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_executor::State)]
struct MacroStaticEntryState {
    seen: Vec<String>,
}

fn macro_static_part(input: &str) -> Vec<&'static str> {
    match input.chars().next() {
        Some('a') => vec!["a"],
        Some('b') => vec!["b"],
        Some('c') => vec!["c"],
        _ => vec!["d"],
    }
}

#[flezha_executor::operation(
    id = "macro_static_entry_operation",
    keys = ["a", "b", "c", "d"],
    segment_for = macro_static_part
)]
fn macro_static_entry(input: String, state: &mut MacroStaticEntryState) -> String {
    state.push_seen(input.clone());
    input
}

#[flezha_executor::operation(id = "macro_static_downstream_operation")]
fn macro_static_downstream(input: String) -> Vec<u8> {
    input.into_bytes()
}

flezha_executor::operative! {
    id: "sugar_static_segment",
    pipeline: input::<String> -> MacroStaticEntry -> MacroStaticDownstream -> plugin::<Vec<u8>>("test_kafka")
}

// Mixed operative: one `#[flezha::operation]` operation wired to one hand-written
// `impl Operation` operation in an explicit `channels()` body.
#[flezha_executor::operation(id = "mixed_macro_entry_operation")]
fn mixed_macro_entry(input: String) -> String {
    input
}

#[derive(Default)]
struct MixedHandDownstream;
impl Operation for MixedHandDownstream {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "mixed_hand_downstream"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct MixedOperative;
impl ContinuousOperative for MixedOperative {
    fn factory_id() -> &'static str {
        "mixed_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<MixedMacroEntry>();
        c.channel::<String>("hop")
            .from::<MixedMacroEntry>()
            .into::<MixedHandDownstream>();
        c.channel::<Vec<u8>>("kafka")
            .from::<MixedHandDownstream>()
            .to_plugin("test_kafka");
    }
}

#[cfg(test)]
mod macro_twin_tests {
    use std::collections::{HashMap, VecDeque};

    use flezha_executor::testing::OperationTestHarness;

    use super::*;

    // -----------------------------------------------------------------------
    // Canonical keyed-collection reference operation (`expressive-state-journaling`,
    // authoring-contract §3 / §6): a rolling per-key window. The sugared form
    // uses `#[journal(map_deque)]` + an `Option<Output>` warmup gate;
    // `RollingWindowTwin` is the equivalent hand-written `Operation`/`Change`. The
    // tests below assert the two are behaviourally identical and that the
    // sugared form survives `replay_equals`.
    // -----------------------------------------------------------------------

    const ROLLING_WINDOW_N: usize = 3;

    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_executor::State)]
    struct RollingWindowState {
        #[journal(map_deque)]
        window: HashMap<String, VecDeque<f64>>,
    }

    #[flezha_executor::operation(id = "rolling_window")]
    fn rolling_window(input: (String, f64), state: &mut RollingWindowState) -> Option<f64> {
        let (key, value) = input;
        state.push_back_window(key.clone(), value);
        let len = state.window.get(&key).map(|w| w.len()).unwrap_or(0);
        if len > ROLLING_WINDOW_N {
            state.pop_front_window(key.clone(), len - ROLLING_WINDOW_N);
        }
        let w = state.window.get(&key)?;
        if w.len() < ROLLING_WINDOW_N {
            return None;
        }
        Some(w.iter().sum::<f64>() / ROLLING_WINDOW_N as f64)
    }

    // Hand-written twin: the exact `Operation` / `Change` impl the sugar expands to
    // (variant names differ; behaviour does not).
    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug)]
    struct RollingWindowTwinState {
        window: HashMap<String, VecDeque<f64>>,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    enum RollingWindowTwinChange {
        PushBack(String, f64),
        PopFront(String, usize),
    }
    impl Change for RollingWindowTwinChange {}

    #[derive(Default)]
    struct RollingWindowTwin;
    impl Operation for RollingWindowTwin {
        type Input = (String, f64);
        type Output = f64;
        type State = RollingWindowTwinState;
        type Change = RollingWindowTwinChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "rolling_window_twin"
        }
        fn init_state(
            _: Option<&str>,
            _: Option<RestoreInitPayload<'_>>,
        ) -> RollingWindowTwinState {
            RollingWindowTwinState::default()
        }
        fn apply_change(state: &mut RollingWindowTwinState, change: &RollingWindowTwinChange) {
            match change {
                RollingWindowTwinChange::PushBack(k, v) => {
                    state.window.entry(k.clone()).or_default().push_back(*v);
                }
                RollingWindowTwinChange::PopFront(k, n) => {
                    let e = state.window.entry(k.clone()).or_default();
                    for _ in 0..(*n).min(e.len()) {
                        e.pop_front();
                    }
                }
            }
        }
        fn run(
            &mut self,
            input: (String, f64),
            mut state: StateHandle<'_, Self>,
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            let (key, value) = input;
            state.apply(RollingWindowTwinChange::PushBack(key.clone(), value));
            let len = state.window.get(&key).map(|w| w.len()).unwrap_or(0);
            if len > ROLLING_WINDOW_N {
                state.apply(RollingWindowTwinChange::PopFront(
                    key.clone(),
                    len - ROLLING_WINDOW_N,
                ));
            }
            let Some(w) = state.window.get(&key) else {
                return Ok(());
            };
            if w.len() < ROLLING_WINDOW_N {
                return Ok(());
            }
            let mean = w.iter().sum::<f64>() / ROLLING_WINDOW_N as f64;
            ctx.emit::<f64>(mean)
        }
    }

    // Per-mode coverage for the remaining journaling modes (`map_set`,
    // `map_vec`), composed with a flat field on one `Change` enum.
    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_executor::State)]
    struct KeyedCollectionsState {
        #[journal(map_set)]
        last: HashMap<String, f64>,
        #[journal(map_vec)]
        closes: HashMap<String, Vec<f64>>,
        ticks: u64,
    }

    #[flezha_executor::operation(id = "keyed_collections")]
    fn keyed_collections(input: (String, f64), state: &mut KeyedCollectionsState) {
        let (key, value) = input;
        state.set_last(key.clone(), value);
        state.push_closes(key.clone(), value);
        // Keep at most three closes per key; drain the overflow off the front.
        let len = state.closes.get(&key).map(|c| c.len()).unwrap_or(0);
        if len > 3 {
            state.drain_closes_front(key.clone(), len - 3);
        }
        state.set_ticks(state.ticks + 1);
    }

    // `init = fn` maps the static segment key to per-segment config the
    // macro's default (`Default::default()`) cannot supply. Mirrors the market
    // `sma`/`ohlc` operations whose window / bucket come from the operation key.
    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_executor::State)]
    struct WindowedState {
        window: usize,
        #[journal(map_vec)]
        closes: HashMap<String, Vec<f64>>,
    }

    fn windowed_init(operation_key: Option<&str>) -> WindowedState {
        WindowedState {
            window: if operation_key == Some("win_2") { 2 } else { 3 },
            closes: HashMap::new(),
        }
    }

    fn windowed_part(_: &(String, f64)) -> Vec<&'static str> {
        vec!["win_2", "win_3"]
    }

    #[flezha_executor::operation(
        id = "windowed",
        init = windowed_init,
        keys = ["win_2", "win_3"],
        segment_for = windowed_part
    )]
    fn windowed(input: (String, f64), state: &mut WindowedState) -> Option<f64> {
        let (key, value) = input;
        let window = state.window;
        state.push_closes(key.clone(), value);
        let len = state.closes.get(&key).map(|c| c.len()).unwrap_or(0);
        if len > window {
            state.drain_closes_front(key.clone(), len - window);
        }
        let c = state.closes.get(&key)?;
        if c.len() < window {
            return None;
        }
        Some(c.iter().sum::<f64>() / window as f64)
    }

    // Cross-SDK conformance (`docs/sdk_authoring_contract.md` §6): the reference
    // operative's operations expand to this exact associated-type mapping. A divergence
    // is a compile error, not a silent drift. The Java SugarTwinTest asserts the
    // same mapping at runtime.
    fn _reference_operative_conformance()
    where
        MacroStaticEntry: Operation<
            Input = String,
            Output = String,
            State = MacroStaticEntryState,
            Clock = WallClock,
            Config = (),
        >,
        MacroStaticDownstream: Operation<
            Input = String,
            Output = Vec<u8>,
            State = (),
            Change = NoChange,
            Clock = WallClock,
            Config = (),
        >,
    {
    }

    #[test]
    fn sugar_do_nothing_emits_like_hand_written() {
        let inputs = vec!["alpha".to_string(), "beta".to_string()];
        let expected: Vec<Vec<u8>> = vec![b"alpha".to_vec(), b"beta".to_vec()];

        let mut sugar = OperationTestHarness::<SugarDoNothing>::new();
        sugar.apply_inputs(inputs.clone());
        sugar.expect_emits(&expected);

        let mut hand = OperationTestHarness::<DoNothingOperation>::new();
        hand.apply_inputs(inputs);
        hand.expect_emits(&expected);
    }

    #[test]
    fn sugar_store_input_journals_and_emits() {
        let inputs = vec!["one".to_string(), "two".to_string()];

        let mut sugar = OperationTestHarness::<SugarStoreInput>::new();
        sugar.apply_inputs(inputs.clone());
        assert_eq!(sugar.state().text_inputs, inputs);
        sugar
            .replay_equals()
            .expect("derived change replay diverged");
        sugar.expect_emits(&inputs);
    }

    #[test]
    fn macro_static_entry_segments_and_journals() {
        // `segment_for` routes by first char; the operation journals every input.
        assert_eq!(
            MacroStaticEntry::segment_for(&"apple".to_string()),
            vec!["a"]
        );
        assert_eq!(
            MacroStaticEntry::segment_for(&"zebra".to_string()),
            vec!["d"]
        );
        assert_eq!(MacroStaticEntry::operation_keys(), vec!["a", "b", "c", "d"]);

        let inputs = vec!["apple".to_string(), "avocado".to_string()];
        let mut h = OperationTestHarness::<MacroStaticEntry>::new();
        h.apply_inputs(inputs.clone());
        assert_eq!(h.state().seen, inputs);
        h.replay_equals().expect("derived change replay diverged");
        h.expect_emits(&inputs);
    }

    #[test]
    fn mixed_macro_and_hand_operations_run() {
        // The macro entry operation passes through; the hand operation turns it to bytes.
        let mut entry = OperationTestHarness::<MixedMacroEntry>::new();
        entry.apply_inputs(vec!["hi".to_string()]);
        entry.expect_emits(&["hi".to_string()]);

        let mut down = OperationTestHarness::<MixedHandDownstream>::new();
        down.apply_inputs(vec!["hi".to_string()]);
        down.expect_emits(&[b"hi".to_vec()]);
    }

    #[test]
    fn sugar_operatives_register() {
        // Auto-wire + `operative!` operatives are harvested without `register_continuous`.
        flezha_executor::register_inventory_operatives();
        let ids = flezha_executor::operative::registry::class_factory_ids();
        for id in [
            "sugar_do_nothing",
            "sugar_store_input",
            "sugar_static_segment",
        ] {
            assert!(
                ids.contains(&id),
                "auto-registered operative `{id}` missing"
            );
        }
        // The mixed operative registers manually, as before.
        register_continuous::<MixedOperative>();
        assert!(flezha_executor::operative::registry::get_class("mixed_operative").is_some());
    }

    #[test]
    fn rolling_window_matches_hand_written_twin() {
        let inputs = vec![
            ("a".to_string(), 1.0),
            ("a".to_string(), 2.0),
            ("a".to_string(), 3.0),
            ("a".to_string(), 4.0),
            ("b".to_string(), 10.0),
            ("b".to_string(), 20.0),
            ("b".to_string(), 30.0),
        ];

        let mut sugar = OperationTestHarness::<RollingWindow>::new();
        sugar.apply_inputs(inputs.clone());

        let mut twin = OperationTestHarness::<RollingWindowTwin>::new();
        twin.apply_inputs(inputs);

        // The generated journaling replays deterministically (4.3).
        sugar.replay_equals().expect("sugared replay diverged");

        // Behaviourally identical to the hand-written twin: same state (4.2).
        assert_eq!(sugar.state().window, twin.state().window);

        // ...and the same emits (warmup gate via `Option<Output>`).
        let expected = [2.0_f64, 3.0, 20.0];
        sugar.expect_emits(&expected);
        twin.expect_emits(&expected);
    }

    #[test]
    fn keyed_collections_modes_journal_and_replay() {
        let inputs = vec![
            ("x".to_string(), 1.0),
            ("x".to_string(), 2.0),
            ("x".to_string(), 3.0),
            ("x".to_string(), 4.0),
            ("y".to_string(), 9.0),
        ];
        let mut h = OperationTestHarness::<KeyedCollections>::new();
        h.apply_inputs(inputs);

        // map_set keeps the latest per key.
        assert_eq!(h.state().last.get("x"), Some(&4.0));
        assert_eq!(h.state().last.get("y"), Some(&9.0));
        // map_vec push + drain_front keeps the last three per key.
        assert_eq!(h.state().closes.get("x"), Some(&vec![2.0, 3.0, 4.0]));
        assert_eq!(h.state().closes.get("y"), Some(&vec![9.0]));
        // The flat field composes into the same `Change` enum.
        assert_eq!(h.state().ticks, 5);

        h.replay_equals()
            .expect("keyed-collections replay diverged");
    }

    #[test]
    fn operation_init_maps_segment_key_to_state() {
        // The `init = fn` hook receives the static segment key, so each
        // segment starts from key-derived config the macro default can't give.
        assert_eq!(Windowed::init_state(Some("win_2"), None).window, 2);
        assert_eq!(Windowed::init_state(Some("win_3"), None).window, 3);
        // Cold-start / harness default (key absent) falls through to the else arm.
        assert_eq!(Windowed::init_state(None, None).window, 3);

        // Behaviour differs per segment: window 2 emits one bar earlier than
        // window 3 for the same input stream, and replay stays deterministic.
        let inputs = vec![
            ("a".to_string(), 2.0),
            ("a".to_string(), 4.0),
            ("a".to_string(), 6.0),
        ];

        let mut w2 =
            OperationTestHarness::<Windowed>::with_initial_state(windowed_init(Some("win_2")));
        w2.apply_inputs(inputs.clone());
        w2.replay_equals().expect("windowed(win_2) replay diverged");
        w2.expect_emits(&[3.0_f64, 5.0]); // means of [2,4] then [4,6]

        let mut w3 =
            OperationTestHarness::<Windowed>::with_initial_state(windowed_init(Some("win_3")));
        w3.apply_inputs(inputs);
        w3.replay_equals().expect("windowed(win_3) replay diverged");
        w3.expect_emits(&[4.0_f64]); // single mean of [2,4,6]
    }
}
