use std::io::{self, Read};
use std::time::Duration;
use std::{env, thread};

use flezha_executor::shard::{Shard, ShardCtx, ShardError};
use flezha_executor::state::{
    Change, CoalesceKey, NoChange, RestoreInitPayload, StateHandle, WallClock,
};
use flezha_executor::{
    register_relay, register_task, ChannelBuilder, OverflowPolicy, Relay, Task, TaskCtx, TaskError,
};
use serde::{Deserialize, Serialize};

fn main() {
    register_relays();
    register_tasks();

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

fn register_relays() {
    register_relay::<DoNothingRelay>();
    register_relay::<StoreInputRelay>();
    register_relay::<CompareFilterRelay>();
    register_relay::<StaticPartitionRelay>();
    register_relay::<PartitioningStressRelay>();
    register_relay::<BoundedMailboxRelay>();
    register_relay::<StatefulCounterRelay>();
    register_relay::<CoalescingCounterRelay>();
    register_relay::<PersistenceOffRelay>();
    register_relay::<HybridChannelRelay>();
    register_relay::<ResumeProbeRelay>();
    register_relay::<DynamicPartitionRelay>();
    register_relay::<MixedRelay>();
}

fn register_tasks() {
    register_task::<EchoTask>();
}

// -- EchoTask -----------------------------------------------------------------
//
// Run-once task exercised by the `task-kind` e2e: emits its args back as
// output once, then returns Ok so the SDK ships `TaskCompleted` and the
// controller deregisters it.
struct EchoTask;

impl Task for EchoTask {
    fn factory_id() -> &'static str {
        "echo_task"
    }

    fn run(ctx: &mut TaskCtx, args: &str) -> Result<(), TaskError> {
        ctx.emit(format!("echo:{args}").into_bytes())?;
        Ok(())
    }
}

// -- DynamicPartitionRelay ----------------------------------------------------
//
// Dynamic-mode relay shared by the e2e correctness test and `mix bench
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
struct DynamicPartitionShard;
impl Shard for DynamicPartitionShard {
    type Input = String;
    type Output = Vec<u8>;
    type State = DynState;
    type Change = DynChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "dynamic_partition_shard"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> DynState {
        DynState::default()
    }
    fn apply_change(state: &mut DynState, change: &DynChange) {
        let DynChange::Push(s) = change;
        state.seen.push(s.clone());
    }
    const SHARD_KEY_OVERRIDDEN: bool = true;
    fn shard_key(input: &String) -> Option<String> {
        (input != "drop").then(|| input.split(':').next().unwrap_or(input).to_string())
    }
    fn partition_idle_ttl() -> Option<Duration> {
        env::var("DYN_TEST_TTL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .map(Duration::from_millis)
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        state.apply(DynChange::Push(input.clone()));
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct DynamicPartitionRelay;
impl Relay for DynamicPartitionRelay {
    fn factory_id() -> &'static str {
        "dynamic_partition"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input")
            .on_full(OverflowPolicy::Block)
            .into::<DynamicPartitionShard>();
        c.channel::<Vec<u8>>("kafka")
            .from::<DynamicPartitionShard>()
            .to_plugin("test_kafka");
    }
}

// -- DoNothingRelay -----------------------------------------------------------
//
// Echoes the input bytes on the plugin-bound `echo` channel. Stateless;
// inherits `Change = NoChange` so the runtime skips flush-buffer registration.

#[derive(Default)]
struct DoNothingShard;
impl Shard for DoNothingShard {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "do_nothing_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct DoNothingRelay;
impl Relay for DoNothingRelay {
    fn factory_id() -> &'static str {
        "do_nothing"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<DoNothingShard>();
        c.channel::<Vec<u8>>("echo")
            .from::<DoNothingShard>()
            .to_plugin("do_nothing_echo");
    }
}

// -- StoreInputRelay ----------------------------------------------------------

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
struct StoreInputShard;
impl Shard for StoreInputShard {
    type Input = String;
    type Output = String;
    type State = StoreInputState;
    type Change = StoreInputChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "store_input_shard"
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
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        state.apply(StoreInputChange::PushText(input.clone()));
        ctx.emit::<String>(input)
    }
}

struct StoreInputRelay;
impl Relay for StoreInputRelay {
    fn factory_id() -> &'static str {
        "store_input"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<StoreInputShard>();
        c.channel::<String>("sink")
            .from::<StoreInputShard>()
            .to_plugin("store_input_sink");
    }
}

// -- ResumeProbeRelay ---------------------------------------------------------
//
// Stateful counter that EMITS its running total on every input, so a Kafka
// plugin on the output makes the persisted value externally observable. Used
// by chaos/tests/stateful-resume.sh to prove WAL-backed state survives a hard
// controller kill / executor restart: after recovery the relay resumes from
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
struct ResumeProbeShard;
impl Shard for ResumeProbeShard {
    type Input = String;
    type Output = Vec<u8>;
    type State = ResumeProbeState;
    type Change = ResumeProbeChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "resume_probe_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) -> ResumeProbeState {
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
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        let delta: i64 = input.parse().unwrap_or(0);
        let new_total = state.total.saturating_add(delta);
        state.apply(ResumeProbeChange::Add(delta));
        ctx.emit::<Vec<u8>>(new_total.to_string().into_bytes())
    }
}

struct ResumeProbeRelay;
impl Relay for ResumeProbeRelay {
    fn factory_id() -> &'static str {
        "resume_probe"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<ResumeProbeShard>();
        c.channel::<Vec<u8>>("total")
            .from::<ResumeProbeShard>()
            .to_plugin("resume_probe_out");
    }
}

// -- CompareFilterRelay -------------------------------------------------------

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
struct CompareFilterShard;
impl Shard for CompareFilterShard {
    type Input = String;
    type Output = String;
    type State = CompareFilterState;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "compare_filter_shard"
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
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        if state.comparable == input {
            ctx.emit::<String>(input)?;
        }
        Ok(())
    }
}

struct CompareFilterRelay;
impl Relay for CompareFilterRelay {
    fn factory_id() -> &'static str {
        "compare_filter"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<CompareFilterShard>();
        c.channel::<String>("sink")
            .from::<CompareFilterShard>()
            .to_plugin("compare_filter_sink");
    }
}

// -- StaticPartitionRelay -----------------------------------------------------

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
struct StaticEntryShard;
impl Shard for StaticEntryShard {
    type Input = String;
    type Output = String;
    type State = StaticEntryState;
    type Change = StaticEntryChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "static_entry_shard"
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
    fn shard_keys() -> Vec<&'static str> {
        vec!["a", "b", "c", "d"]
    }
    const PARTITION_FOR_OVERRIDDEN: bool = true;
    fn partition_for(input: &String) -> Vec<&'static str> {
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
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        state.apply(StaticEntryChange::Push(input.clone()));
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StaticDownstreamShard;
impl Shard for StaticDownstreamShard {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "static_downstream_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct StaticPartitionRelay;
impl Relay for StaticPartitionRelay {
    fn factory_id() -> &'static str {
        "static_partition"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<StaticEntryShard>();
        c.channel::<String>("hop")
            .from::<StaticEntryShard>()
            .into::<StaticDownstreamShard>();
        c.channel::<Vec<u8>>("kafka")
            .from::<StaticDownstreamShard>()
            .to_plugin("test_kafka");
    }
}

// -- PartitioningStressRelay --------------------------------------------------

const STRESS_KEYS: &[&str] = &["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"];

#[derive(Default)]
struct StressShardA;
impl Shard for StressShardA {
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
    fn shard_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const PARTITION_FOR_OVERRIDDEN: bool = true;
    fn partition_for(_: &String) -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StressShardB;
impl Shard for StressShardB {
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
    fn shard_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const PARTITION_FOR_OVERRIDDEN: bool = true;
    fn partition_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StressShardC;
impl Shard for StressShardC {
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
    fn shard_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const PARTITION_FOR_OVERRIDDEN: bool = true;
    fn partition_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StressShardD;
impl Shard for StressShardD {
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
    fn shard_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const PARTITION_FOR_OVERRIDDEN: bool = true;
    fn partition_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct PartitioningStressRelay;
impl Relay for PartitioningStressRelay {
    fn factory_id() -> &'static str {
        "partitioning_stress"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<StressShardA>();
        c.channel::<String>("a_to_b")
            .from::<StressShardA>()
            .into::<StressShardB>();
        c.channel::<String>("b_to_c")
            .from::<StressShardB>()
            .into::<StressShardC>();
        c.channel::<String>("c_to_d")
            .from::<StressShardC>()
            .into::<StressShardD>();
        c.channel::<Vec<u8>>("echo")
            .from::<StressShardD>()
            .to_plugin("partitioning_stress_echo");
    }
}

// -- BoundedMailboxRelay ------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct BoundedShardState {
    pub processed: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum BoundedChange {
    Inc,
}
impl Change for BoundedChange {}

fn apply_inc(state: &mut BoundedShardState, _: &BoundedChange) {
    state.processed += 1;
}

#[derive(Default)]
struct BlockTestShard;
impl Shard for BlockTestShard {
    type Input = String;
    type Output = Vec<u8>;
    type State = BoundedShardState;
    type Change = BoundedChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "block_test_shard"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> BoundedShardState {
        BoundedShardState::default()
    }
    fn apply_change(state: &mut BoundedShardState, change: &BoundedChange) {
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
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        state.apply(BoundedChange::Inc);
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

#[derive(Default)]
struct DropNewestTestShard;
impl Shard for DropNewestTestShard {
    type Input = String;
    type Output = Vec<u8>;
    type State = BoundedShardState;
    type Change = BoundedChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "drop_newest_test_shard"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> BoundedShardState {
        BoundedShardState::default()
    }
    fn apply_change(state: &mut BoundedShardState, change: &BoundedChange) {
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
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        state.apply(BoundedChange::Inc);
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

#[derive(Default)]
struct DropOldestTestShard;
impl Shard for DropOldestTestShard {
    type Input = String;
    type Output = Vec<u8>;
    type State = BoundedShardState;
    type Change = BoundedChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "drop_oldest_test_shard"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> BoundedShardState {
        BoundedShardState::default()
    }
    fn apply_change(state: &mut BoundedShardState, change: &BoundedChange) {
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
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        state.apply(BoundedChange::Inc);
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct BoundedMailboxRelay;
impl Relay for BoundedMailboxRelay {
    fn factory_id() -> &'static str {
        "bounded_mailbox"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("block_input")
            .on_full(OverflowPolicy::Block)
            .into::<BlockTestShard>();
        c.channel::<Vec<u8>>("block_echo")
            .from::<BlockTestShard>()
            .to_plugin("bounded_mailbox_echo");

        c.input::<String>("drop_newest_input")
            .into::<DropNewestTestShard>();
        c.channel::<Vec<u8>>("drop_newest_echo")
            .from::<DropNewestTestShard>()
            .to_plugin("bounded_mailbox_echo");

        c.input::<String>("drop_oldest_input")
            .on_full(OverflowPolicy::DropOldest)
            .into::<DropOldestTestShard>();
        c.channel::<Vec<u8>>("drop_oldest_echo")
            .from::<DropOldestTestShard>()
            .to_plugin("bounded_mailbox_echo");
    }
}

// -- StatefulCounterRelay (Section 6.1) --------------------------------------
//
// One Shard `CounterShard` with `Change = CounterChange { delta: i64 }`,
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
struct CounterShard;
impl Shard for CounterShard {
    type Input = i64;
    type Output = ();
    type State = CounterState;
    type Change = CounterChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "counter_shard"
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
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        state.apply(CounterChange { delta: input });
        Ok(())
    }
}

struct StatefulCounterRelay;
impl Relay for StatefulCounterRelay {
    fn factory_id() -> &'static str {
        "stateful_counter"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<CounterEntryShard>();
        c.channel::<i64>("ticks")
            .from::<CounterEntryShard>()
            .into::<CounterShard>();
    }
}

#[derive(Default)]
struct CounterEntryShard;
impl Shard for CounterEntryShard {
    type Input = String;
    type Output = i64;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "counter_entry_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        let delta: i64 = input.parse().unwrap_or(0);
        ctx.emit::<i64>(delta)
    }
}

// -- CoalescingCounterRelay (Section 6.2) ------------------------------------
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
struct TickShard;
impl Shard for TickShard {
    type Input = TickInput;
    type Output = ();
    type State = TickState;
    type Change = TickChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tick_shard"
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
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
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
struct TickEntryShard;
impl Shard for TickEntryShard {
    type Input = String;
    type Output = TickInput;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tick_entry_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
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

struct CoalescingCounterRelay;
impl Relay for CoalescingCounterRelay {
    fn factory_id() -> &'static str {
        "coalescing_counter"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<TickEntryShard>();
        c.channel::<TickInput>("ticks")
            .from::<TickEntryShard>()
            .into::<TickShard>();
    }
}

// -- PersistenceOffRelay (Section 6.3) ---------------------------------------
//
// Same Shard topology as `StatefulCounterRelay` but `PERSISTENCE_ENABLED`
// is `false`. Used by integration tests to verify the toggle-off drain
// path discards frames without invoking the wire stub.

struct PersistenceOffRelay;
impl Relay for PersistenceOffRelay {
    fn factory_id() -> &'static str {
        "persistence_off_counter"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input")
            .into::<PersistenceOffEntryShard>();
        c.channel::<i64>("ticks")
            .from::<PersistenceOffEntryShard>()
            .into::<PersistenceOffCounterShard>();
    }
    const PERSISTENCE_ENABLED: bool = false;
}

#[derive(Default)]
struct PersistenceOffEntryShard;
impl Shard for PersistenceOffEntryShard {
    type Input = String;
    type Output = i64;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "persistence_off_entry_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        let delta: i64 = input.parse().unwrap_or(0);
        ctx.emit::<i64>(delta)
    }
}

#[derive(Default)]
struct PersistenceOffCounterShard;
impl Shard for PersistenceOffCounterShard {
    type Input = i64;
    type Output = ();
    type State = CounterState;
    type Change = CounterChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "persistence_off_counter_shard"
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
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        state.apply(CounterChange { delta: input });
        Ok(())
    }
}

// -- HybridChannelRelay -------------------------------------------------------
//
// Demonstrates `also_to_plugin_with(...)`: the producer Shard emits a
// `u32` value that fans out to an inter-shard consumer and is also
// serialized to bytes for the `hybrid_indicators` plugin. The
// serializer panics on the sentinel `0xDEADBEEF` so an operator can
// observe the panic-isolation guarantee end-to-end.

#[derive(Default)]
struct HybridProducerShard;
impl Shard for HybridProducerShard {
    type Input = String;
    type Output = u32;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "hybrid_producer_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        let n: u32 = input.parse().unwrap_or(0);
        match ctx.emit::<u32>(n) {
            Ok(()) => Ok(()),
            Err(ShardError::PluginSerializerPanic { channel, message }) => {
                eprintln!("hybrid_producer_shard: serializer panic on `{channel}`: {message}");
                Ok(())
            }
            Err(other) => Err(other),
        }
    }
}

#[derive(Default)]
struct HybridConsumerShard;
impl Shard for HybridConsumerShard {
    type Input = u32;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "hybrid_consumer_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: u32,
        _: StateHandle<'_, Self>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        println!("hybrid_consumer_shard: received {input}");
        Ok(())
    }
}

struct HybridChannelRelay;
impl Relay for HybridChannelRelay {
    fn factory_id() -> &'static str {
        "hybrid_channel"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<HybridProducerShard>();
        c.channel::<u32>("hybrid_indicator")
            .from::<HybridProducerShard>()
            .also_to_plugin_with("hybrid_indicators", |v: &u32| {
                if *v == 0xDEAD_BEEF {
                    panic!("hybrid_indicators: serializer rejected sentinel 0xDEADBEEF");
                }
                v.to_be_bytes().to_vec()
            })
            .into::<HybridConsumerShard>();
    }
}

// ---------------------------------------------------------------------------
// Authoring-sugar twins (`simplify-sdk-authoring`). Each mirrors a
// hand-written relay above to prove the macros expand to the same trait
// surface and interoperate with raw shards. See
// `docs/sdk_authoring_contract.md`.
// ---------------------------------------------------------------------------

// Single-shard stateless sugar — twin of `DoNothingShard` / `DoNothingRelay`.
// Auto-wires input -> shard -> plugin and auto-registers (no `register_relay`).
#[flezha_executor::shard(id = "sugar_do_nothing", sink = "sugar_do_nothing_echo")]
fn sugar_do_nothing(input: String) -> Vec<u8> {
    input.into_bytes()
}

// Single-shard stateful sugar with `#[derive(flezha::State)]` — twin of
// `StoreInputShard` / `StoreInputRelay`.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_executor::State)]
struct SugarStoreState {
    text_inputs: Vec<String>,
}

#[flezha_executor::shard(id = "sugar_store_input", sink = "sugar_store_sink")]
fn sugar_store_input(input: String, state: &mut SugarStoreState) -> String {
    state.push_text_inputs(input.clone());
    input
}

// `relay!` linear-pipeline twin of `StaticPartitionRelay`.
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

#[flezha_executor::shard(
    id = "macro_static_entry_shard",
    keys = ["a", "b", "c", "d"],
    partition_for = macro_static_part
)]
fn macro_static_entry(input: String, state: &mut MacroStaticEntryState) -> String {
    state.push_seen(input.clone());
    input
}

#[flezha_executor::shard(id = "macro_static_downstream_shard")]
fn macro_static_downstream(input: String) -> Vec<u8> {
    input.into_bytes()
}

flezha_executor::relay! {
    id: "sugar_static_partition",
    pipeline: input::<String> -> MacroStaticEntry -> MacroStaticDownstream -> plugin::<Vec<u8>>("test_kafka")
}

// Mixed relay: one `#[flezha::shard]` shard wired to one hand-written
// `impl Shard` shard in an explicit `channels()` body.
#[flezha_executor::shard(id = "mixed_macro_entry_shard")]
fn mixed_macro_entry(input: String) -> String {
    input
}

#[derive(Default)]
struct MixedHandDownstream;
impl Shard for MixedHandDownstream {
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
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct MixedRelay;
impl Relay for MixedRelay {
    fn factory_id() -> &'static str {
        "mixed_relay"
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
    use super::*;
    use flezha_executor::testing::ShardTestHarness;
    use std::collections::{HashMap, VecDeque};

    // -----------------------------------------------------------------------
    // Canonical keyed-collection reference shard (`expressive-state-journaling`,
    // authoring-contract §3 / §6): a rolling per-key window. The sugared form
    // uses `#[journal(map_deque)]` + an `Option<Output>` warmup gate;
    // `RollingWindowTwin` is the equivalent hand-written `Shard`/`Change`. The
    // tests below assert the two are behaviourally identical and that the
    // sugared form survives `replay_equals`.
    // -----------------------------------------------------------------------

    const ROLLING_WINDOW_N: usize = 3;

    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_executor::State)]
    struct RollingWindowState {
        #[journal(map_deque)]
        window: HashMap<String, VecDeque<f64>>,
    }

    #[flezha_executor::shard(id = "rolling_window")]
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

    // Hand-written twin: the exact `Shard` / `Change` impl the sugar expands to
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
    impl Shard for RollingWindowTwin {
        type Input = (String, f64);
        type Output = f64;
        type State = RollingWindowTwinState;
        type Change = RollingWindowTwinChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "rolling_window_twin"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> RollingWindowTwinState {
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
            ctx: &mut ShardCtx<'_>,
        ) -> Result<(), ShardError> {
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

    #[flezha_executor::shard(id = "keyed_collections")]
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

    // `init = fn` maps the static partition key to per-partition config the
    // macro's default (`Default::default()`) cannot supply. Mirrors the market
    // `sma`/`ohlc` shards whose window / bucket come from the shard key.
    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_executor::State)]
    struct WindowedState {
        window: usize,
        #[journal(map_vec)]
        closes: HashMap<String, Vec<f64>>,
    }

    fn windowed_init(shard_key: Option<&str>) -> WindowedState {
        WindowedState {
            window: if shard_key == Some("win_2") { 2 } else { 3 },
            closes: HashMap::new(),
        }
    }

    fn windowed_part(_: &(String, f64)) -> Vec<&'static str> {
        vec!["win_2", "win_3"]
    }

    #[flezha_executor::shard(
        id = "windowed",
        init = windowed_init,
        keys = ["win_2", "win_3"],
        partition_for = windowed_part
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
    // relay's shards expand to this exact associated-type mapping. A divergence
    // is a compile error, not a silent drift. The Java SugarTwinTest asserts the
    // same mapping at runtime.
    fn _reference_relay_conformance()
    where
        MacroStaticEntry: Shard<
            Input = String,
            Output = String,
            State = MacroStaticEntryState,
            Clock = WallClock,
            Config = (),
        >,
        MacroStaticDownstream: Shard<
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

        let mut sugar = ShardTestHarness::<SugarDoNothing>::new();
        sugar.apply_inputs(inputs.clone());
        sugar.expect_emits(&expected);

        let mut hand = ShardTestHarness::<DoNothingShard>::new();
        hand.apply_inputs(inputs);
        hand.expect_emits(&expected);
    }

    #[test]
    fn sugar_store_input_journals_and_emits() {
        let inputs = vec!["one".to_string(), "two".to_string()];

        let mut sugar = ShardTestHarness::<SugarStoreInput>::new();
        sugar.apply_inputs(inputs.clone());
        assert_eq!(sugar.state().text_inputs, inputs);
        sugar.replay_equals().expect("derived change replay diverged");
        sugar.expect_emits(&inputs);
    }

    #[test]
    fn macro_static_entry_partitions_and_journals() {
        // `partition_for` routes by first char; the shard journals every input.
        assert_eq!(MacroStaticEntry::partition_for(&"apple".to_string()), vec!["a"]);
        assert_eq!(MacroStaticEntry::partition_for(&"zebra".to_string()), vec!["d"]);
        assert_eq!(MacroStaticEntry::shard_keys(), vec!["a", "b", "c", "d"]);

        let inputs = vec!["apple".to_string(), "avocado".to_string()];
        let mut h = ShardTestHarness::<MacroStaticEntry>::new();
        h.apply_inputs(inputs.clone());
        assert_eq!(h.state().seen, inputs);
        h.replay_equals().expect("derived change replay diverged");
        h.expect_emits(&inputs);
    }

    #[test]
    fn mixed_macro_and_hand_shards_run() {
        // The macro entry shard passes through; the hand shard turns it to bytes.
        let mut entry = ShardTestHarness::<MixedMacroEntry>::new();
        entry.apply_inputs(vec!["hi".to_string()]);
        entry.expect_emits(&["hi".to_string()]);

        let mut down = ShardTestHarness::<MixedHandDownstream>::new();
        down.apply_inputs(vec!["hi".to_string()]);
        down.expect_emits(&[b"hi".to_vec()]);
    }

    #[test]
    fn sugar_relays_register() {
        // Auto-wire + `relay!` relays are harvested without `register_relay`.
        flezha_executor::register_inventory_relays();
        let ids = flezha_executor::relay::registry::class_factory_ids();
        for id in ["sugar_do_nothing", "sugar_store_input", "sugar_static_partition"] {
            assert!(ids.contains(&id), "auto-registered relay `{id}` missing");
        }
        // The mixed relay registers manually, as before.
        register_relay::<MixedRelay>();
        assert!(flezha_executor::relay::registry::get_class("mixed_relay").is_some());
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

        let mut sugar = ShardTestHarness::<RollingWindow>::new();
        sugar.apply_inputs(inputs.clone());

        let mut twin = ShardTestHarness::<RollingWindowTwin>::new();
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
        let mut h = ShardTestHarness::<KeyedCollections>::new();
        h.apply_inputs(inputs);

        // map_set keeps the latest per key.
        assert_eq!(h.state().last.get("x"), Some(&4.0));
        assert_eq!(h.state().last.get("y"), Some(&9.0));
        // map_vec push + drain_front keeps the last three per key.
        assert_eq!(h.state().closes.get("x"), Some(&vec![2.0, 3.0, 4.0]));
        assert_eq!(h.state().closes.get("y"), Some(&vec![9.0]));
        // The flat field composes into the same `Change` enum.
        assert_eq!(h.state().ticks, 5);

        h.replay_equals().expect("keyed-collections replay diverged");
    }

    #[test]
    fn shard_init_maps_partition_key_to_state() {
        // The `init = fn` hook receives the static partition key, so each
        // partition starts from key-derived config the macro default can't give.
        assert_eq!(Windowed::init_state(Some("win_2"), None).window, 2);
        assert_eq!(Windowed::init_state(Some("win_3"), None).window, 3);
        // Cold-start / harness default (key absent) falls through to the else arm.
        assert_eq!(Windowed::init_state(None, None).window, 3);

        // Behaviour differs per partition: window 2 emits one bar earlier than
        // window 3 for the same input stream, and replay stays deterministic.
        let inputs = vec![
            ("a".to_string(), 2.0),
            ("a".to_string(), 4.0),
            ("a".to_string(), 6.0),
        ];

        let mut w2 = ShardTestHarness::<Windowed>::with_initial_state(windowed_init(Some("win_2")));
        w2.apply_inputs(inputs.clone());
        w2.replay_equals().expect("windowed(win_2) replay diverged");
        w2.expect_emits(&[3.0_f64, 5.0]); // means of [2,4] then [4,6]

        let mut w3 = ShardTestHarness::<Windowed>::with_initial_state(windowed_init(Some("win_3")));
        w3.apply_inputs(inputs);
        w3.replay_equals().expect("windowed(win_3) replay diverged");
        w3.expect_emits(&[4.0_f64]); // single mean of [2,4,6]
    }
}
