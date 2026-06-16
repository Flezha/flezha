use std::io::{self, Read};
use std::time::Duration;
use std::{env, thread};

use flezha_executor::shard::{Shard, ShardCtx, ShardError};
use flezha_executor::state::{
    Change, CoalesceKey, NoChange, RestoreInitPayload, StateHandle, WallClock,
};
use flezha_executor::{register_relay, ChannelBuilder, OverflowPolicy, Relay};
use serde::{Deserialize, Serialize};

fn main() {
    register_relays();

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
