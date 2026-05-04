use std::env;
use std::io::{self, Read};
use std::thread;
use std::time::Duration;

use flezha_compute_engine::shard::{Shard, ShardCtx, ShardError};
use flezha_compute_engine::{register_relay, ChannelBuilder, Relay};

use serde::{Deserialize, Serialize};

fn main() {
    register_relays();

    let _compute_engine = flezha_compute_engine::new(namespace());

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
    // RELAY_MESSAGE_HANDLERS_PENDING — EchoMessageRelay /
    // ErrorMessageRelay / JsonStateMessageRelay are removed in
    // relay-mvp-rust per design.md Decision 9. They are restored in
    // `relay-message-handlers` once per-Shard `handle_message` ships.
}

// -- DoNothingRelay -----------------------------------------------------------
//
// Echoes the input bytes on the plugin-bound `echo` channel. Mirrors the
// historical pre-relay-mvp `DoNothingProcessor::process` (returned `Ok(input)`)
// so the agent's `:on_output` extensions see one event per input — the
// integration tests using `attach_output_msg(do_nothing)` and the
// `mix bench --shape regression` regression bench both depend on this
// echo to drive `BeamMsgForward.on_output` exactly once per dispatched input.

#[derive(Default)]
struct DoNothingShard;
impl Shard for DoNothingShard {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    fn name() -> &'static str {
        "do_nothing_shard"
    }
    fn init_state(_: Option<&str>) {}
    fn run(
        &mut self,
        input: String,
        _: &mut (),
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

#[derive(Default)]
struct StoreInputShard;
impl Shard for StoreInputShard {
    type Input = String;
    type Output = String;
    type State = StoreInputState;
    fn name() -> &'static str {
        "store_input_shard"
    }
    fn init_state(_: Option<&str>) -> StoreInputState {
        StoreInputState::default()
    }
    fn run(
        &mut self,
        input: String,
        state: &mut StoreInputState,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        state.text_inputs.push(input);
        Ok(())
    }
}

struct StoreInputRelay;
impl Relay for StoreInputRelay {
    fn factory_id() -> &'static str {
        "store_input"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<StoreInputShard>();
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
    fn name() -> &'static str {
        "compare_filter_shard"
    }
    fn init_state(_: Option<&str>) -> CompareFilterState {
        CompareFilterState::default()
    }
    fn run(
        &mut self,
        input: String,
        state: &mut CompareFilterState,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        if state.comparable == input {
            // matched
        } else {
            // not matched
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
    }
}

// -- StaticPartitionRelay -----------------------------------------------------
// Used by the new partitioning-related integration tests. Entry shard has
// 4 static partitions keyed `a` / `b` / `c` / `d`; partition_for picks by
// the input's first character. A second shard receives the entry shard's
// output. A `to_plugin("test_kafka")` channel wires plugin-bound coverage.

#[derive(Serialize, Deserialize, Clone, Default)]
struct StaticEntryState {
    seen: Vec<String>,
}

#[derive(Default)]
struct StaticEntryShard;
impl Shard for StaticEntryShard {
    type Input = String;
    type Output = String;
    type State = StaticEntryState;
    fn name() -> &'static str {
        "static_entry_shard"
    }
    fn init_state(_: Option<&str>) -> StaticEntryState {
        StaticEntryState::default()
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
        state: &mut StaticEntryState,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        state.seen.push(input.clone());
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StaticDownstreamShard;
impl Shard for StaticDownstreamShard {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    fn name() -> &'static str {
        "static_downstream_shard"
    }
    fn init_state(_: Option<&str>) {}
    fn run(
        &mut self,
        input: String,
        _: &mut (),
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
// Targeted by `mix bench --runtime rust --shape partitioning` per
// `relay-bench-harness/design.md` Decision 7: 4 shards × 8 partitions, the
// entry shard fans out every input across all 8 partition keys, downstream
// shards route each emit to a single partition so the per-input invocation
// count is a bounded `4 × 8 = 32` rather than exponential.

const STRESS_KEYS: &[&str] = &["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"];

#[derive(Default)]
struct StressShardA;
impl Shard for StressShardA {
    type Input = String;
    type Output = String;
    type State = ();
    fn name() -> &'static str {
        "stress_a"
    }
    fn init_state(_: Option<&str>) {}
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
        _: &mut (),
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
    fn name() -> &'static str {
        "stress_b"
    }
    fn init_state(_: Option<&str>) {}
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
        _: &mut (),
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
    fn name() -> &'static str {
        "stress_c"
    }
    fn init_state(_: Option<&str>) {}
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
        _: &mut (),
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
    fn name() -> &'static str {
        "stress_d"
    }
    fn init_state(_: Option<&str>) {}
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
        _: &mut (),
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        // Emits one frame per ShardD invocation so `mix bench --shape
        // partitioning`'s `BeamMsgForward(:on_output)` plugin sees a
        // bounded number of responses per dispatched input
        // (`STRESS_KEYS.len()` = 8 ShardD runs per input → 8 responses).
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
