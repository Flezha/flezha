// Twins of the reference's durable structured workflow fixtures
// (test/apps/rust_runner/src/workflow.rs and workflow_loop.rs) under the same
// factory ids, procedure names, channel names and output bytes.
//
// Entry payloads and plugin frames are UTF-8 text on the wire, as in the
// reference: its bare `.sink(name)` serialises a String as UTF-8, while this
// SDK's plugin default is MessagePack — so every sink here declares an encoder.
// An inter-procedure hop between two Node procedures carries MessagePack, and
// the core derives that channel's journal codec from the channel's kind, so no
// fixture declares one.

import { defineProcedure, defineWorkflow } from '@flezha/sdk';

import { bytes, text } from './fixture_support.mjs';

// -- the release gate -------------------------------------------------------

const RELEASE_ENV = 'FLEZHA_WORKFLOW_E2E_RELEASE';

/**
 * Hold this activation until a runner boots with the release variable set.
 *
 * The harness kills the first runner and launches its replacement with the
 * variable present, so the gate is a boot-time decision: never settling on the
 * first runner, returning at once on the second. It SUSPENDS rather than holds
 * — every other activation on this event loop, the control pings among them,
 * has to keep running while a fixture waits here.
 */
const blockUntilReleased = () =>
  process.env[RELEASE_ENV] ? Promise.resolve() : new Promise(() => {});

/** Read at handler time: each scenario arms it on the runner it launches. */
const armed = (name) => process.env[name] !== undefined;

// -- the three factories ----------------------------------------------------

/**
 * Insert into a sorted, de-duplicated array.
 *
 * The reference's `SeenState` is a `BTreeSet<String>`, chosen because a set
 * keeps snapshot bytes deterministic (workflow.rs:50). A `Set` has no durable
 * form here — it is refused at definition, since it would decode as a plain
 * object or array — so the twin is the array it would decode to, kept sorted
 * for the same determinism and idempotent for the same re-run.
 */
const insertSorted = (items, value) => {
  const at = items.findIndex((item) => item >= value);
  if (at < 0) items.push(value);
  else if (items[at] !== value) items.splice(at, 0, value);
};

/**
 * The reference's `durable_step!` (workflow.rs:66-108): a `String -> String`
 * step committing one idempotent Change keyed by its own name, so a re-run
 * after a resume folds to the state it already had.
 *
 * `emit` and `compensate` are the two axes that macro parameterises. `change`
 * and `gate` are the two the reference varies by hand-writing the procedure
 * instead — the value recorded, and whether this step waits for the release
 * gate BEFORE recording it. Carrying all four here is what lets every durable
 * procedure in this module be one call.
 */
const durableStep = (name, { change, emit, compensate, gate } = {}) =>
  defineProcedure({
    name,
    initialState: () => ({ items: [] }),
    applyChange: (state, recorded) => insertSorted(state.items, recorded.insert),
    handle: async (input, ctx) => {
      // A gated step waits BEFORE its change and its emit, so the kill window
      // always finds it as the uncommitted producer while its sibling arm has
      // committed.
      if (gate && gate(input)) await blockUntilReleased();
      ctx.change({ insert: change ? change(input) : name });
      if (emit) emit(input, ctx);
      else ctx.emit(input);
    },
    // A crash-resumed compensation reconstructs completed stages with EMPTY
    // outputs, so a hook that records an undo does so unconditionally.
    ...(compensate ? { compensate } : {}),
  });

/**
 * The reference's `stateless_step!` (workflow.rs:113-140): no state, no
 * change, the whole behaviour is the handler.
 */
const statelessStep = (name, handle) => defineProcedure({ name, handle });

/** A terminal step that always fails, which is how a saga is made to unwind. */
const failingStep = (name) =>
  statelessStep(name, () => {
    throw new Error(`${name} always fails`);
  });

// -- echo_workflow ----------------------------------------------------------
//
// The run-sugar shape (workflow.rs:14): emits its args back once and returns,
// so the SDK ships the completion and the controller deregisters it.

export const echoWorkflow = defineWorkflow({
  factoryId: 'echo_workflow',
  run: (args, ctx) => {
    ctx.emit(`echo:${args}`);
  },
});

// -- commit_block_workflow / sync_durable_workflow --------------------------
//
// Proc A commits durable state, proc B blocks until the resumed runner sets
// the release variable — so a SIGKILL lands with A's state already on the WAL
// and B never finished. `sync_durable_workflow` is the same chain declaring
// SYNCHRONOUS remote durability, which is what lets the kill-the-disk drill
// destroy the state directory and still resume with A committed.

const CommitA = durableStep('commit_a');
const BlockB = statelessStep('block_b', blockUntilReleased);

const commitBlockChain = (c) => {
  c.input('in', text).into(CommitA).named('mid').into(BlockB);
};

export const commitBlockWorkflow = defineWorkflow({
  factoryId: 'commit_block_workflow',
  channels: commitBlockChain,
});

export const syncDurableWorkflow = defineWorkflow({
  factoryId: 'sync_durable_workflow',
  syncDurability: true,
  channels: commitBlockChain,
});

// -- optout_commit_block ----------------------------------------------------
//
// The opt-out twin with a live mid-chain signal: proc A's output also reaches
// a plugin sink, so the crash-resume drill can observe the mid-run point
// without durable state. A crash re-runs the chain from the top, so the sink
// sees A's marker once per run and no journal file exists.

export const optoutCommitBlock = defineWorkflow({
  factoryId: 'optout_commit_block',
  persistent: false,
  channels: (c) => {
    c.input('in', text)
      .into(CommitA)
      .named('mid')
      .sink('optout_sink', (value) => bytes(`a-ran:${value}`))
      .into(BlockB);
  },
});

// -- tier_commit_block_workflow ---------------------------------------------
//
// The effectively-once fixture. Proc A commits durable state AND emits
// `committed:<input>` under a stable key on a keyed hybrid channel, captured
// into the atomic effect record; proc B blocks. A SIGKILL lands with A's
// effect record durable and B unfinished, and the resume re-runs A — the
// change is idempotent and the re-emit deterministic.

const TierCommitA = durableStep('tier_commit_a', {
  emit: (input, ctx) => ctx.emit(`committed:${input}`, { key: 'job' }),
});

export const tierCommitBlockWorkflow = defineWorkflow({
  factoryId: 'tier_commit_block_workflow',
  channels: (c) => {
    c.effectivelyOnce();
    c.input('in', text)
      .into(TierCommitA)
      .named('mid')
      .keyed()
      .sink('tier_sink', bytes)
      .into(BlockB);
  },
});

// -- saga_block_workflow ----------------------------------------------------
//
// SA and SB commit durable state and compensate; SC fails, triggering
// reverse-order rollback (SB then SA). SA's compensation BLOCKS, so a SIGKILL
// lands mid-rollback with SB's done-marker already durable — the respawn
// finishes SA and SKIPS the already-done SB.

const SagaA = durableStep('saga_a', { compensate: blockUntilReleased });
const SagaB = durableStep('saga_b', { compensate: () => {} });
const SagaC = failingStep('saga_c');

export const sagaBlockWorkflow = defineWorkflow({
  factoryId: 'saga_block_workflow',
  channels: (c) => {
    c.input('in', text).into(SagaA).named('s1').into(SagaB).named('s2').into(SagaC);
  },
});

// -- outbox_block_workflow --------------------------------------------------
//
// The transactional outbox. Proc A commits durable state AND emits
// plugin-bound output on a hybrid channel — STAGED, so it reaches the sink
// only after the controller has journaled and fsynced A's committed entry with
// the output inside it and flushed the outbox. Proc B blocks, so a SIGKILL
// lands with A committed and flushed and B unfinished.

export const outboxBlockWorkflow = defineWorkflow({
  factoryId: 'outbox_block_workflow',
  channels: (c) => {
    c.input('in', text)
      .into(CommitA)
      .named('mid')
      .sink('outbox_sink', (value) => bytes(`a-out:${value}`))
      .into(BlockB);
  },
});

// -- undo_outbox_workflow ---------------------------------------------------
//
// The compensation half of the transactional outbox. Proc UA commits, emits
// its forward output on a hybrid plugin channel, and compensates by EMITTING
// an undo record — plugin-bound, so it is staged and rides a compensated
// journal entry rather than the wire. Proc C fails immediately, so the chain
// always rolls back; the hook BLOCKS, so on the first runner the undo is
// emitted-but-never-journaled when the SIGKILL lands, proving staged undo
// output cannot leak. The respawn re-runs the outstanding hook, whose done
// marker never landed, and the undo is delivered exactly once.

const UndoOutboxA = durableStep('undo_outbox_a', {
  compensate: async (_outputs, ctx) => {
    await blockUntilReleased();
    ctx.emit('undone');
  },
});

const UndoC = failingStep('undo_outbox_c');

export const undoOutboxWorkflow = defineWorkflow({
  factoryId: 'undo_outbox_workflow',
  channels: (c) => {
    c.input('in', text)
      .into(UndoOutboxA)
      .named('mid')
      .sink('undo_sink', (value) => bytes(`out:${value}`))
      .into(UndoC);
  },
});

// -- branch_workflow --------------------------------------------------------
//
//     BranchA ─┬─▶ BranchB ─▶ BranchD
//              └─▶ BranchC
//
// The procedure TREE (workflow.rs:352). Every procedure commits durable state
// and compensates by emitting an undo record, so both tree behaviours are
// observable at one sink:
//
//   * crash-resume — BranchD BLOCKS, so the SIGKILL lands with A, B and C
//     committed and only D's subtree outstanding. The respawn re-runs D alone,
//     seeded from B's journaled edge, so the sibling branch's `c:` marker
//     appears exactly ONCE across both runs.
//   * compensation — with FLEZHA_BRANCH_FAIL armed, BranchD fails instead of
//     blocking. A, B and C have all completed, so the rollback unwinds the two
//     branches before the root they share: `undo-c`, `undo-b`, `undo-a`.
//
// The inter-proc channels are hybrid, which is what makes a non-leaf
// procedure's forward and undo output visible at the sink.

const BRANCH_FAIL_ENV = 'FLEZHA_BRANCH_FAIL';

const undoing = (record) => (_outputs, ctx) => ctx.emit(record);

const BranchA = durableStep('branch_a', { compensate: undoing('undo-a') });
const BranchB = durableStep('branch_b', { compensate: undoing('undo-b') });

// The sibling leaf: emits straight to the sink, and its undo does too.
const BranchC = durableStep('branch_c', {
  emit: (input, ctx) => ctx.emit(`c:${input}`),
  compensate: undoing('undo-c'),
});

// The deep leaf under BranchB: blocks (crash-resume), or fails (compensation).
const BranchD = statelessStep('branch_d', async (input, ctx) => {
  if (armed(BRANCH_FAIL_ENV)) throw new Error('branch_d failed');
  await blockUntilReleased();
  ctx.emit(`d:${input}`);
});

export const branchWorkflow = defineWorkflow({
  factoryId: 'branch_workflow',
  // Fail fast: the compensation scenario is about the rollback order, not the
  // ladder that precedes it.
  retryAttempts: 0,
  channels: (c) => {
    // One channel, two consumers: THE fan-out. Both branches read exactly what
    // BranchA released on it (held handle, wired twice).
    const mid = c
      .input('in', text)
      .into(BranchA)
      .named('mid')
      .sink('branch_sink', (value) => bytes(`a:${value}`));
    mid
      .into(BranchB)
      .named('bmid')
      .sink('branch_sink', (value) => bytes(`b:${value}`))
      .into(BranchD)
      .named('d_out')
      .sink('branch_sink', bytes);
    mid.into(BranchC).named('c_out').sink('branch_sink', bytes);
  },
});

// -- diamond_workflow -------------------------------------------------------
//
//     DiamA ─┬─▶ DiamB1 ─▶ DiamB2(blocks) ─┬─▶ DiamJoin
//            └─▶ DiamC ────────────────────┘
//
// The fan-in DAG (workflow.rs:475). The join accumulates BOTH arms' outputs
// and is dispatched exactly once, after both commit. DiamB2 blocks one level
// BELOW its sibling arm — siblings commit at one level barrier — so a SIGKILL
// lands with A, B1 and C committed and B2 mid-run: the respawn re-runs B2
// alone, and the join runs once with C's JOURNALED handoff banked and B2's
// fresh output appended, so `c:` appears exactly once across both runs.

const DiamA = durableStep('diam_a');
const DiamB1 = durableStep('diam_b1');

const DiamB2 = durableStep('diam_b2', {
  change: () => 'diam_b',
  gate: () => true,
  emit: (input, ctx) => ctx.emit(`b:${input}`),
});

const DiamC = durableStep('diam_c', { emit: (input, ctx) => ctx.emit(`c:${input}`) });

// The join: one `j:`-tagged record per accumulated contribution.
const DiamJoin = statelessStep('diam_join', (input, ctx) => ctx.emit(`j:${input}`));

export const diamondWorkflow = defineWorkflow({
  factoryId: 'diamond_workflow',
  channels: (c) => {
    // Held handle wired twice = the fan-out; two arms naming DiamJoin with no
    // node name = the fan-in, since two unnamed mentions are one node.
    const fan = c
      .input('in', text)
      .into(DiamA)
      .named('dfan')
      .sink('diamond_sink', (value) => bytes(`a:${value}`));
    fan
      .into(DiamB1)
      .named('db1')
      .into(DiamB2)
      .named('db2')
      .sink('diamond_sink', bytes)
      .into(DiamJoin);
    fan
      .into(DiamC)
      .named('dc')
      .sink('diamond_sink', bytes)
      .into(DiamJoin)
      .named('j_out')
      .sink('diamond_sink', bytes);
  },
});

// -- retry_workflow ---------------------------------------------------------
//
// The per-entity retry ladder (workflow.rs:607). Proc RA commits durable
// state, forwards on a hybrid channel, and compensates by emitting an undo
// record; `retry_flaky` fails its first FLEZHA_WORKFLOW_RETRY_FAILS attempts
// (none by default — each scenario arms it) and then emits
// `attempt:<n>:<input>`. One armed failure is absorbed by the ladder, so the
// run completes and the sink sees attempt 2; arming more failures than the
// ladder has attempts exhausts it, RA rolls back and the run reports an error.

const RETRY_FAILS_ENV = 'FLEZHA_WORKFLOW_RETRY_FAILS';

// Attempts made in this process. A retry re-runs the procedure of the same
// instance in place and dispatch is entity-affine, so a module-level counter
// is the whole ladder's view even under a worker pool.
let retryAttempts = 0;

const armedFailures = () => Number.parseInt(process.env[RETRY_FAILS_ENV] ?? '0', 10) || 0;

const RetryA = durableStep('retry_a', { compensate: undoing('undone') });

const RetryFlaky = statelessStep('retry_flaky', (input, ctx) => {
  retryAttempts += 1;
  if (retryAttempts <= armedFailures()) throw new Error(`flaky attempt ${retryAttempts}`);
  ctx.emit(`attempt:${retryAttempts}:${input}`);
});

export const retryWorkflow = defineWorkflow({
  factoryId: 'retry_workflow',
  channels: (c) => {
    c.input('in', text)
      .into(RetryA)
      .named('mid')
      .sink('retry_sink', (value) => bytes(`res:${value}`))
      .into(RetryFlaky)
      .named('out')
      .sink('retry_sink', bytes);
  },
});

// -- the durable loop family ------------------------------------------------
//
// chat_loop_workflow is the durable-loop + suspension shape
// (workflow_loop.rs:8). One iteration per inbox item: TURN commits the turn to
// Entity state, so a resumed iteration is visible in the WAL, and REPLY emits
// the answer to the sink. Between turns the loop finds an empty inbox at a
// committed boundary and PARKS — the instance is evicted and rehydrated by the
// next message. The turn whose payload is the crash turn BLOCKS while the
// release variable is unset, so a SIGKILL lands with the earlier iterations
// durable and that one in flight. Keying the block on the payload rather than
// on an invocation count keeps it deterministic across the park/rehydrate
// cycles, which re-run the driver but not the turns.
//
// stateless_loop_workflow is the same loop MINUS its stateful first
// procedure: ChatReply alone holds no per-Entity state, so nothing writes an
// Entity WAL and the instance's proc-journal reaches the driver only if it
// rides the bind itself.
//
// chat_rewind_workflow is the loop whose replying procedure can be undone. The
// undo handle lives in Entity state, because a rewind's compensation is
// rebuilt from the journal and is handed NO retained outputs, so the hook
// reads the last thing it said, retracts it and pops it.

const CRASH_TURN = 'mid-crash';

const ChatTurn = durableStep('chat_turn', {
  // Keyed by the turn, so the record count is the number of DISTINCT turns
  // that ran — a re-run of a committed iteration would show up.
  change: (input) => input,
  gate: (input) => input === CRASH_TURN,
});

const ChatReply = statelessStep('chat_reply', (input, ctx) => ctx.emit(`reply:${input}`));

const ChatReplyUndoable = defineProcedure({
  name: 'chat_reply_undoable',
  initialState: () => ({ said: [] }),
  applyChange: (state, change) => {
    if (change.said === undefined) state.said.pop();
    else state.said.push(change.said);
  },
  handle: (input, ctx) => {
    ctx.change({ said: input });
    ctx.emit(`reply:${input}`);
  },
  compensate: (_outputs, ctx) => {
    const turn = ctx.state.said.at(-1);
    if (turn === undefined) return;
    ctx.change({ retracted: true });
    ctx.emit(`retract:${turn}`);
  },
});

// A chat session ends on an external stop, not on its own; the duration
// backstop keeps a stray instance from outliving the test run.
const loopKnobs = { loopMode: 'per_input', maxDurationMs: 600_000 };

export const chatLoopWorkflow = defineWorkflow({
  factoryId: 'chat_loop_workflow',
  ...loopKnobs,
  channels: (c) => {
    c.input('in', text)
      .into(ChatTurn)
      .named('mid')
      .into(ChatReply)
      .named('out')
      .sink('chat_sink', bytes);
  },
});

export const statelessLoopWorkflow = defineWorkflow({
  factoryId: 'stateless_loop_workflow',
  ...loopKnobs,
  channels: (c) => {
    c.input('in', text).into(ChatReply).named('out').sink('chat_sink', bytes);
  },
});

export const chatRewindWorkflow = defineWorkflow({
  factoryId: 'chat_rewind_workflow',
  ...loopKnobs,
  channels: (c) => {
    c.input('in', text)
      .into(ChatTurn)
      .named('mid')
      .into(ChatReplyUndoable)
      .named('out')
      .sink('chat_sink', bytes);
  },
});

// -- the bench chains -------------------------------------------------------
//
// What `mix bench.workflow --scenario chain` drives (workflow_loop.rs:232): K
// durable pass-through steps in a line, the payload unchanged, so the sink
// receives the seed input and the bench reads its own seq and send timestamp
// back. Each chain position needs its own procedure, so the sixteen are
// generated rather than written out; the edge names follow the reference,
// which spells them from the step TYPE rather than from the procedure name.

const benchSteps = Array.from({ length: 16 }, (_, at) => durableStep(`bench_s${at}`));

const benchChain = (factoryId, steps, persistent) =>
  defineWorkflow({
    factoryId,
    persistent,
    channels: (c) => {
      let stream = c.input('in', text).into(benchSteps[0]);
      for (let at = 1; at < steps; at += 1) {
        stream = stream.named(`edge_BenchS${at}`).into(benchSteps[at]);
      }
      stream.named('out').sink('bench_sink', bytes);
    },
  });

export const benchChain1 = benchChain('bench_chain_1', 1, true);
export const benchChain4 = benchChain('bench_chain_4', 4, true);
export const benchChain16 = benchChain('bench_chain_16', 16, true);
export const benchChainOptout4 = benchChain('bench_chain_optout_4', 4, false);

// -- bench_loop_workflow ----------------------------------------------------
//
// The long-lived loop the `turns` scenario drives (workflow_loop.rs:359): the
// chat loop's shape with a BOUNDED per-turn state, so a million-turn soak
// snapshots the same few bytes at every iteration, and a wake deadline for the
// timer-wake measurements.

const WAKE_AFTER_ENV = 'FLEZHA_BENCH_WAKE_AFTER_MS';

const BenchTurn = defineProcedure({
  name: 'bench_turn',
  initialState: () => ({ turns: 0, last: '' }),
  applyChange: (state, seen) => {
    state.turns += 1;
    state.last = seen.turn;
  },
  handle: (input, ctx) => {
    ctx.change({ turn: input });
    ctx.emit(input);
  },
});

export const benchLoopWorkflow = defineWorkflow({
  factoryId: 'bench_loop_workflow',
  loopMode: 'per_input',
  // A day, not the chat loop's ten minutes: a soak outlives 600 s.
  maxDurationMs: 24 * 3_600_000,
  wakeAfterMs: Number.parseInt(process.env[WAKE_AFTER_ENV] ?? '', 10) || undefined,
  channels: (c) => {
    c.input('in', text)
      .into(BenchTurn)
      .named('mid')
      .into(ChatReply)
      .named('out')
      .sink('bench_sink', bytes);
  },
});
