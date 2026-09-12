/**
 * The factory-function authoring surface and the fluent channel builder.
 *
 * Factory functions rather than decorators: decorators need a build step, do
 * not apply to plain functions, and cannot reshape the decorated type — which
 * is exactly what a typed graph needs. A generic function can, so
 * `.into(procedure)` refuses a producer whose value the procedure's declared
 * input does not accept, before the program runs.
 */

import { assertRepresentable, type Codec } from './codec.js';
import {
  DeclarationError,
  DROPPED,
  type BeyondWindowPolicy,
  type ChannelBuilder,
  type OperativeDefinition,
  type OverflowPolicy,
  type Procedure,
  type ProcedureDefinition,
  type Stream,
  type WorkflowDefinition,
} from './types.js';

/** A procedure as the graph holds it, with its position in the declaration. */
export interface ProcNode {
  index: number;
  /** The node identity — the procedure's name, or the name `named()` gave. */
  name: string;
  procedure: Procedure<unknown, unknown, unknown, unknown>;
  /** The one channel this node produces on, created on first wiring. */
  output?: ChannelSpec;
}

/** One consumer of a channel, with the transforms and key of its own edge. */
export interface ConsumerEdge {
  procedure: number;
  /** Producer-side transforms for this edge, applied in declaration order. */
  transforms: Array<(value: unknown) => unknown>;
  /** Dynamic routing key for this edge. */
  keyBy?: (value: unknown) => string;
}

export type ChannelKind = 'entry' | 'inter_proc' | 'plugin';

/**
 * One link of a chain, exactly as the core deserializes it.
 *
 * A chain is one fluent sequence declared off a channel handle, in the order
 * the author called it; the core reads the consumers, the channel kind, the
 * sink and the name off them. Closures never enter the record — they stay on
 * the edge held under the chain that declared them, and `validate` attaches
 * them to the consumers the core derived from that same chain.
 */
export type Link =
  | { link: 'transform' }
  | { link: 'key' }
  | { link: 'named'; name: string }
  | { link: 'sink'; name?: string }
  | { link: 'into'; procedure: number }
  | { link: 'branch'; arms: number };

/** A channel as the graph holds it. Its position in the list IS its id. */
export interface ChannelSpec {
  index: number;
  name: string;
  kind: ChannelKind;
  producer?: number;
  /** The authoring record: one entry per fluent sequence, in call order. */
  chains: Link[][];
  /**
   * The edge each chain declared, by chain index. Held aside rather than
   * wired: the core answers which chains produced a consumer and in what
   * order, so no builder decides which of two re-declarations survives.
   */
  chainEdges: Map<number, ConsumerEdge>;
  /** The consumer edges the core derived. Empty until `validate` runs. */
  consumers: ConsumerEdge[];
  overflow: OverflowPolicy;
  retryMax?: number;
  retryBackoffMs?: number;
  keyed: boolean;
  /** The plugin sink attached to this channel; absent means none. */
  sink?: string;
  /** Entry payload decoder; absent means the MessagePack default. */
  decode?: (bytes: Uint8Array) => unknown;
  /**
   * The plugin sink's payload encoder; absent means the MessagePack default.
   * It belongs to the sink frame alone: an inter-procedure frame is decoded
   * with the class codec, so encoding one with this hands the consuming
   * procedure bytes it cannot read.
   */
  sinkEncode?: (value: unknown) => Uint8Array;
}

export interface WorkflowSpec {
  loopMode: 'once' | 'per_input' | 'chained';
  rollbackMode: 'all_or_nothing' | 'keep_progress';
  retryAttempts?: number;
  retryBackoffMs?: number;
  barrierRetryAttempts?: number;
  maxDurationMs?: number;
  wakeAfterMs?: number;
}

/** Everything the graph knows about one declared class. */
export interface ClassSpec {
  factoryId: string;
  kind: 'stream' | 'workflow';
  persistent: boolean;
  effectivelyOnce: boolean;
  /** The author's explicit `checkpointAck()`; the core derives the granularity. */
  checkpointAck: boolean;
  checkpointEvery?: number;
  checkpointOutputs: 'as_produced' | 'at_checkpoint';
  checkpointMaxDelayMs?: number;
  correctionWindowMs?: number;
  beyondWindowPolicy: BeyondWindowPolicy;
  compactAfterBytes?: number;
  syncDurability: boolean;
  procedures: ProcNode[];
  channels: ChannelSpec[];
  workflow?: WorkflowSpec;
  run?: WorkflowDefinition['run'];
}

/**
 * Define a procedure.
 *
 * The declaration's own shape carries the inference: the handler's first
 * parameter is the input type, its return type the output. A handler that
 * emits through the context returns nothing, and names its output type through
 * the definition's type arguments.
 */
export function defineProcedure<TIn, TOut = never, TState = void, TChange = never>(
  definition: ProcedureDefinition<TIn, TOut, TState, TChange>,
): Procedure<TIn, TOut, TState, TChange> {
  if (!definition.name || !definition.name.trim()) {
    throw new DeclarationError('<procedure>', null, 'a procedure declares a non-empty name');
  }
  if (typeof definition.handle !== 'function') {
    throw new DeclarationError('<procedure>', definition.name, 'a procedure declares a handler');
  }
  // State without a fold is legitimate: a value seeded at start and read but
  // never mutated has nothing to journal, and `ctx.change` already refuses at
  // the point of use. A fold with no state to fold into is not.
  if (!definition.initialState && definition.applyChange) {
    throw new DeclarationError(
      '<procedure>',
      definition.name,
      'declares applyChange but no initialState; there is nothing for a change to fold into',
    );
  }
  if (definition.entities && definition.dynamicEntities) {
    throw new DeclarationError(
      '<procedure>',
      definition.name,
      'declares both a static key set and dynamic entities; the two modes are exclusive',
    );
  }
  if (definition.entities && definition.entities.keys.length === 0) {
    throw new DeclarationError(
      '<procedure>',
      definition.name,
      'declares an empty static key set; a static procedure names its entities',
    );
  }
  // A state that cannot be encoded cannot be restored, and the first anyone
  // would learn of it is a restart. Walk the declared starting state now.
  if (definition.initialState && !definition.stateCodec) {
    assertRepresentable(`${definition.name}.state`, definition.initialState(undefined));
  }
  return {
    definition,
    name: definition.name,
  } as Procedure<TIn, TOut, TState, TChange>;
}

/** One pending edge's declared options, accumulated by the fluent calls. */
interface PendingEdge {
  /** Overrides the derived channel name, never the consuming node's identity. */
  channelName?: string;
  transforms: Array<(value: unknown) => unknown>;
  keyBy?: (value: unknown) => string;
  overflow?: OverflowPolicy;
  retryMax?: number;
  retryBackoffMs?: number;
  keyed?: boolean;
  /** This handle's chain in its channel's record, absent until a link joins. */
  chain?: number;
  /** How much of that chain is this handle's: a later link forks past it. */
  chainLength?: number;
}

class Builder implements ChannelBuilder {
  readonly procedures: ProcNode[] = [];
  readonly channels: ChannelSpec[] = [];
  effectivelyOnceFlag = false;
  checkpointAckFlag = false;
  checkpointEveryValue?: number;
  checkpointOutputsValue: 'as_produced' | 'at_checkpoint' = 'as_produced';
  checkpointMaxDelayMsValue?: number;
  correctionWindowMsValue?: number;
  beyondWindowPolicyValue: BeyondWindowPolicy = 'process_as_arrival';
  compactAfterBytesValue?: number;
  syncDurabilityFlag = false;

  constructor(readonly factoryId: string) {}

  effectivelyOnce(): ChannelBuilder {
    this.effectivelyOnceFlag = true;
    return this;
  }

  // Each of the three writes its own raw flag and nothing else: any checkpoint
  // key implies the acknowledgement granularity, and the core is where that is
  // stated.
  checkpointAck(): ChannelBuilder {
    this.checkpointAckFlag = true;
    return this;
  }

  checkpointEvery(n: number): ChannelBuilder {
    this.checkpointEveryValue = n;
    return this;
  }

  outputsAtCheckpoint(maxDelayMs?: number): ChannelBuilder {
    this.checkpointOutputsValue = 'at_checkpoint';
    if (maxDelayMs !== undefined) this.checkpointMaxDelayMsValue = maxDelayMs;
    return this;
  }

  correctionWindow(ms: number): ChannelBuilder {
    this.correctionWindowMsValue = ms;
    return this;
  }

  beyondWindowPolicy(policy: BeyondWindowPolicy): ChannelBuilder {
    this.beyondWindowPolicyValue = policy;
    return this;
  }

  compactAfterBytes(bytes: number): ChannelBuilder {
    this.compactAfterBytesValue = bytes;
    return this;
  }

  syncDurability(): ChannelBuilder {
    this.syncDurabilityFlag = true;
    return this;
  }

  input<T>(name: string, decode?: (bytes: Uint8Array) => T): Stream<T> {
    const channel: ChannelSpec = {
      index: this.channels.length,
      name,
      kind: 'entry',
      chains: [],
      chainEdges: new Map(),
      consumers: [],
      overflow: 'drop_nothing',
      keyed: false,
      decode: decode as ((bytes: Uint8Array) => unknown) | undefined,
    };
    this.channels.push(channel);
    return new EdgeStream<T>(this, { source: 'entry', channel }, { transforms: [] });
  }

  /**
   * The node for a procedure. Two unnamed mentions of one procedure are one
   * node — the naming convention every SDK mirrors — so a diamond that fans
   * out and back in has one instance, not two.
   */
  nodeFor(procedure: Procedure<unknown, unknown, unknown, unknown>, name?: string): ProcNode {
    const identity = name ?? procedure.name;
    const existing = this.procedures.find(
      (node) => node.name === identity && node.procedure === procedure,
    );
    if (existing) return existing;
    const collision = this.procedures.find((node) => node.name === identity);
    if (collision) {
      // The node table is keyed by identity and physically cannot hold two, so
      // the record the core would judge is unwritable. The refusal answers
      // under the core's own name all the same.
      throw new DeclarationError(
        this.factoryId,
        identity,
        'two different procedures claim this node identity; name one of them with ' +
          '`named(...)` so each node has its own',
        ['NodeIdentityCollision'],
      );
    }
    const node: ProcNode = { index: this.procedures.length, name: identity, procedure };
    this.procedures.push(node);
    return node;
  }

  /**
   * The channel this node produces on, created on first use so a leaf node
   * never carries a phantom channel. Named by the node's identity until a
   * `named(...)` override, and reused by every later call on a handle whose
   * source is this node — which is what makes a fan-out one channel with two
   * consumers rather than two channels claiming one producer.
   */
  outputChannelFor(node: ProcNode): ChannelSpec {
    if (node.output) return node.output;
    const channel: ChannelSpec = {
      index: this.channels.length,
      name: node.name,
      kind: 'inter_proc',
      producer: node.index,
      chains: [],
      chainEdges: new Map(),
      consumers: [],
      overflow: 'drop_nothing',
      keyed: false,
    };
    this.channels.push(channel);
    node.output = channel;
    return channel;
  }
}

/** Where a stream's values come from: an entry channel, or a procedure. */
type Source = { source: 'entry'; channel: ChannelSpec } | { source: 'procedure'; node: ProcNode };

class EdgeStream<T> implements Stream<T> {
  constructor(
    private readonly builder: Builder,
    private readonly from: Source,
    private readonly pending: PendingEdge,
  ) {}

  private with(next: Partial<PendingEdge>): Stream<T> {
    return new EdgeStream<T>(this.builder, this.from, { ...this.pending, ...next });
  }

  /**
   * Record one link on this handle's chain, forking when another handle has
   * already extended it — which is what lets a shared prefix branch without
   * the arms colliding. Answers the chain fields the next handle carries.
   */
  private record(channel: ChannelSpec, link: Link): { chain: number; chainLength: number } {
    const { chain, chainLength = 0 } = this.pending;
    if (chain === undefined) {
      channel.chains.push([link]);
      return { chain: channel.chains.length - 1, chainLength: 1 };
    }
    const links = channel.chains[chain] as Link[];
    if (links.length === chainLength) {
      links.push(link);
      return { chain, chainLength: links.length };
    }
    channel.chains.push([...links.slice(0, chainLength), link]);
    return { chain: channel.chains.length - 1, chainLength: chainLength + 1 };
  }

  /**
   * This handle with one more chain link — the one place a transform's or a
   * key's result handle is built, so every link the author declared reaches
   * the record whether or not an `.into(...)` ever closes it.
   */
  private relink<U>(next: Partial<PendingEdge>, link: Link): Stream<U> {
    const channel = this.channel();
    return new EdgeStream<U>(this.builder, this.from, {
      ...this.pending,
      ...next,
      ...this.record(channel, link),
    });
  }

  named(name: string): Stream<T> {
    return this.with({ channelName: name });
  }

  map<U>(transform: (value: T) => U): Stream<U> {
    return this.relink<U>(
      { transforms: [...this.pending.transforms, transform as (value: unknown) => unknown] },
      { link: 'transform' },
    );
  }

  filter(predicate: (value: T) => boolean): Stream<T> {
    return this.relink<T>(
      {
        transforms: [
          ...this.pending.transforms,
          (value: unknown) => (predicate(value as T) ? value : DROPPED),
        ],
      },
      { link: 'transform' },
    );
  }

  keyBy(key: (value: T) => string): Stream<T> {
    return this.relink<T>({ keyBy: key as (value: unknown) => string }, { link: 'key' });
  }

  onFull(policy: OverflowPolicy): Stream<T> {
    return this.with({ overflow: policy });
  }

  retryMax(attempts: number): Stream<T> {
    return this.with({ retryMax: attempts });
  }

  retryBackoffMs(ms: number): Stream<T> {
    return this.with({ retryBackoffMs: ms });
  }

  keyed(): Stream<T> {
    return this.with({ keyed: true });
  }

  branch(arms: Array<(edge: Stream<T>) => void>): Stream<T> {
    // The sugar's arity is recorded, never judged: each arm wires itself with
    // `.into(...)` off this same handle, so the arms are ordinary chains.
    this.channel().chains.push([{ link: 'branch', arms: arms.length }]);
    for (const arm of arms) arm(this);
    return this;
  }

  /**
   * The channel this handle's values leave on, with everything the fluent
   * calls put aside applied to it. An entry handle already has one; a
   * procedure handle's is created on first use and reused after.
   */
  private channel(): ChannelSpec {
    const channel =
      this.from.source === 'entry'
        ? this.from.channel
        : this.builder.outputChannelFor(this.from.node);
    const rename = this.pending.channelName;
    if (rename !== undefined && channel.name !== rename) {
      // The override is recorded once and applied here too, so the name this
      // host addresses the channel by is the one the core derives from the
      // record — for every terminator, not for all but the last.
      channel.chains.push([{ link: 'named', name: rename }]);
      channel.name = rename;
    }
    applyPending(channel, this.pending);
    return channel;
  }

  into<TOut, TState, TChange>(
    procedure: Procedure<T, TOut, TState, TChange>,
    nodeName?: string,
  ): Stream<TOut> {
    const node = this.builder.nodeFor(
      procedure as unknown as Procedure<unknown, unknown, unknown, unknown>,
      nodeName,
    );
    const channel = this.channel();
    const { chain } = this.record(channel, { link: 'into', procedure: node.index });
    // The transforms and the key belong to this edge, not to the channel: one
    // channel may feed several procedures, and a chain declared before one
    // `into` must not reach another. The edge is held under the chain that
    // declared it and wired by `validate` from the core's own answer, so a
    // re-declaration is judged there rather than here.
    channel.chainEdges.set(chain, {
      procedure: node.index,
      transforms: this.pending.transforms,
      keyBy: this.pending.keyBy,
    });

    return new EdgeStream<TOut>(this.builder, { source: 'procedure', node }, { transforms: [] });
  }

  sink(
    nameOrEncode?: string | ((value: T) => Uint8Array),
    encode?: (value: T) => Uint8Array,
  ): Stream<T> {
    // One method, two positions: a string names the channel, a function is the
    // serializer. Telling them apart here is what keeps the serializer-only
    // form from spelling `sink(undefined, encode)`.
    const name = typeof nameOrEncode === 'string' ? nameOrEncode : undefined;
    const serialize = typeof nameOrEncode === 'function' ? nameOrEncode : encode;
    if (this.from.source !== 'procedure') {
      // Not a topology rule the core states: a record whose entry channel
      // carries a sink derives a plugin channel with no producer, and there is
      // nothing in it that says which end fed the plugin.
      throw new DeclarationError(
        this.builder.factoryId,
        name ?? '<derived>',
        'a plugin sink is fed by a procedure; wire the entry channel into one first',
      );
    }
    const channel = this.channel();
    // A chain of its own: the sink terminates the channel rather than this
    // handle, so `.into(...)` still chains after it and the value reaches both
    // the consumer mailboxes and the sink. Terminality is the absence of
    // further wiring, not a different method.
    channel.chains.push(name === undefined ? [{ link: 'sink' }] : [{ link: 'sink', name }]);
    // The sink names the channel: `sink('x')` IS the name, and `sink()` leaves
    // it derived. One string, which is what the core answers back.
    if (name !== undefined) channel.name = name;
    if (channel.sink === undefined) {
      channel.sink = channel.name;
      channel.sinkEncode = serialize as ((value: unknown) => Uint8Array) | undefined;
    }
    return this;
  }
}

function applyPending(channel: ChannelSpec, pending: PendingEdge): void {
  if (pending.overflow) channel.overflow = pending.overflow;
  if (pending.retryMax !== undefined) channel.retryMax = pending.retryMax;
  if (pending.retryBackoffMs !== undefined) channel.retryBackoffMs = pending.retryBackoffMs;
  if (pending.keyed) channel.keyed = true;
}

function buildGraph(factoryId: string, channels: OperativeDefinition['channels']): Builder {
  const builder = new Builder(factoryId);
  channels(builder);
  return builder;
}

/** Define a stream operative from its channels graph. */
export function defineOperative(definition: OperativeDefinition): ClassSpec {
  const builder = buildGraph(definition.factoryId, definition.channels);
  return {
    factoryId: definition.factoryId,
    kind: 'stream',
    persistent: definition.persistent ?? true,
    effectivelyOnce: builder.effectivelyOnceFlag,
    checkpointAck: builder.checkpointAckFlag,
    checkpointEvery: builder.checkpointEveryValue,
    checkpointOutputs: builder.checkpointOutputsValue,
    checkpointMaxDelayMs: builder.checkpointMaxDelayMsValue,
    correctionWindowMs: builder.correctionWindowMsValue,
    beyondWindowPolicy: builder.beyondWindowPolicyValue,
    compactAfterBytes: builder.compactAfterBytesValue,
    syncDurability: builder.syncDurabilityFlag,
    procedures: builder.procedures,
    channels: builder.channels,
  };
}

/** Define a workflow operative — a run-sugar body, or a structured chain. */
export function defineWorkflow(definition: WorkflowDefinition): ClassSpec {
  if (definition.run && definition.channels) {
    throw new DeclarationError(
      definition.factoryId,
      null,
      'declares both a run body and a channels graph; a workflow is one or the other',
    );
  }
  if (!definition.run && !definition.channels) {
    throw new DeclarationError(
      definition.factoryId,
      null,
      'declares neither a run body nor a channels graph',
    );
  }
  const builder = definition.channels
    ? buildGraph(definition.factoryId, definition.channels)
    : new Builder(definition.factoryId);
  return {
    factoryId: definition.factoryId,
    kind: 'workflow',
    persistent: definition.persistent ?? true,
    effectivelyOnce: builder.effectivelyOnceFlag,
    checkpointAck: false,
    checkpointOutputs: 'as_produced',
    beyondWindowPolicy: 'process_as_arrival',
    syncDurability: definition.syncDurability ?? false,
    procedures: builder.procedures,
    channels: builder.channels,
    workflow: {
      loopMode: definition.loopMode ?? 'once',
      rollbackMode: definition.rollbackMode ?? 'all_or_nothing',
      retryAttempts: definition.retryAttempts,
      retryBackoffMs: definition.retryBackoffMs,
      barrierRetryAttempts: definition.barrierRetryAttempts,
      maxDurationMs: definition.maxDurationMs,
      wakeAfterMs: definition.wakeAfterMs,
    },
    run: definition.run,
  };
}

export type { Codec };

/**
 * Run an edge's transform chain, stopping at the first drop.
 *
 * One implementation for the three places a chain runs — an entry payload
 * before its handler, a payload at the prepare crossing, and an emit on its way
 * out — so a filter cannot mean one thing on one path and another elsewhere.
 */
export function applyTransforms(
  transforms: Array<(value: unknown) => unknown>,
  value: unknown,
): unknown {
  let current = value;
  for (const transform of transforms) {
    current = transform(current);
    if (current === DROPPED) return DROPPED;
  }
  return current;
}

/**
 * Whether this class's producers release their values untransformed.
 *
 * A structured workflow's stage releases ONE value per emit and the driver
 * fans it out to every consumer itself, so each consumer edge — its transforms
 * and its key — belongs to the consumer. The producer and the consumer read
 * this one predicate, so they cannot disagree about which side applies the
 * edge.
 */
export function isStructuredWorkflow(spec: ClassSpec): boolean {
  return spec.kind === 'workflow' && spec.channels.length > 0;
}
