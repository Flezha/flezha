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
}

export type ChannelKind = 'entry' | 'inter_proc' | 'plugin';

/** A channel as the graph holds it. Its position in the list IS its id. */
export interface ChannelSpec {
  index: number;
  name: string;
  kind: ChannelKind;
  producer?: number;
  consumers: number[];
  overflow: OverflowPolicy;
  retryMax?: number;
  retryBackoffMs?: number;
  keyed: boolean;
  /** Producer-side transforms, applied in declaration order. */
  transforms: Array<(value: unknown) => unknown>;
  /** Dynamic routing key for this channel's consumer. */
  keyBy?: (value: unknown) => string;
  /** Entry payload decoder; absent means the MessagePack default. */
  decode?: (bytes: Uint8Array) => unknown;
  /** Plugin payload encoder; absent means the MessagePack default. */
  encode?: (value: unknown) => Uint8Array;
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
  ackGranularity: 'per_input' | 'checkpoint';
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
}

class Builder implements ChannelBuilder {
  readonly procedures: ProcNode[] = [];
  readonly channels: ChannelSpec[] = [];
  effectivelyOnceFlag = false;
  ackGranularity: 'per_input' | 'checkpoint' = 'per_input';
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

  checkpointEvery(n: number): ChannelBuilder {
    this.effectivelyOnceFlag = true;
    this.ackGranularity = 'checkpoint';
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
      consumers: [],
      overflow: 'drop_nothing',
      keyed: false,
      transforms: [],
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
      throw new DeclarationError(
        this.factoryId,
        identity,
        'two different procedures claim this node identity; name one of them with ' +
          '`named(...)` so each node has its own',
      );
    }
    const node: ProcNode = { index: this.procedures.length, name: identity, procedure };
    this.procedures.push(node);
    return node;
  }

  addChannel(spec: Omit<ChannelSpec, 'index'>): ChannelSpec {
    const channel: ChannelSpec = { ...spec, index: this.channels.length };
    this.channels.push(channel);
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

  named(name: string): Stream<T> {
    return this.with({ channelName: name });
  }

  map<U>(transform: (value: T) => U): Stream<U> {
    return new EdgeStream<U>(this.builder, this.from, {
      ...this.pending,
      transforms: [...this.pending.transforms, transform as (value: unknown) => unknown],
    });
  }

  filter(predicate: (value: T) => boolean): Stream<T> {
    return this.with({
      transforms: [
        ...this.pending.transforms,
        (value: unknown) => (predicate(value as T) ? value : DROPPED),
      ],
    });
  }

  keyBy(key: (value: T) => string): Stream<T> {
    return this.with({ keyBy: key as (value: unknown) => string });
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
    if (arms.length < 2) {
      throw new DeclarationError(
        this.builder.factoryId,
        this.pending.channelName ?? sourceName(this.from),
        `branch(...) requires at least two arms; ${arms.length} given — a single arm is ` +
          '`into(...)` written the long way',
      );
    }
    for (const arm of arms) arm(this);
    return this;
  }

  into<TOut, TState, TChange>(
    procedure: Procedure<T, TOut, TState, TChange>,
    nodeName?: string,
  ): Stream<TOut> {
    const node = this.builder.nodeFor(
      procedure as unknown as Procedure<unknown, unknown, unknown, unknown>,
      nodeName,
    );

    if (this.from.source === 'entry') {
      // One entry channel may feed several procedures; the transforms and the
      // key belong to the channel, which is what the prepare crossing names.
      const channel = this.from.channel;
      channel.consumers.push(node.index);
      applyPending(channel, this.pending);
    } else {
      this.builder.addChannel({
        name: this.pending.channelName ?? `${this.from.node.name}__${node.name}`,
        kind: 'inter_proc',
        producer: this.from.node.index,
        consumers: [node.index],
        overflow: this.pending.overflow ?? 'drop_nothing',
        retryMax: this.pending.retryMax,
        retryBackoffMs: this.pending.retryBackoffMs,
        keyed: this.pending.keyed ?? false,
        transforms: this.pending.transforms,
        keyBy: this.pending.keyBy,
      });
    }

    return new EdgeStream<TOut>(this.builder, { source: 'procedure', node }, { transforms: [] });
  }

  sink(name: string, encode?: (value: T) => Uint8Array): void {
    if (this.from.source !== 'procedure') {
      throw new DeclarationError(
        this.builder.factoryId,
        name,
        'a plugin sink is fed by a procedure; wire the entry channel into one first',
      );
    }
    this.builder.addChannel({
      // The channel keeps its `.named()` name; `name` is the sink's, which is
      // the default when the edge was not named. The channel name is what a
      // frame carries and what selects a forward target's entry channel.
      name: this.pending.channelName ?? name,
      kind: 'plugin',
      producer: this.from.node.index,
      consumers: [],
      overflow: this.pending.overflow ?? 'drop_nothing',
      retryMax: this.pending.retryMax,
      retryBackoffMs: this.pending.retryBackoffMs,
      keyed: this.pending.keyed ?? false,
      transforms: this.pending.transforms,
      keyBy: this.pending.keyBy,
      encode: encode as ((value: unknown) => Uint8Array) | undefined,
    });
  }
}

function sourceName(from: Source): string {
  return from.source === 'entry' ? from.channel.name : from.node.name;
}

function applyPending(channel: ChannelSpec, pending: PendingEdge): void {
  if (pending.overflow) channel.overflow = pending.overflow;
  if (pending.retryMax !== undefined) channel.retryMax = pending.retryMax;
  if (pending.retryBackoffMs !== undefined) channel.retryBackoffMs = pending.retryBackoffMs;
  if (pending.keyed) channel.keyed = true;
  if (pending.keyBy) channel.keyBy = pending.keyBy;
  channel.transforms.push(...pending.transforms);
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
    ackGranularity: builder.ackGranularity,
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
    effectivelyOnce: false,
    ackGranularity: 'per_input',
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
