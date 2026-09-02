/**
 * The authoring surface's public types.
 *
 * Handler input and output types drive the graph: `.into(procedure)` accepts a
 * procedure whose declared input the producing edge's value satisfies, so a
 * mismatched edge is a compile error rather than a registration-time one.
 */

import type { Codec } from './codec.js';

/**
 * What a `filter` yields for a value it rejects.
 *
 * A dropped value is not `undefined` or `null` — both are values a handler may
 * legitimately carry — so the marker is a symbol nothing else can produce. It
 * stops the transform chain: the item is consumed without reaching a handler,
 * and at the prepare crossing it answers no key, which is how the core learns
 * not to route it.
 */
export const DROPPED: unique symbol = Symbol('flezha.dropped');

export type OverflowPolicy = 'drop_nothing' | 'drop_newest' | 'drop_oldest';
export type ClockKind = 'wall' | 'logical' | 'event_time';
export type TimerKind = 'event_time' | 'processing_time';
export type BeyondWindowPolicy = 'process_as_arrival' | 'drop';
export type LoopMode = 'once' | 'per_input' | 'chained';
export type RollbackMode = 'all_or_nothing' | 'keep_progress';
export type IfRunning = 'deliver' | 'error';

/** What the controller answered a spawn request with. */
export type SpawnOutcome = string;

/** A timer firing. `now` is the SCHEDULED time, never the wall clock. */
export interface TimerFiring {
  kind: TimerKind;
  scheduledAt: bigint;
}

export interface EmitOptions {
  /** Routing key for the destination. Defaults to this entity's own key. */
  key?: string;
  /** Which declared output channel, when the procedure has more than one. */
  channel?: string;
  /** User metadata. Keys under `flz.` are reserved and refused. */
  metadata?: Record<string, string>;
}

/**
 * The invocation context. Every deterministic effect is served from here — the
 * ambient clock and ambient randomness are not replay-stable, and the SDK
 * warns when a handler reaches for them.
 */
export interface Ctx<TOut = never, TState = void, TChange = never> {
  /** The entity's state. Assigning to a field is refused; record a change. */
  readonly state: TState;
  /** This entity's key, or undefined for a single-entity procedure. */
  readonly entityKey: string | undefined;
  /** True while the controller is re-forwarding a journaled slice. */
  readonly replay: boolean;

  /** The core-evaluated declared clock for this item. Never ambient time. */
  now(): bigint;
  /** A replay-stable identifier, distinct per invocation within the entity. */
  stableId(): string;

  /** Record one declared change: journaled, then folded into the state. */
  change(change: TChange): void;

  /** Emit one output value on the procedure's declared output. */
  emit(value: TOut, options?: EmitOptions): void;
  /** Emit onto a named channel — the spelling for a procedure with several. */
  emitTo(channel: string, value: unknown, options?: EmitOptions): void;

  /** Register a processing-time timer `ms` from `now()`. Returns its time. */
  timerAfter(ms: number | bigint): bigint;
  /** Register an event-time timer at an absolute time. Returns it. */
  timerAtEventTime(at: number | bigint): bigint;
  /** Cancel a registered timer by the value its registration returned. */
  cancelTimer(kind: TimerKind, at: bigint): void;

  /** Close the open checkpoint interval (checkpoint ack-granularity classes). */
  checkpoint(): void;

  /** Spawn or deliver to a workflow instance. */
  spawnWorkflow(
    instanceId: string,
    factoryId: string,
    payload: unknown,
    options?: { ifRunning?: IfRunning },
  ): Promise<SpawnOutcome>;
}

/** The context a run-sugar workflow body receives. */
export interface WorkflowCtx {
  readonly instanceId: string;
  /** Emit one output payload. Best-effort and at-least-once. */
  emit(value: unknown): void;
  /** The next inbox item, or null on timeout. */
  recv(timeoutMs?: number): Promise<unknown | null>;
  /** Whether the body's cancellation token has tripped. */
  isCancelled(): boolean;
  /** Set the terminal result; the body is `completed` once this is called. */
  complete(result?: unknown): void;
  now(): bigint;
}

/** Static entity mode: a fixed key set, plus the fan-out an input belongs to. */
export interface StaticEntities<TIn> {
  keys: string[];
  entitiesFor?: (input: TIn) => string[];
}

/** Dynamic entity mode: entities appear on first key, with optional limits. */
export interface DynamicEntities {
  idleTtlMs?: number;
  maxEntities?: number;
}

export interface ProcedureDefinition<TIn, TOut, TState, TChange> {
  /** The procedure's node identity within its operative. */
  name: string;

  /** Fixed key set. Dynamic keying is declared on the wiring with `keyBy`. */
  entities?: StaticEntities<TIn>;
  /** Lazy entities. The key comes from the wiring's `keyBy`. */
  dynamicEntities?: DynamicEntities;

  clock?: ClockKind;
  /** Required by an event-time clock: the event time of an input. */
  eventTime?: (input: TIn) => bigint | number;

  mailboxCapacity?: number;

  /** Stamped into every durable record this procedure writes. */
  schemaVersion?: number;
  /** The oldest version this procedure still reads. Defaults to its own. */
  minReadableSchemaVersion?: number;
  migrateState?: (fromVersion: number, bytes: Uint8Array) => TState;
  migrateChange?: (fromVersion: number, bytes: Uint8Array) => TChange;

  /** The entity's starting state. Its absence declares a stateless procedure. */
  initialState?: (entityKey?: string) => TState;
  /** The replay path: fold one recorded change into the state. */
  applyChange?: (state: TState, change: TChange) => void;

  stateCodec?: Codec<TState>;
  changeCodec?: Codec<TChange>;

  /**
   * The handler. A returned value is emitted; `undefined` emits nothing, which
   * is the warmup gate. A handler that emits more than once per input uses
   * `ctx.emit` and returns nothing.
   */
  handle: (
    input: TIn,
    ctx: Ctx<TOut, TState, TChange>,
  ) => TOut | undefined | void | Promise<TOut | undefined | void>;

  /** A per-entity deadline fired. `ctx.now()` is the scheduled time. */
  onTimer?: (firing: TimerFiring, ctx: Ctx<TOut, TState, TChange>) => void | Promise<void>;

  /** Saga compensation for this procedure's recorded forward outputs. */
  compensate?: (outputs: TOut[], ctx: Ctx<TOut, TState, TChange>) => void | Promise<void>;
}

/** A defined procedure. Opaque to authors beyond its identity and types. */
export interface Procedure<TIn, TOut = never, TState = void, TChange = never> {
  readonly definition: ProcedureDefinition<TIn, TOut, TState, TChange>;
  readonly name: string;
  /** Present only to carry the input and output types; never read. */
  readonly __types?: { input: TIn; output: TOut };
}

/** An edge carrying values of type `T` into whatever consumes it next. */
export interface Stream<T> {
  /**
   * Override the channel name this edge creates. The default is derived from
   * the producing node's identity.
   */
  named(name: string): Stream<T>;
  /** Transform every value on this edge. Evaluated producer-side. */
  map<U>(transform: (value: T) => U): Stream<U>;
  /**
   * Drop the values a predicate rejects, before this edge's key is computed.
   * A dropped value is consumed and reaches no handler.
   */
  filter(predicate: (value: T) => boolean): Stream<T>;
  /** Route by a computed key: the consumer becomes a dynamic keyed procedure. */
  keyBy(key: (value: T) => string): Stream<T>;
  onFull(policy: OverflowPolicy): Stream<T>;
  retryMax(attempts: number): Stream<T>;
  retryBackoffMs(ms: number): Stream<T>;
  /** Stamp `flz.entity` / `flz.seq` / `flz.key` onto this channel's plugin frames. */
  keyed(): Stream<T>;
  /**
   * Wire this edge into a procedure whose declared input accepts `T`.
   *
   * Two unnamed mentions of one procedure are one node, so a fan-out and
   * fan-back-in describes a diamond rather than two instances. `nodeName`
   * gives a second mention its own node.
   */
  into<TOut, TState, TChange>(
    procedure: Procedure<T, TOut, TState, TChange>,
    nodeName?: string,
  ): Stream<TOut>;
  /**
   * Fan out: each arm receives this same edge, so `branch` builds exactly the
   * topology the separate `into` statements would. At least two arms.
   */
  branch(arms: Array<(edge: Stream<T>) => void>): Stream<T>;
  /**
   * Attach this edge's channel to the named plugin sink, and return the
   * handle: `.sink(...).into(...)` wires a consumer behind the sink, so one
   * emit reaches both. A channel with no consumer behind it is the
   * plugin-bound terminal — terminality is the absence of further wiring,
   * not a different method.
   */
  sink(name: string, encode?: (value: T) => Uint8Array): Stream<T>;
}

export interface ChannelBuilder {
  /** Effectively-once delivery tier. */
  effectivelyOnce(): ChannelBuilder;
  /**
   * Acknowledge at the checkpoint rather than per input; the interval closes
   * only on an explicit `ctx.checkpoint()`. Requires `effectivelyOnce()`.
   */
  checkpointAck(): ChannelBuilder;
  /** Close a checkpoint interval every `n` inputs. Implies `checkpointAck()`. */
  checkpointEvery(n: number): ChannelBuilder;
  /**
   * Hold outputs until the interval closes, with an optional deadline.
   * Implies `checkpointAck()`.
   */
  outputsAtCheckpoint(maxDelayMs?: number): ChannelBuilder;
  /** Late-input correction window, in milliseconds. Event-time classes. */
  correctionWindow(ms: number): ChannelBuilder;
  beyondWindowPolicy(policy: BeyondWindowPolicy): ChannelBuilder;
  compactAfterBytes(bytes: number): ChannelBuilder;
  /** Synchronous remote durability. Workflow classes only. */
  syncDurability(): ChannelBuilder;
  /** Declare a controller-fed entry channel carrying values of type `T`. */
  input<T>(name: string, decode?: (bytes: Uint8Array) => T): Stream<T>;
}

export interface OperativeDefinition {
  factoryId: string;
  /** Whether change bytes are flushed to the per-entity WAL. Default true. */
  persistent?: boolean;
  channels: (builder: ChannelBuilder) => void;
}

export interface WorkflowDefinition {
  factoryId: string;
  persistent?: boolean;
  loopMode?: LoopMode;
  rollbackMode?: RollbackMode;
  retryAttempts?: number;
  retryBackoffMs?: number;
  barrierRetryAttempts?: number;
  maxDurationMs?: number;
  wakeAfterMs?: number;
  syncDurability?: boolean;
  /** A run-sugar body: one imperative function. */
  run?: (args: unknown, ctx: WorkflowCtx) => unknown | Promise<unknown>;
  /** A structured chain over a channels graph. Mutually exclusive with `run`. */
  channels?: (builder: ChannelBuilder) => void;
}

/** A registration refusal: names the offending handler and the defect. */
export class DeclarationError extends Error {
  constructor(
    readonly factoryId: string,
    readonly handler: string | null,
    readonly defect: string,
  ) {
    super(handler ? `${factoryId}: ${handler}: ${defect}` : `${factoryId}: ${defect}`);
    this.name = 'DeclarationError';
  }
}
