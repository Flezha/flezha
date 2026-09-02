/**
 * The procedure test harness.
 *
 * A defined handler is an ordinary function: a plain unit test can call it
 * with no runner started and no controller running. What the harness adds is
 * the parts a runtime would otherwise own — the entity's state, the recorded
 * change log, the emits, the clock a timer fires on — and the replay-equality
 * gate, which is the conformance gate for every procedure.
 *
 * It drives the same `Invocation` the dispatcher does, so what a test exercises
 * is the production path rather than a second one written to agree with it.
 */

import { isDeepStrictEqual } from 'node:util';

import { defineOperative, type ChannelSpec, type ClassSpec, type ProcNode } from './authoring.js';
import { decodeValue } from './codec.js';
import { Invocation } from './ctx.js';
import { invocation } from './determinism.js';
import { foldRestore, rawState, type RestoreBody } from './state.js';
import type { Procedure, TimerKind, TimerFiring } from './types.js';

/** The recorded change log did not rebuild the state the handler produced. */
export class ReplayMismatch extends Error {
  constructor(
    readonly procedure: string,
    readonly live: unknown,
    readonly replayed: unknown,
  ) {
    super(
      `${procedure}: folding the recorded change log against a fresh initial state did not ` +
        `reproduce the handler's state. The engine rebuilds an entity exactly this way on a ` +
        `warm restart, so the difference is what a restart would lose.\n` +
        `  live:     ${JSON.stringify(live, replacer)}\n` +
        `  replayed: ${JSON.stringify(replayed, replacer)}`,
    );
    this.name = 'ReplayMismatch';
  }
}

function replacer(_key: string, value: unknown): unknown {
  return typeof value === 'bigint' ? `${value}n` : value;
}

export interface HarnessOptions<TState> {
  entityKey?: string;
  /** Override the declared starting state. */
  state?: TState;
  /** The value `ctx.now()` serves until an input or an advance moves it. */
  now?: bigint;
  operative?: string;
}

/** A restore payload, as the core would hand one across. */
export interface RestorePayload {
  snapshot?: Uint8Array;
  changes?: Uint8Array[];
  /** Stamp every body as written at this version, so a migration is exercised. */
  fromVersion?: number;
}

/** One invocation's recorded effects. */
export interface Invoked<TOut, TChange> {
  emits: TOut[];
  changes: TChange[];
  timers: Array<{ kind: TimerKind; scheduledAt: bigint; register: boolean }>;
  checkpoint: boolean;
}

export class ProcedureTestHarness<TIn, TOut, TState, TChange> {
  private state: TState;
  private clock: bigint;
  private readonly log: TChange[] = [];
  private readonly emitted: TOut[] = [];
  private readonly spec: ClassSpec;
  private readonly node: ProcNode;
  private readonly outgoing: ChannelSpec[];

  private constructor(
    private readonly procedure: Procedure<TIn, TOut, TState, TChange>,
    private readonly options: HarnessOptions<TState>,
    state: TState,
  ) {
    this.state = state;
    this.clock = options.now ?? 0n;
    // Wired by the production builder rather than by a hand-written topology
    // literal: one entry channel in, one plugin channel out. A field the
    // declaration gains later reaches the harness for free, and the emits the
    // harness reads back have gone through the declared codec exactly as they
    // would on the wire.
    this.spec = defineOperative({
      factoryId: `harness_${procedure.name}`,
      channels: (c) =>
        c
          .input('harness_in')
          .into(procedure as never)
          .sink('harness_out'),
    });
    this.node = this.spec.procedures[0] as ProcNode;
    this.outgoing = this.spec.channels.filter((channel) => channel.producer !== undefined);
  }

  /** A harness over the procedure's declared starting state. */
  static of<TIn, TOut, TState, TChange>(
    procedure: Procedure<TIn, TOut, TState, TChange>,
    options: HarnessOptions<TState> = {},
  ): ProcedureTestHarness<TIn, TOut, TState, TChange> {
    const initial =
      options.state ?? (procedure.definition.initialState?.(options.entityKey) as TState);
    return new ProcedureTestHarness(procedure, options, initial);
  }

  /**
   * A harness restored from a durable payload — the shape a warm restart
   * hands across, including one stamped at an older schema version, so a
   * migration is provable without a controller.
   */
  static withPayload<TIn, TOut, TState, TChange>(
    procedure: Procedure<TIn, TOut, TState, TChange>,
    payload: RestorePayload,
    options: HarnessOptions<TState> = {},
  ): ProcedureTestHarness<TIn, TOut, TState, TChange> {
    const body = (bytes: Uint8Array): RestoreBody => ({
      bytes,
      fromVersion: payload.fromVersion,
    });
    // The same fold the dispatcher runs, so what a test constructs from is
    // what a warm restart would have produced rather than an agreement with it.
    const state = foldRestore(
      procedure.definition as never,
      {
        snapshot: payload.snapshot ? body(payload.snapshot) : undefined,
        changes: (payload.changes ?? []).map(body),
      },
      options.entityKey,
      procedure.name,
    ) as TState;
    return new ProcedureTestHarness(procedure, options, state);
  }

  private context(now: bigint): Invocation {
    return new Invocation({
      spec: this.spec,
      node: this.node,
      outgoing: this.outgoing,
      operative: this.options.operative ?? `harness_${this.procedure.name}`,
      entityKey: this.options.entityKey,
      changeSeq: BigInt(this.log.length),
      now,
      replay: false,
      state: this.state,
      bridge: {
        spawnWorkflow: async () => 'harness_no_controller',
      },
    });
  }

  private collect(ctx: Invocation, returned: unknown): Invoked<TOut, TChange> {
    if (returned !== undefined) ctx.emit(returned);
    this.state = rawState(ctx.currentState) as TState;

    const emits = ctx.outputs.map((output) => decodeValue<TOut>(output.payload));
    const changes = ctx.changes.map((bytes) =>
      this.procedure.definition.changeCodec
        ? this.procedure.definition.changeCodec.decode(bytes)
        : decodeValue<TChange>(bytes),
    );
    this.emitted.push(...emits);
    this.log.push(...changes);
    return {
      emits,
      changes,
      timers: ctx.timers.map((timer) => ({ ...timer })),
      checkpoint: ctx.checkpointed,
    };
  }

  /**
   * Apply one input at the harness clock.
   *
   * Run inside the same async context the dispatcher establishes, so a handler
   * that reads an ambient clock is reported here rather than only in
   * production.
   */
  async input(value: TIn): Promise<Invoked<TOut, TChange>> {
    const ctx = this.context(this.clock);
    const returned = await invocation.run({ procedure: this.procedure.name }, () =>
      this.procedure.definition.handle(value, ctx as never),
    );
    return this.collect(ctx, returned);
  }

  /** Fire a timer. `ctx.now()` inside the callback is the scheduled time. */
  async fireTimer(kind: TimerKind, scheduledAt: bigint): Promise<Invoked<TOut, TChange>> {
    const onTimer = this.procedure.definition.onTimer;
    if (!onTimer) {
      throw new Error(
        `${this.procedure.name} declares no onTimer; a procedure that registers a timer ` +
          'handles its firing',
      );
    }
    const firing: TimerFiring = { kind, scheduledAt };
    const ctx = this.context(scheduledAt);
    await invocation.run({ procedure: this.procedure.name }, () => onTimer(firing, ctx as never));
    return this.collect(ctx, undefined);
  }

  /** Move the clock the next input is served at. */
  advanceTo(now: bigint): void {
    this.clock = now;
  }

  /** Move the clock forward. */
  advanceBy(delta: bigint): void {
    this.clock += delta;
  }

  get now(): bigint {
    return this.clock;
  }

  /** The entity's state as the handler left it. */
  get currentState(): TState {
    return this.state;
  }

  /** Every change recorded so far, in order. */
  get recordedChanges(): TChange[] {
    return [...this.log];
  }

  /** Every value emitted so far, in order. */
  get emits(): TOut[] {
    return [...this.emitted];
  }

  /**
   * The determinism gate: fold the recorded change log against a freshly
   * constructed initial state and require the result to equal the live one.
   *
   * A handler that mutates its state outside a recorded change passes every
   * live assertion and loses the difference at the next restart. This is the
   * only check that sees it.
   */
  replayEquals(): void {
    const definition = this.procedure.definition;
    const replayed = definition.initialState?.(this.options.entityKey) as TState;
    for (const change of this.log) definition.applyChange?.(replayed, change);
    if (!isDeepStrictEqual(replayed, this.state)) {
      throw new ReplayMismatch(this.procedure.name, this.state, replayed);
    }
  }
}
