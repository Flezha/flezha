/**
 * The invocation context: every deterministic effect a handler is allowed.
 *
 * The context accumulates one item's effects — recorded changes, outputs,
 * timer operations, the checkpoint flag — and the dispatcher turns them into
 * the `ItemEffect` the core folds. Nothing here reaches the wire on its own;
 * an emit is a value in a list until the completion carries it.
 */

import { encodeValue } from './codec.js';
import { applyTransforms } from './authoring.js';
import type { ChannelSpec, ClassSpec, ConsumerEdge, ProcNode } from './authoring.js';
import type { NativeItemEffect, NativeMeta, NativeOutput, NativeTimerOp } from './native.js';
import { entityIdentity, stableId } from './identity.js';
import { readOnlyState } from './state.js';
import { DROPPED, type Ctx, type EmitOptions, type IfRunning, type TimerKind } from './types.js';

/** Metadata keys the runtime owns. A handler that sets one is refused. */
const RESERVED_PREFIX = 'flz.';

export interface SpawnBridge {
  spawnWorkflow(
    instanceId: string,
    factoryId: string,
    payload: Buffer,
    ifRunning: string,
  ): Promise<string>;
}

export interface InvocationInput {
  spec: ClassSpec;
  node: ProcNode;
  /** Channels this procedure produces on, in declaration order. */
  outgoing: ChannelSpec[];
  operative: string;
  entityKey: string | undefined;
  changeSeq: bigint;
  now: bigint;
  replay: boolean;
  state: unknown;
  bridge: SpawnBridge;
}

export class EmitError extends Error {}

function metaList(metadata: Record<string, string> | undefined, procedure: string): NativeMeta[] {
  if (!metadata) return [];
  return Object.entries(metadata).map(([key, value]) => {
    if (key.startsWith(RESERVED_PREFIX)) {
      throw new EmitError(
        `${procedure}: metadata key \`${key}\` is inside the reserved \`${RESERVED_PREFIX}\` ` +
          'namespace, which the runtime stamps itself (entity identity, sequence, key and ' +
          'offset); choose a key outside it',
      );
    }
    return { key, value };
  });
}

/**
 * One invocation's context. Created per work item, discarded with it: the
 * per-invocation ordinal `stableId` counts is exactly this object's lifetime.
 */
export class Invocation implements Ctx<unknown, unknown, unknown> {
  readonly changes: Buffer[] = [];
  readonly outputs: NativeOutput[] = [];
  readonly timers: NativeTimerOp[] = [];
  checkpointed = false;

  private ordinal = 0;
  private emitOrdinal = 0;
  private identityBytes: Uint8Array | null = null;
  private live: unknown;

  constructor(private readonly input: InvocationInput) {
    this.live = input.state;
  }

  /** The state after this item's changes folded — what the entity keeps. */
  get currentState(): unknown {
    return this.live;
  }

  get state(): unknown {
    return readOnlyState(this.input.node.name, this.live);
  }

  get entityKey(): string | undefined {
    return this.input.entityKey;
  }

  get replay(): boolean {
    return this.input.replay;
  }

  now(): bigint {
    return this.input.now;
  }

  stableId(): string {
    if (!this.identityBytes) {
      // Lazily, because most invocations never ask and the identity is the
      // one part of the derivation that costs an allocation.
      this.identityBytes = entityIdentity(
        this.input.operative,
        this.input.node.name,
        this.input.entityKey ?? '',
      );
    }
    return stableId(this.identityBytes, this.input.changeSeq, this.ordinal++);
  }

  change(change: unknown): void {
    const definition = this.input.node.procedure.definition;
    if (!definition.applyChange) {
      throw new EmitError(
        `${this.input.node.name}: recorded a change but declares no applyChange; a change ` +
          'that cannot be folded cannot rebuild the state it describes',
      );
    }
    const codec = definition.changeCodec;
    this.changes.push(Buffer.from(codec ? codec.encode(change) : encodeValue(change)));
    // Fold it live through the same function the restore fold uses, so the
    // live path and the replay path cannot drift.
    definition.applyChange(this.live, change);
  }

  emit(value: unknown, options?: EmitOptions): void {
    const targets = options?.channel
      ? this.input.outgoing.filter((channel) => channel.name === options.channel)
      : this.input.outgoing;
    if (targets.length === 0) {
      throw new EmitError(
        options?.channel
          ? `${this.input.node.name}: emits on channel \`${options.channel}\`, which it does ` +
              'not produce on'
          : `${this.input.node.name}: emits but declares no output channel; wire its edge ` +
              'with .into(...) or .sink(...)',
      );
    }
    for (const channel of targets) {
      // A channel with no consumer releases nothing on the edge path: on a
      // workflow that is the plugin-bound terminal, and seeding the next stage
      // from it would hand the driver a terminal output.
      if (this.structuredWorkflow) {
        const edge = channel.consumers[0];
        if (edge) this.outputs.push(this.workflowOutput(channel, edge, value));
      } else {
        for (const edge of channel.consumers) {
          const output = this.edgeOutput(channel, edge, value, options);
          if (output) this.outputs.push(output);
        }
      }
      if (channel.sink !== undefined) this.outputs.push(this.pluginOutput(channel, value, options));
    }
  }

  /**
   * A structured workflow releases ONE value per emit: the driver fans it out
   * to every consumer itself, from the journaled handoff, so the host names
   * the producing channel and its first consumer and applies neither the
   * edge's transforms nor its key.
   */
  private get structuredWorkflow(): boolean {
    return this.input.spec.kind === 'workflow' && this.input.spec.channels.length > 0;
  }

  private workflowOutput(channel: ChannelSpec, edge: ConsumerEdge, value: unknown): NativeOutput {
    return {
      target: 'edge',
      channel: channel.index,
      consumer: edge.procedure,
      pluginChannel: undefined,
      key: undefined,
      eventTime: undefined,
      metadata: [],
      payload: Buffer.from(channel.encode ? channel.encode(value) : encodeValue(value)),
    };
  }

  emitTo(channel: string, value: unknown, options?: EmitOptions): void {
    this.emit(value, { ...options, channel });
  }

  /** One consumer's frame: its own transforms, its own key, its own clock. */
  private edgeOutput(
    channel: ChannelSpec,
    edge: ConsumerEdge,
    value: unknown,
    options: EmitOptions | undefined,
  ): NativeOutput | null {
    // Producer-side, in declaration order: the core routes on `(edge, key)`
    // and never evaluates a host closure.
    const transformed = applyTransforms(edge.transforms, value);
    if (transformed === DROPPED) return null;

    const key =
      options?.key ?? (edge.keyBy ? edge.keyBy(transformed) : undefined) ?? this.input.entityKey;

    const extractor = this.input.spec.procedures[edge.procedure]?.procedure.definition.eventTime;

    return {
      target: 'edge',
      channel: channel.index,
      consumer: edge.procedure,
      pluginChannel: undefined,
      key,
      eventTime: extractor ? BigInt(extractor(transformed)) : undefined,
      metadata: metaList(options?.metadata, this.input.node.name),
      payload: Buffer.from(channel.encode ? channel.encode(transformed) : encodeValue(transformed)),
    };
  }

  /**
   * The plugin frame, serialized once from the value as emitted: an edge's
   * transforms belong to that edge, so they never reach the sink.
   */
  private pluginOutput(
    channel: ChannelSpec,
    value: unknown,
    options: EmitOptions | undefined,
  ): NativeOutput {
    const key = options?.key ?? this.input.entityKey;
    if (channel.keyed && key === undefined) {
      throw new EmitError(
        `${this.input.node.name}: emits on keyed channel \`${channel.name}\` with no key, ` +
          'and has no entity key of its own to fall back on; pass { key } on the emit',
      );
    }
    const metadata = metaList(options?.metadata, this.input.node.name);
    this.stampIdentity(metadata, channel, key);
    return {
      target: 'plugin',
      channel: undefined,
      consumer: undefined,
      pluginChannel: channel.name,
      key,
      eventTime: undefined,
      metadata,
      payload: Buffer.from(channel.encode ? channel.encode(value) : encodeValue(value)),
    };
  }

  /**
   * Output identity for one plugin frame, appended after the user's metadata.
   * Written only off the tier: under it the controller derives the identity
   * from the durable effect record, and a second answer here would contradict
   * it. The ordinal advances on every plugin output, stamped or not, so hosts
   * of one topology agree on the sequence.
   */
  private stampIdentity(
    metadata: NativeMeta[],
    channel: ChannelSpec,
    key: string | undefined,
  ): void {
    if (channel.keyed && !this.input.spec.effectivelyOnce) {
      const entity = `${this.input.operative}/${this.input.node.name}/${this.input.entityKey ?? ''}`;
      metadata.push(
        { key: 'flz.entity', value: entity },
        { key: 'flz.seq', value: `${this.input.changeSeq}.${this.emitOrdinal}` },
        { key: 'flz.key', value: key ?? '' },
      );
    }
    this.emitOrdinal += 1;
  }

  timerAfter(ms: number | bigint): bigint {
    // Anchored on `now()`, never an ambient read: that is what makes a
    // replayed invocation register the timestamp the live one did.
    const at = this.input.now + BigInt(ms);
    this.timers.push({ kind: 'processing_time', scheduledAt: at, register: true });
    return at;
  }

  timerAtEventTime(at: number | bigint): bigint {
    const scheduledAt = BigInt(at);
    this.timers.push({ kind: 'event_time', scheduledAt, register: true });
    return scheduledAt;
  }

  cancelTimer(kind: TimerKind, at: bigint): void {
    this.timers.push({ kind, scheduledAt: at, register: false });
  }

  checkpoint(): void {
    this.checkpointed = true;
  }

  async spawnWorkflow(
    instanceId: string,
    factoryId: string,
    payload: unknown,
    options?: { ifRunning?: IfRunning },
  ): Promise<string> {
    if (this.input.replay) {
      // A spawn is an external side effect. A replayed invocation must not
      // issue it twice, and a timer must — a timer is internal state.
      return 'skipped_on_replay';
    }
    return this.input.bridge.spawnWorkflow(
      instanceId,
      factoryId,
      Buffer.from(encodeValue(payload)),
      options?.ifRunning ?? 'deliver',
    );
  }

  /** The effect record for this item, as the core folds it. */
  effect(consumedOffset: bigint | undefined): NativeItemEffect {
    return {
      verdict: 'ok',
      changes: this.changes,
      outputs: this.outputs,
      consumedOffset,
      eventTime: this.input.now,
      timers: this.timers,
      checkpoint: this.checkpointed,
    };
  }
}
