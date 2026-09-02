/**
 * Turning one activation into one answer, exactly once.
 *
 * Per-entity exclusivity IS the handout: the core holds an entity's next
 * activation until this one is answered, so an activation that returns
 * unanswered retires that entity for the life of the process. Every path out
 * of `dispatch` therefore ends in a completion or an abandon, through a
 * `finally` rather than a rule applied at each return site — a thrown handler,
 * a rejected promise and an encoding failure all reach the same guard.
 */

import { applyTransforms } from './authoring.js';
import type { ChannelSpec, ClassSpec, ProcNode } from './authoring.js';
import { decodeValue, encodeValue, fromStateJson, toStateJson } from './codec.js';
import { Invocation, type SpawnBridge } from './ctx.js';
import { invocation } from './determinism.js';
import { log } from './logger.js';
import type {
  Native,
  NativeActivation,
  NativeActivationResult,
  NativeItemEffect,
  NativePrepareAnswer,
} from './native.js';
import { foldRestore, rawState } from './state.js';
import { DROPPED, type WorkflowCtx } from './types.js';

/** A declared class, with the lookups dispatch needs on the hot path. */
export class RegisteredClass {
  /** Channels each procedure produces on, by procedure index. */
  readonly outgoing: ChannelSpec[][];
  /** Live entity state, keyed by procedure index and entity key. */
  private readonly entities = new Map<string, unknown>();

  constructor(readonly spec: ClassSpec) {
    this.outgoing = spec.procedures.map((node) =>
      spec.channels.filter((channel) => channel.producer === node.index),
    );
  }

  private cellKey(procedure: number, entityKey: string | undefined): string {
    return `${procedure}/${entityKey ?? ''}`;
  }

  /** The entity's live state, created from the declaration on first touch. */
  stateOf(node: ProcNode, entityKey: string | undefined): unknown {
    const key = this.cellKey(node.index, entityKey);
    if (!this.entities.has(key)) {
      const initial = node.procedure.definition.initialState;
      this.entities.set(key, initial ? initial(entityKey) : undefined);
    }
    return this.entities.get(key);
  }

  setState(node: ProcNode, entityKey: string | undefined, state: unknown): void {
    this.entities.set(this.cellKey(node.index, entityKey), state);
  }
}

export class Registry {
  private readonly classes = new Map<string, RegisteredClass>();

  add(spec: ClassSpec): RegisteredClass {
    if (this.classes.has(spec.factoryId)) {
      throw new Error(
        `duplicate factory id \`${spec.factoryId}\`: a runner declares each factory once`,
      );
    }
    const registered = new RegisteredClass(spec);
    this.classes.set(spec.factoryId, registered);
    return registered;
  }

  get(factoryId: string): RegisteredClass | undefined {
    return this.classes.get(factoryId);
  }

  all(): RegisteredClass[] {
    return [...this.classes.values()];
  }
}

function failed(token: bigint, message: string, nonRetryable = false): NativeActivationResult {
  return { token, outcome: 'failed', error: message, nonRetryable };
}

/**
 * What a failure says on the wire. `name: message`, in family with what the
 * other SDKs send — this string is stored in a dead-letter entry and read by
 * an operator, and a multi-line stack there is noise the retry ladder
 * multiplies. The stack is logged host-side instead, where it is a diagnostic
 * rather than a record.
 */
function errorMessage(error: unknown): string {
  if (error instanceof Error) return `${error.name}: ${error.message}`;
  return String(error);
}

/** A handler declares an error non-retryable by setting the flag on it. */
function isNonRetryable(error: unknown): boolean {
  return Boolean(
    error && typeof error === 'object' && (error as { nonRetryable?: boolean }).nonRetryable,
  );
}

/** A live run-sugar workflow body's context, plus what it recorded. */
export type WorkflowSession = WorkflowCtx & { completed?: boolean; result?: unknown };

export interface DispatchDeps {
  registry: Registry;
  bridge: SpawnBridge;
  /** The workflow context for a run-sugar body. */
  workflowCtx(instanceId: string): WorkflowSession;
}

/**
 * What a dispatch loop needs to answer an activation, over one addon.
 *
 * Both loops build it the same way — that is what "one dispatch path" means at
 * this level — so a third kind of loop gets it by calling this rather than by
 * copying eight lines.
 */
export function dispatchDeps(
  native: Native,
  registry: Registry,
  session: (native: Native, instanceId: string) => WorkflowSession,
): DispatchDeps {
  return {
    registry,
    bridge: {
      spawnWorkflow: (instanceId, factoryId, payload, ifRunning) =>
        native.spawnWorkflow(instanceId, factoryId, payload, ifRunning),
    },
    workflowCtx: (instanceId) => session(native, instanceId),
  };
}

/**
 * Produce the answer for one activation. Throwing is not an option here: every
 * failure becomes a result the core can act on, because the alternative is an
 * entity that never receives work again.
 */
export async function answer(
  activation: NativeActivation,
  deps: DispatchDeps,
): Promise<NativeActivationResult> {
  const registered = deps.registry.get(activation.factory);
  if (!registered) {
    return failed(
      activation.token,
      `no class registered for factory \`${activation.factory}\``,
      true,
    );
  }

  try {
    // The workflow body is the one kind addressed to a class rather than to a
    // procedure, so it is answered before the procedure is resolved — which is
    // what lets every arm below hold a procedure rather than assert one.
    if (activation.kind === 'workflow_run') return await runWorkflow(activation, registered, deps);

    const node = registered.spec.procedures[activation.procedure];
    if (!node) {
      return failed(
        activation.token,
        `class \`${activation.factory}\` has no procedure at index ${activation.procedure}`,
        true,
      );
    }

    switch (activation.kind) {
      case 'work':
        return await runWork(activation, registered, node, deps);
      case 'restore_fold':
        return restoreFold(activation, registered, node);
      case 'snapshot_request':
        return snapshot(activation, registered, node);
      case 'live_state_read':
        return {
          token: activation.token,
          outcome: 'state_json',
          json: toStateJson(registered.stateOf(node, activation.entityKey)),
        };
      case 'migration_serialize':
        return {
          token: activation.token,
          outcome: 'migration_state',
          json: toStateJson(registered.stateOf(node, activation.entityKey)),
        };
      case 'apply_state':
        registered.setState(
          node,
          activation.entityKey,
          fromStateJson(activation.stateJson ?? Buffer.alloc(0)),
        );
        return { token: activation.token, outcome: 'state_applied' };
      case 'key_request':
        return prepare(activation, registered);
      case 'compensate_run':
        return await compensate(activation, registered, node, deps);
      default:
        return failed(activation.token, `unknown activation kind \`${activation.kind}\``, true);
    }
  } catch (error) {
    return failed(activation.token, errorMessage(error), isNonRetryable(error));
  }
}

/**
 * Run user code against one entity, and keep the state it produced.
 *
 * Every activation kind that reaches a handler goes through here — an input,
 * a timer firing, a compensation — so the context they receive and the state
 * they leave behind cannot differ between them. The state is written back
 * whether or not the body threw: a change recorded before the throw is
 * already journaled, and dropping it here would make the live entity
 * disagree with its own log.
 */
async function invoke(
  activation: NativeActivation,
  registered: RegisteredClass,
  node: ProcNode,
  deps: DispatchDeps,
  at: { now: bigint; replay: boolean },
  body: (ctx: Invocation) => Promise<unknown>,
): Promise<{ ctx: Invocation; returned: unknown }> {
  const ctx = new Invocation({
    spec: registered.spec,
    node,
    outgoing: registered.outgoing[node.index] as ChannelSpec[],
    operative: activation.operative,
    entityKey: activation.entityKey,
    changeSeq: activation.changeSeq,
    now: at.now,
    replay: at.replay,
    state: registered.stateOf(node, activation.entityKey),
    bridge: deps.bridge,
  });
  try {
    const returned = await invocation.run({ procedure: node.name }, () => body(ctx));
    return { ctx, returned };
  } finally {
    registered.setState(node, activation.entityKey, rawState(ctx.currentState));
  }
}

async function runWork(
  activation: NativeActivation,
  registered: RegisteredClass,
  node: ProcNode,
  deps: DispatchDeps,
): Promise<NativeActivationResult> {
  const definition = node.procedure.definition;
  const effects: NativeItemEffect[] = [];

  for (const item of activation.items ?? []) {
    const now = item.now ?? 0n;
    try {
      const { ctx, returned } = await invoke(
        activation,
        registered,
        node,
        deps,
        { now, replay: item.replay ?? false },
        async (ctx) => {
          if (item.kind === 'timer') {
            if (!definition.onTimer) {
              throw new Error(
                `${node.name} registered a timer but declares no onTimer; registration is a ` +
                  'runtime call, so this cannot be refused at declaration time',
              );
            }
            await definition.onTimer(
              { kind: item.timerKind ?? 'processing_time', scheduledAt: item.scheduledAt ?? now },
              ctx as never,
            );
            return undefined;
          }
          const channel = registered.spec.channels[item.channel ?? 0] as ChannelSpec | undefined;
          const decoded = channel?.decode
            ? channel.decode(item.payload ?? Buffer.alloc(0))
            : decodeValue(item.payload ?? Buffer.alloc(0));
          // This node's own edge off the channel: a transform chain declared
          // before one `into` belongs to that consumer and to no other.
          const edge = channel?.consumers.find((each) => each.procedure === node.index);
          const input = applyTransforms(edge?.transforms ?? [], decoded);
          // A filtered value is consumed and reaches no handler. It still
          // produces an effect, so its offset advances and nothing is retried.
          if (input === DROPPED) return undefined;
          return await definition.handle(input as never, ctx as never);
        },
      );

      // A returned value is emitted; `undefined` emits nothing, which is the
      // warmup gate. A handler that emits through the context returns nothing,
      // so the two spellings compose rather than conflict.
      if (returned !== undefined) ctx.emit(returned);
      effects.push(ctx.effect(item.consumedOffset));
    } catch (error) {
      // The stack is a host diagnostic, not a record: the verdict below
      // travels to the dead-letter entry and the controller's log.
      log().debug(`${node.name} failed an item: ${(error as Error)?.stack ?? error}`);
      // Items after this one are re-handed by the core, so the batch stops
      // here rather than reporting effects the entity never saw.
      effects.push({
        verdict: isNonRetryable(error) ? 'non_retryable' : 'retryable',
        message: errorMessage(error),
        eventTime: now,
        consumedOffset: item.consumedOffset,
      });
      break;
    }
  }

  return { token: activation.token, outcome: 'work', effects };
}

function restoreFold(
  activation: NativeActivation,
  registered: RegisteredClass,
  node: ProcNode,
): NativeActivationResult {
  registered.setState(
    node,
    activation.entityKey,
    foldRestore(
      node.procedure.definition as never,
      { snapshot: activation.snapshot, changes: activation.changes },
      activation.entityKey,
      node.name,
    ),
  );
  return { token: activation.token, outcome: 'restored' };
}

function snapshot(
  activation: NativeActivation,
  registered: RegisteredClass,
  node: ProcNode,
): NativeActivationResult {
  const codec = node.procedure.definition.stateCodec;
  const state = registered.stateOf(node, activation.entityKey);
  return {
    token: activation.token,
    outcome: 'snapshot',
    state: Buffer.from(codec ? codec.encode(state as never) : encodeValue(state)),
  };
}

function prepare(
  activation: NativeActivation,
  registered: RegisteredClass,
): NativeActivationResult {
  const request = activation.keyRequest;
  if (!request) {
    return failed(activation.token, 'a key request carried no payloads', true);
  }
  const channel = registered.spec.channels[request.channel] as ChannelSpec | undefined;
  // The crossing answers one key set for the whole payload, so it reads the
  // first consumer edge — the one the core routes the answer to.
  const edge = channel?.consumers[0];
  const node = edge === undefined ? undefined : registered.spec.procedures[edge.procedure];
  const definition = node?.procedure.definition;

  const answers: NativePrepareAnswer[] = request.payloads.map((payload) => {
    try {
      const decoded = channel?.decode ? channel.decode(payload) : decodeValue(payload);
      const value = applyTransforms(edge?.transforms ?? [], decoded);
      // A filtered payload answers no key. For a dynamic consumer that is what
      // tells the core not to route it at all — the drop happens before an
      // entity is minted rather than inside one.
      if (value === DROPPED) return { keys: [] };

      const keys = request.wantKeys
        ? edge?.keyBy
          ? [edge.keyBy(value)]
          : definition?.entities?.entitiesFor
            ? definition.entities.entitiesFor(value as never)
            : []
        : [];
      const eventTime =
        request.wantEventTime && definition?.eventTime
          ? BigInt(definition.eventTime(value as never))
          : undefined;
      return { keys, eventTime };
    } catch (error) {
      // A per-payload refusal enters the input channel's retry ladder; it is
      // not a failure of the crossing, whose other payloads still have answers.
      return { error: errorMessage(error), nonRetryable: isNonRetryable(error) };
    }
  });

  return { token: activation.token, outcome: 'prepared', prepared: answers };
}

async function runWorkflow(
  activation: NativeActivation,
  registered: RegisteredClass,
  deps: DispatchDeps,
): Promise<NativeActivationResult> {
  const run = registered.spec.run;
  if (!run) {
    return failed(
      activation.token,
      `class \`${activation.factory}\` was handed a workflow body but declares none`,
      true,
    );
  }
  const ctx = deps.workflowCtx(activation.operative);
  const args = activation.args ? decodeValue(activation.args) : undefined;
  const returned = await invocation.run({ procedure: activation.factory }, () => run(args, ctx));
  const completed = ctx.completed ?? returned !== undefined;
  return {
    token: activation.token,
    outcome: 'workflow_done',
    workflowCompleted: completed,
    workflowResult: Buffer.from(encodeValue(ctx.completed ? ctx.result : returned)),
  };
}

async function compensate(
  activation: NativeActivation,
  registered: RegisteredClass,
  node: ProcNode,
  deps: DispatchDeps,
): Promise<NativeActivationResult> {
  const definition = node.procedure.definition;
  if (!definition.compensate) {
    return failed(activation.token, `${node.name} declares no compensate hook`, true);
  }
  const outputs = (activation.outputs ?? []).map((bytes) => decodeValue(bytes));
  const { ctx } = await invoke(
    activation,
    registered,
    node,
    deps,
    { now: 0n, replay: false },
    async (ctx) => (definition.compensate as (o: unknown[], c: unknown) => unknown)(outputs, ctx),
  );
  return { token: activation.token, outcome: 'work', effects: [ctx.effect(undefined)] };
}

/**
 * Dispatch one activation and answer it exactly once.
 *
 * The guard is the contract, not politeness. An unanswered activation retires
 * its entity for the life of the process, so a throw from the dispatcher, from
 * encoding the result, or from `complete` itself abandons rather than logging
 * and returning.
 */
export async function dispatch(
  activation: NativeActivation,
  native: Native,
  deps: DispatchDeps,
): Promise<void> {
  let answered = false;
  try {
    const result = await answer(activation, deps);
    await native.complete(result);
    answered = true;
  } catch (error) {
    log().error(`activation processing failed: ${errorMessage(error)}`);
  } finally {
    if (!answered) {
      try {
        native.abandon(activation.token);
      } catch (error) {
        // The core refuses an unknown token: `complete` had already landed and
        // only the bookkeeping after it threw. Nothing is wedged.
        log().warn(`abandoning activation ${activation.token} failed: ${errorMessage(error)}`);
      }
    }
  }
}
