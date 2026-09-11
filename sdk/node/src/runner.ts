/**
 * The runner: what an author starts, and everything it owns while running.
 *
 * Handler modules are registered by specifier rather than as imported values,
 * in every dispatch configuration. A worker thread is a separate isolate and a
 * live closure cannot cross into it, so taking an object here would make
 * enabling more dispatch loops a breaking change to the authoring surface
 * later. Taking a specifier makes it a flag.
 */

import { pathToFileURL } from 'node:url';

import type { ClassSpec } from './authoring.js';
import { Admin } from './admin.js';
import { envPositive } from './env.js';
import { toHostClassDecl, validate } from './graph.js';
import { installDeterminismWarnings, uninstallDeterminismWarnings } from './determinism.js';
import { EntryIdleWatcher, lastEntryDelivery, type IdleObserver } from './entry-idle.js';
import { Registry, dispatch, dispatchDeps, type WorkflowSession } from './dispatch.js';
import { consoleLogger, levelFromNative, log, setLogger, type Logger } from './logger.js';
import { loadNative, type Native } from './native.js';
import { LoopWatch } from './loop-watch.js';
import { WorkerPool } from './workers.js';
import { encodeValue, decodeValue } from './codec.js';

/**
 * The activation schema this package was built against. Passed at context
 * creation and compared with the native binary's, so a skewed pair fails
 * naming both versions before any controller connection is attempted.
 */
export const ACTIVATION_SCHEMA_VERSION = 3;

/** How long a workflow body's `recv` parks when the caller names no timeout. */
const DEFAULT_RECV_TIMEOUT_MS = 1_000;

/**
 * How long a drain has after end of file on standard input before the process
 * is terminated from a native thread. Comfortably past the poll timeout that
 * bounds how quickly a shutdown request is observed, so a healthy runner always
 * exits on its own first.
 */
const STDIN_EOF_GRACE_MS = 5_000;

/** Import a handler module, naming the specifier and chaining the cause. */
export async function importModule(specifier: string): Promise<unknown> {
  const resolved =
    specifier.startsWith('.') || specifier.startsWith('/')
      ? pathToFileURL(specifier.startsWith('/') ? specifier : `${process.cwd()}/${specifier}`).href
      : specifier;
  try {
    return await import(resolved);
  } catch (cause) {
    throw new Error(
      `could not import handler module \`${specifier}\`; a runner registers handlers by ` +
        'module specifier so every dispatch loop can import them itself',
      { cause },
    );
  }
}

/** The class declarations a handler module exports. */
export function specsFrom(specifier: string, module: unknown): ClassSpec[] {
  const record = module as Record<string, unknown>;
  const candidates = [record.default, ...Object.values(record)].filter(Boolean);
  const specs: ClassSpec[] = [];
  for (const candidate of candidates) {
    const spec = candidate as ClassSpec;
    if (spec && typeof spec === 'object' && typeof spec.factoryId === 'string' && spec.channels) {
      if (!specs.includes(spec)) specs.push(spec);
    }
  }
  if (specs.length === 0) {
    throw new Error(
      `handler module \`${specifier}\` exports no operative; export the value ` +
        '`defineOperative(...)` or `defineWorkflow(...)` returned',
    );
  }
  return specs;
}

/** Build the registry a dispatch loop serves from. Shared by both tiers. */
export async function loadRegistry(specifiers: string[]): Promise<Registry> {
  const registry = new Registry();
  for (const specifier of specifiers) {
    for (const spec of specsFrom(specifier, await importModule(specifier))) {
      validate(spec);
      registry.add(spec);
    }
  }
  return registry;
}

/** The workflow context a run-sugar body receives, over the veneer. */
export function workflowContext(native: Native, instanceId: string): WorkflowSession {
  let completed = false;
  let result: unknown;
  return {
    instanceId,
    emit(value: unknown) {
      native.workflowEmit(instanceId, Buffer.from(encodeValue(value)));
    },
    async recv(timeoutMs = DEFAULT_RECV_TIMEOUT_MS) {
      const payload = await native.workflowRecv(instanceId, timeoutMs);
      return payload === null ? null : decodeValue(payload);
    },
    isCancelled() {
      return native.workflowCancelled(instanceId);
    },
    complete(value?: unknown) {
      completed = true;
      result = value;
    },
    now() {
      return native.monotonicNanos();
    },
    get completed() {
      return completed;
    },
    get result() {
      return result;
    },
  };
}

export class Runner {
  private readonly specifiers: string[] = [];
  private workerCount = envPositive('FLEZHA_NODE_WORKERS', 0);
  private fetcherCount: number | undefined;
  private native: Native | null = null;
  private registry: Registry | null = null;
  private dispatchId: number | null = null;
  private pool: WorkerPool | null = null;
  private idle: EntryIdleWatcher | null = null;
  private stopping: Promise<void> | null = null;
  private readonly teardown: Array<() => void> = [];

  constructor(readonly namespace: string) {}

  /** Register a stream operative by module specifier. */
  defineStream(specifier: string): this {
    this.specifiers.push(specifier);
    return this;
  }

  /**
   * Register a workflow operative by module specifier.
   *
   * The same registration as a stream, and deliberately the same code: what a
   * module declares is read from what it exports, so the two names are for the
   * reader rather than for the runner.
   */
  defineWorkflow(specifier: string): this {
    return this.defineStream(specifier);
  }

  /**
   * How many dispatch loops to run. `0`, the default, dispatches on the main
   * event loop. Above zero the same activations are dispatched on that many
   * worker threads instead — the recommended production posture, because a
   * wedged worker can be replaced and a wedged main thread cannot.
   */
  workers(count: number): this {
    this.workerCount = Math.max(0, count);
    return this;
  }

  /** How many native fetch threads to poll on. Defaults to two. */
  fetchers(count: number): this {
    this.fetcherCount = count;
    return this;
  }

  logger(logger: Logger): this {
    setLogger(logger);
    return this;
  }

  /** Whether dispatch runs on worker threads, and how many. */
  get dispatchLoops(): number {
    return this.workerCount;
  }

  /**
   * Administer this namespace from ordinary application code: start, stop and
   * feed operatives, enable and disable plugins, read what is running.
   *
   * The controller judges every call as the identity this connection proved,
   * so no credential is held here and nothing expires. An operator grants a
   * namespace's applications the authority once, with
   * `flz role bind runner@<namespace> app <namespace>`; until then every call
   * is refused.
   */
  admin(): Admin {
    return new Admin(this.native ?? loadNative());
  }

  /**
   * When the runner last delivered a frame to entry channel `channel`, or
   * `null` before its first frame (and for a name no declared operative has —
   * both mean "nothing has arrived here"). Legal before `run()`; reading never
   * blocks delivery.
   *
   * The instant is the *signal*; whether the silence since then is anomalous
   * is the application's call. Register an observer with `onEntryIdle`
   * instead of polling this in a loop.
   */
  lastEntryDelivery(channel: string): Date | null {
    return lastEntryDelivery(this.native ?? loadNative(), channel);
  }

  /**
   * Observe silence on entry channel `channel`: `observer` is notified once
   * per idle episode, when the gap since the last delivered frame first
   * exceeds `thresholdMs`, and once again when a frame resumes it. Silence
   * before the channel's very first frame is never an episode.
   *
   * Legal before and after `run()`. Evaluation runs on one timer per runner —
   * never on the delivery path — so an observer that throws costs the data
   * plane nothing. Several observers may watch one channel.
   */
  onEntryIdle(
    channel: string,
    thresholdMs: number,
    observer: IdleObserver | ((gapMs: number) => void),
  ): this {
    if (!this.idle) {
      const watcher = new EntryIdleWatcher(this.native ?? loadNative());
      this.teardown.push(() => watcher.stop());
      this.idle = watcher;
    }
    this.idle.register(channel, thresholdMs, observer);
    return this;
  }

  /**
   * Declare one more class while the runner is up. The chainable
   * `defineStream` records a specifier for `run()`; this is the form that
   * takes effect against a running core, and it is the one to await.
   */
  async declare(specifier: string): Promise<void> {
    if (!this.native || !this.registry) {
      this.specifiers.push(specifier);
      return;
    }
    for (const spec of specsFrom(specifier, await importModule(specifier))) {
      validate(spec);
      this.registry.add(spec);
      this.native.declareClass(Buffer.from(JSON.stringify(toHostClassDecl(spec))));
    }
    await this.pool?.declare(specifier);
  }

  async run(): Promise<void> {
    const native = loadNative();
    this.native = native;

    // Before anything else: a binary built against a different activation
    // schema is refused here, with no controller connection attempted.
    native.contextNew(ACTIVATION_SCHEMA_VERSION);
    native.setLogCallback((line) => log()[levelFromNative(line.level)](line.message));

    installDeterminismWarnings();
    this.teardown.push(uninstallDeterminismWarnings);

    const registry = await this.declareAll(native);
    await this.serve(native, registry);

    await native.start(this.namespace);

    // A controller-requested stop reaches the host as every fetcher ending.
    native.setHostDrainedCallback(() => {
      log().info('the core stopped handing out work; draining');
      void this.stop();
    });

    native.startHost(this.fetcherCount);
    this.observeShutdown(native);

    log().info(
      `runner up on namespace ${this.namespace}: ` +
        `${this.workerCount === 0 ? 'the main loop' : `${this.workerCount} worker loops`}, ` +
        `${native.hostStats().fetchers} fetchers`,
    );
  }

  /**
   * The main thread builds the registry in both configurations: it is what
   * emits the declarations, and under `workers(0)` it also dispatches.
   */
  private async declareAll(native: Native): Promise<Registry> {
    const registry = await loadRegistry(this.specifiers);
    this.registry = registry;
    for (const registered of registry.all()) {
      native.declareClass(Buffer.from(JSON.stringify(toHostClassDecl(registered.spec))));
    }
    return registry;
  }

  /** Where activations are dispatched: a pool of worker loops, or this one. */
  private async serve(native: Native, registry: Registry): Promise<void> {
    if (this.workerCount > 0) {
      this.pool = new WorkerPool(this.namespace, this.specifiers, this.workerCount);
      await this.pool.start();
      return;
    }

    const deps = dispatchDeps(native, registry, workflowContext);
    const watch = new LoopWatch('main');
    this.dispatchId = native.registerDispatch((activation) => {
      // What the delay warning names when this loop is held. The worker tier
      // observes the same thing on its own loop.
      const node = registry.get(activation.factory)?.spec.procedures[activation.procedure];
      watch.observe(node?.name ?? activation.factory);
      void dispatch(activation, native, deps);
    });
    watch.start();
    this.teardown.push(() => watch.stop());
  }

  /**
   * Shutdown is observed three ways, because the harness, an operator and the
   * controller each have their own: end of file on standard input, a signal,
   * and the core's own stop.
   *
   * The first two are the core's, on a thread of its own rather than on this
   * loop. A compute-bound handler holds the loop that would otherwise notice
   * either, and in the single-loop configuration there is no second loop to
   * notice instead — so a wedged runner would be unstoppable, which is exactly
   * what a harness that closes pipes cannot fix by signalling.
   */
  private observeShutdown(native: Native): void {
    if (!process.stdin.isTTY) {
      native.watchStdinEof(STDIN_EOF_GRACE_MS);
    }
  }

  /** Stop fetching, drain what is in flight, and release the core. */
  stop(): Promise<void> {
    if (this.stopping) return this.stopping;
    this.stopping = this.doStop();
    return this.stopping;
  }

  private async doStop(): Promise<void> {
    const native = this.native;
    if (!native) return;
    log().info('draining');
    try {
      native.shutdown();
      await native.stopHost();
      if (this.dispatchId !== null) native.unregisterDispatch(this.dispatchId);
      await this.pool?.stop();
      native.setLogCallback(null);
      native.setHostDrainedCallback(null);
      native.contextFree();
    } finally {
      for (const undo of this.teardown.splice(0)) undo();
      this.native = null;
    }
  }
}

/** Start here: `createRunner('default').defineStream('./candle.js').run()`. */
export function createRunner(namespace: string): Runner {
  if (!log()) setLogger(consoleLogger());
  return new Runner(namespace);
}
