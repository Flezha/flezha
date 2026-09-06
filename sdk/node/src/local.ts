/**
 * Running a declared operative graph in this process, with no controller.
 *
 * `LocalOperative.start` takes the class spec you deploy and runs the
 * `channels` graph it already declares — no local-mode member on the
 * definition, no second declaration. What runs is the deployed machinery:
 * entry decode, edge transforms and keying, one Entity per resolved key with
 * its own mailbox, the retry ladder, and inter-procedure delivery performed
 * by the runtime.
 *
 * State is held in memory and nothing is durable. Durability, plugins,
 * placement and migration, the dead-letter queue, observability export,
 * authz and multi-runner scale all need a controller; the run says so on
 * stderr every time it starts. `docs/quickstart.md` walks through it.
 */
import { Registry, dispatch, dispatchDeps } from './dispatch.js';
import { toHostClassDecl, validate } from './graph.js';
import { loadNative, type Native, type NativeOutput } from './native.js';
import { ACTIVATION_SCHEMA_VERSION, workflowContext } from './runner.js';
import type { ClassSpec } from './authoring.js';

/** One value a graph released on a declared sink. */
export interface SinkOutput {
  channel: string;
  payload: Buffer;
  /** What a deployed run would have put on the wire, `flz.*` stamps included. */
  metadata: Record<string, string>;
}

/** Bounded so a stopped run's drain leaves promptly. */
const OUTPUT_POLL_MS = 50;

/** One operative graph running in this process. */
export class LocalOperative {
  readonly #native: Native;
  readonly #sinks = new Map<string, (output: SinkOutput) => void>();
  #dispatchId: number | null = null;
  #draining: Promise<void> | null = null;
  #closed = false;

  private constructor(native: Native) {
    this.#native = native;
  }

  /**
   * Register `spec`'s declared graph and start it locally. Refuses a
   * declaration that needs a controller, naming the capability.
   */
  static start(spec: ClassSpec): LocalOperative {
    const native = loadNative();
    native.contextNew(ACTIVATION_SCHEMA_VERSION);
    const run = new LocalOperative(native);
    try {
      // `validate` is what writes the derived channel names and consumer
      // edges back into the spec, so it runs before anything is declared.
      validate(spec, native);
      const registry = new Registry();
      registry.add(spec);
      native.declareClass(Buffer.from(JSON.stringify(toHostClassDecl(spec))));
      native.localStart(spec.factoryId, `${spec.factoryId}-local`);
      const deps = dispatchDeps(native, registry, workflowContext);
      run.#dispatchId = native.registerDispatch((activation) => {
        void dispatch(activation, native, deps);
      });
      // The fetchers are what run the procedures; local mode does not touch
      // them. Not `start`, which is the one call that dials a controller.
      native.startHost();
      run.#draining = run.#drain();
    } catch (failure) {
      native.contextFree();
      throw failure;
    }
    return run;
  }

  /**
   * Deliver every value released on the sink `channel` to `callback`, in
   * release order. A sink channel takes the producing procedure's name
   * unless the declaration `named` it.
   */
  onSink(channel: string, callback: (output: SinkOutput) => void): this {
    // Registered with the core first: a channel the declaration does not
    // carry is refused there, naming it, rather than silently accepted.
    this.#native.localOnSink(channel);
    this.#sinks.set(channel, callback);
    return this;
  }

  /** Feed one value's wire bytes to a declared entry channel. */
  async feed(channel: string, payload: Buffer | string): Promise<this> {
    await this.#native.localFeed(
      channel,
      typeof payload === 'string' ? Buffer.from(payload, 'utf8') : payload,
    );
    return this;
  }

  /**
   * Block until the graph has nothing left to do. A budget that elapses
   * first is reported, never passed off as done.
   */
  async runUntilIdle(budgetMs: number): Promise<void> {
    await this.#native.localRunUntilIdle(budgetMs);
  }

  /** End the run and release its Entities. Idempotent. */
  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#native.localStop();
    try {
      // The drain rethrows what a delivery died of, so the teardown below is
      // a `finally`: a failed delivery must not strand the context.
      await this.#draining;
    } finally {
      // Not `shutdown`: that stops the process-wide poll core, and a process
      // may start another local run after this one. The fetchers leave on
      // their own flag instead, which is why it is raised before the drain
      // rather than after it.
      this.#native.stopFetching();
      await this.#native.stopHost();
      if (this.#dispatchId !== null) {
        this.#native.unregisterDispatch(this.#dispatchId);
      }
      this.#native.contextFree();
    }
  }

  /** One released value per pass, handed to whoever registered its channel. */
  async #drain(): Promise<void> {
    while (!this.#closed) {
      let output: NativeOutput | null;
      try {
        output = await this.#native.localNextOutput(OUTPUT_POLL_MS);
      } catch {
        // The run ended under us; the close path owns the teardown.
        return;
      }
      if (output !== null) {
        this.#deliver(output);
      }
    }
  }

  #deliver(output: NativeOutput): void {
    if (output.target !== 'plugin' || output.pluginChannel === undefined) {
      // The core queues what a sink released and nothing else; an edge output
      // is an in-process mailbox push that never leaves.
      throw new Error(`local mode released a ${output.target} output`);
    }
    const channel = output.pluginChannel;
    const callback = this.#sinks.get(channel);
    if (callback === undefined) {
      return;
    }
    const metadata: Record<string, string> = {};
    for (const entry of output.metadata ?? []) {
      metadata[entry.key] = entry.value;
    }
    callback({ channel, payload: output.payload, metadata });
  }
}
