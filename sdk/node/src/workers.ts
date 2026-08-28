/**
 * The worker-thread dispatch tier.
 *
 * The fetcher hands an activation to *a* JavaScript loop, not *the* JavaScript
 * loop, and choosing which is the only difference between the configurations.
 * So this file is what a second tier costs: spawn, register, replace, stop —
 * and nothing in the dispatch path.
 *
 * Worker mode is the recommended production posture for two things the main
 * loop cannot do. A synchronous compute-bound handler blocks its loop and
 * there is no other fix. And a wedged worker can be killed and restarted,
 * where a wedged main thread cannot: the runner would keep heartbeating with
 * `inflight_oldest_ms` climbing until the controller evicts it.
 */

import { Worker } from 'node:worker_threads';

import { log } from './logger.js';
import { runtimeDir } from './runtime-shim.js';

/** What the parent sends a worker. */
export type ToWorker = { type: 'declare'; specifier: string } | { type: 'stop' };

/** What a worker sends back. */
export type FromWorker =
  { type: 'ready' } | { type: 'declared' } | { type: 'failed'; message: string };

interface Slot {
  worker: Worker;
  index: number;
}

export class WorkerPool {
  private readonly slots = new Map<number, Slot>();
  private stopping = false;
  private nextIndex = 0;

  constructor(
    private readonly namespace: string,
    private readonly specifiers: string[],
    private readonly count: number,
  ) {}

  async start(): Promise<void> {
    await Promise.all(Array.from({ length: this.count }, () => this.spawn()));
  }

  get size(): number {
    return this.slots.size;
  }

  private async spawn(): Promise<void> {
    const index = this.nextIndex++;
    const worker = new Worker(`${runtimeDir}/worker-entry.js`, {
      workerData: { namespace: this.namespace, specifiers: this.specifiers, index },
      stdout: false,
      stderr: false,
    });
    const slot: Slot = { worker, index };
    this.slots.set(index, slot);

    const ready = new Promise<void>((resolve, reject) => {
      const onMessage = (message: FromWorker) => {
        if (message.type === 'ready') {
          worker.off('message', onMessage);
          resolve();
        } else if (message.type === 'failed') {
          worker.off('message', onMessage);
          reject(new Error(`dispatch worker ${index} failed to start: ${message.message}`));
        }
      };
      worker.on('message', onMessage);
      worker.once('error', reject);
    });

    // A worker that ends other than at shutdown is replaced, and the
    // configured loop count restored. What it still held was already abandoned
    // by the native side, which is the only place that still knew.
    worker.on('exit', (code) => {
      this.slots.delete(index);
      if (this.stopping) return;
      log().error(
        `dispatch worker ${index} exited with code ${code}; starting a replacement so the ` +
          'configured loop count is restored',
      );
      void this.spawn().catch((error) => {
        log().error(`could not start a replacement dispatch worker: ${String(error)}`);
      });
    });

    await ready;
  }

  /** Declare one more class on every worker while the runner is up. */
  async declare(specifier: string): Promise<void> {
    this.specifiers.push(specifier);
    await Promise.all(
      [...this.slots.values()].map(
        (slot) =>
          new Promise<void>((resolve, reject) => {
            const onMessage = (message: FromWorker) => {
              if (message.type === 'declared') {
                slot.worker.off('message', onMessage);
                resolve();
              } else if (message.type === 'failed') {
                slot.worker.off('message', onMessage);
                reject(new Error(message.message));
              }
            };
            slot.worker.on('message', onMessage);
            slot.worker.postMessage({ type: 'declare', specifier } satisfies ToWorker);
          }),
      ),
    );
  }

  async stop(): Promise<void> {
    this.stopping = true;
    const slots = [...this.slots.values()];
    for (const slot of slots) slot.worker.postMessage({ type: 'stop' } satisfies ToWorker);
    await Promise.all(
      slots.map(
        (slot) =>
          new Promise<void>((resolve) => {
            slot.worker.once('exit', () => resolve());
            // A worker still inside a handler is not waited on forever: the
            // core is being freed next, and an activation it never answers was
            // already abandoned when its registration was released.
            setTimeout(() => void slot.worker.terminate().then(() => resolve()), 5_000).unref();
          }),
      ),
    );
    this.slots.clear();
  }
}
