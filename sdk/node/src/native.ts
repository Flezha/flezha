/**
 * Loading the native addon, and the typed shape of what it exports.
 *
 * The addon is `flz_core_ffi` built with its `napi` feature — the same binary
 * the Java SDK loads under its `jni` feature, with a second veneer over the
 * same C entry points. Nothing here re-expresses dispatch, ordering, overflow,
 * the retry ladder, durability or the workflow driver; every function below is
 * one call into the core.
 */

import { requireNative, runtimeDir } from './runtime-shim.js';

/** One metadata pair, in declared order. The schema allows a repeated key. */
export interface NativeMeta {
  key: string;
  value: string;
}

export interface NativeWorkItem {
  kind: 'input' | 'timer';
  channel?: number;
  payload?: Buffer;
  metadata?: NativeMeta[];
  consumedOffset?: bigint;
  replay?: boolean;
  /** The core-evaluated declared clock for this item; what `ctx.now()` serves. */
  now?: bigint;
  timerKind?: 'event_time' | 'processing_time';
  scheduledAt?: bigint;
}

export interface NativeRestoreBody {
  bytes: Buffer;
  /** Absent when the body was written at the running schema version. */
  fromVersion?: number;
}

export interface NativeKeyRequest {
  channel: number;
  payloads: Buffer[];
  wantKeys: boolean;
  wantEventTime: boolean;
}

export type NativeActivationKind =
  | 'work'
  | 'restore_fold'
  | 'snapshot_request'
  | 'live_state_read'
  | 'apply_state'
  | 'migration_serialize'
  | 'key_request'
  | 'workflow_run'
  | 'compensate_run';

export interface NativeActivation {
  token: bigint;
  operative: string;
  procedure: number;
  entityKey?: string;
  changeSeq: bigint;
  factory: string;
  kind: NativeActivationKind;
  items?: NativeWorkItem[];
  snapshot?: NativeRestoreBody;
  changes?: NativeRestoreBody[];
  stateJson?: Buffer;
  keyRequest?: NativeKeyRequest;
  args?: Buffer;
  outputs?: Buffer[];
}

export interface NativeOutput {
  target: 'edge' | 'plugin';
  channel?: number;
  consumer?: number;
  pluginChannel?: string;
  key?: string;
  eventTime?: bigint;
  metadata?: NativeMeta[];
  payload: Buffer;
}

export interface NativeTimerOp {
  kind: 'event_time' | 'processing_time';
  scheduledAt: bigint;
  register: boolean;
}

export interface NativeDlqEntry {
  channel: string;
  error: string;
  attempts: number;
  payload?: Buffer;
}

export interface NativeItemEffect {
  verdict: 'ok' | 'retryable' | 'non_retryable';
  message?: string;
  changes?: Buffer[];
  outputs?: NativeOutput[];
  consumedOffset?: bigint;
  eventTime: bigint;
  timers?: NativeTimerOp[];
  checkpoint?: boolean;
  dlq?: NativeDlqEntry;
}

export interface NativePrepareAnswer {
  keys?: string[];
  eventTime?: bigint;
  /** Present when this payload's prepare failed; the retry ladder takes it. */
  error?: string;
  nonRetryable?: boolean;
}

export type NativeOutcome =
  | 'work'
  | 'restored'
  | 'snapshot'
  | 'state_json'
  | 'state_applied'
  | 'migration_state'
  | 'prepared'
  | 'failed'
  | 'workflow_done';

export interface NativeActivationResult {
  token: bigint;
  outcome: NativeOutcome;
  effects?: NativeItemEffect[];
  state?: Buffer;
  json?: Buffer;
  prepared?: NativePrepareAnswer[];
  error?: string;
  nonRetryable?: boolean;
  workflowCompleted?: boolean;
  workflowResult?: Buffer;
}

export interface NativeExport {
  name: string;
  /** The C ABI entry point behind this export; absent for host bookkeeping. */
  abi?: string;
  blocking: boolean;
}

/** One diagnostic from a native thread. */
export interface NativeLogLine {
  level: number;
  message: string;
}

export interface NativeHostStats {
  fetchers: number;
  fetcherCeiling: number;
  dispatchLoops: number;
  poolSize: number;
  poolBusy: number;
  poolPeak: number;
  poolQueued: number;
  handedOff: number;
  abandoned: number;
}

/** One answer from `idleWatchPoll`: the ABI's status and the episode's gap. */
export interface NativeIdleEpisode {
  /** `0` no episode, `1` idle, `2` resumed, `-1` an id no watch has. */
  status: number;
  gapNanos: bigint;
}

/**
 * The addon's exports. Every blocking entry point is a promise whose work runs
 * on a thread the addon owns — never on this event loop, and never on libuv's
 * shared pool. `poll` and `waitStarvation` are absent by design.
 */
export interface Native {
  schemaVersion(): number;
  contextNew(shimSchemaVersion: number): void;
  contextFree(): void;
  declareClass(json: Buffer): void;
  /** Judge a declaration without registering it; answers the derived one. */
  checkClass(json: Buffer): Buffer;
  start(namespace: string): Promise<void>;
  /** The alternative to `start`: the declared graph runs here, no controller. */
  localStart(factoryId: string, operativeId: string): void;
  localOnSink(channel: string): void;
  localFeed(channel: string, payload: Buffer): Promise<void>;
  localNextOutput(timeoutMs: number): Promise<NativeOutput | null>;
  localRunUntilIdle(timeoutMs: number): Promise<void>;
  localStop(): void;
  complete(result: NativeActivationResult): Promise<void>;
  abandon(token: bigint): void;
  shutdown(): void;
  workflowEmit(instanceId: string, payload: Buffer): void;
  workflowRecv(instanceId: string, timeoutMs: number): Promise<Buffer | null>;
  workflowCancelled(instanceId: string): boolean;
  spawnWorkflow(
    instanceId: string,
    factoryId: string,
    payload: Buffer,
    ifRunning: string,
  ): Promise<string>;
  monotonicNanos(): bigint;
  entryLastDeliveryNanos(channel: string): bigint;
  idleWatchNew(channel: string, thresholdNanos: bigint): bigint;
  idleWatchPoll(id: bigint): NativeIdleEpisode;
  idleWatchFree(id: bigint): void;
  lastError(): string | null;
  setLogCallback(callback: ((line: NativeLogLine) => void) | null): void;
  setHostDrainedCallback(callback: (() => void) | null): void;
  watchStdinEof(graceMs: number): void;
  registerDispatch(callback: (activation: NativeActivation) => void): number;
  unregisterDispatch(id: number): void;
  dispatchLoopCount(): number;
  startHost(fetchers?: number): void;
  /** Raise the fetchers' own flag, so they leave without the core's word. */
  stopFetching(): void;
  stopHost(): Promise<void>;
  hostStats(): NativeHostStats;
  nativeSurface(): NativeExport[];
}

/**
 * The per-platform package holding this host's binary. One is installed, not
 * five: the others are optional dependencies npm skips.
 */
export function nativePackageName(
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch,
): string | null {
  const triple: Record<string, string> = {
    'linux-x64': 'linux-x64-gnu',
    'linux-arm64': 'linux-arm64-gnu',
    'darwin-x64': 'darwin-x64',
    'darwin-arm64': 'darwin-arm64',
    'win32-x64': 'win32-x64-msvc',
  };
  return triple[`${platform}-${arch}`] ? `@flezha/sdk-${triple[`${platform}-${arch}`]}` : null;
}

/**
 * Where the addon is looked for, in order: an explicitly configured binary
 * first, so this repository's own suites run against a freshly built core;
 * then the installed per-platform package; then a locally built binary beside
 * the package, which is where the source-build fallback puts it.
 */
export function nativeCandidates(
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch,
  configured: string | undefined = process.env.FLEZHA_NATIVE_BINARY,
): string[] {
  const pkg = nativePackageName(platform, arch);
  return [
    ...(configured ? [configured] : []),
    ...(pkg ? [pkg] : []),
    `${runtimeDir}/../../native/flz_core_ffi.node`,
  ];
}

/** What to do when none of the candidates resolved. */
export function nativeResolutionError(
  attempts: string[],
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch,
): Error {
  const pkg = nativePackageName(platform, arch);
  return new Error(
    `@flezha/sdk could not load its native binary for ${platform}-${arch}.\n` +
      (pkg
        ? `Expected the optional dependency ${pkg} to be installed.\n`
        : 'No prebuilt binary is published for this platform; build from source.\n') +
      "A source build needs a Rust toolchain (stable) and this repository's " +
      'flz_core_ffi crate: cargo build -p flz_core_ffi --features napi --release.\n' +
      'Set FLEZHA_NATIVE_BINARY to an already-built addon to skip resolution.\n' +
      `Tried:\n  ${attempts.join('\n  ')}`,
  );
}

let loaded: Native | null = null;

/** Load the addon, or throw naming this platform and what a source build needs. */
export function loadNative(): Native {
  if (loaded) return loaded;
  const attempts: string[] = [];
  for (const candidate of nativeCandidates()) {
    try {
      loaded = requireNative(candidate) as Native;
      return loaded;
    } catch (cause) {
      attempts.push(`${candidate}: ${(cause as Error).message}`);
    }
  }
  throw nativeResolutionError(attempts);
}

/** Test seam: forget the loaded addon so the next call resolves again. */
export function resetNativeForTests(): void {
  loaded = null;
}
