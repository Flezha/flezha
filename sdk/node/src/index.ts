/**
 * `@flezha/sdk` — the Flezha SDK for Node.
 *
 * The runner kernel is `flz_core`, reached through its C ABI: it owns
 * transport, ordering, per-entity exclusivity, durability, retries, timers,
 * credit, routing and the workflow driver. This package supplies threads and
 * user code, and nothing else.
 */

export { defineOperative, defineProcedure, defineWorkflow } from './authoring.js';
export type { ChannelSpec, ClassSpec, Link, ProcNode } from './authoring.js';
export {
  assertRepresentable,
  decodeValue,
  encodeValue,
  msgpack,
  UnrepresentableValueError,
} from './codec.js';
export type { Codec } from './codec.js';
export { FORMAT_ID_MSGPACK, SDK_LANG_NODE, toHostClassDecl, validate } from './graph.js';
export type { DerivedChannel, DerivedDecl, DerivedEdge } from './graph.js';
export { consoleLogger, setLogger } from './logger.js';
export type { Logger, LogLevel } from './logger.js';
export { loadNative, nativePackageName } from './native.js';
export type { Native } from './native.js';
export { ACTIVATION_SCHEMA_VERSION, createRunner, Runner } from './runner.js';
export { StateMutationError } from './state.js';
export { DELAY_WARN_MS } from './loop-watch.js';
export type { IdleObserver } from './entry-idle.js';
export { DeclarationError } from './types.js';
export type {
  BeyondWindowPolicy,
  ChannelBuilder,
  ClockKind,
  Ctx,
  DynamicEntities,
  EmitOptions,
  IfRunning,
  LoopMode,
  OperativeDefinition,
  OverflowPolicy,
  Procedure,
  ProcedureDefinition,
  RollbackMode,
  SpawnOutcome,
  StaticEntities,
  Stream,
  TimerFiring,
  TimerKind,
  WorkflowCtx,
  WorkflowDefinition,
} from './types.js';
