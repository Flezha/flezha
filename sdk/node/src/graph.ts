/**
 * The record the core is handed, and the verdict it answers.
 *
 * Every rule a declared topology can be refused under is one function in
 * `flz_core::host::register::check`, and `validate` is the call that submits
 * to it: this SDK records what the author declared, hands it over, and renders
 * the answer as its own exception. Nothing here restates a rule the core
 * holds — a refusal added there reaches Node with no edit to this file.
 *
 * What is still refused here is what the record cannot carry: a closure the
 * declaration never names, which the core has nothing to judge.
 *
 * Static typing catches a wrong edge type before the program runs (a
 * `.into(procedure)` whose producer's value the procedure's declared input
 * does not accept is a compile error, pinned by the fixtures under
 * `types-tests/`); that backstop is unchanged and earlier than this one.
 */

import type { ChannelKind, ChannelSpec, ClassSpec, ConsumerEdge, ProcNode } from './authoring.js';
import { loadNative, type Native } from './native.js';
import { DeclarationError } from './types.js';

/** The Node SDK's byte in the core's `sdk_lang` allocation table. */
export const SDK_LANG_NODE = 4;

/**
 * This SDK's own released version, stamped into every durable record it writes.
 * Pinned to `sdk/node/package.json` by `test/sdk_conformance/gate_test.exs`; a
 * source constant rather than a runtime read of the manifest, because the
 * package ships `dist/cjs`, `dist/esm` and `dist/types` from one `src`, so the
 * manifest sits at three different depths and any bundler that inlines the
 * module loses it entirely. The core packs it.
 */
export const SDK_VERSION = '0.1.0';

/** MessagePack, shared with every SDK whose codec is MessagePack. */
export const FORMAT_ID_MSGPACK = 5;

/** One consumer edge, as the core read it off the channel's chains. */
export interface DerivedEdge {
  procedure: number;
  /** Which of the channel's chains produced this edge; the host holds its closures. */
  chain: number;
}

/** One refusal, as the core answers it beside the rendered verdict. */
interface VerdictIssue {
  name: string;
}

/** One channel, as the core derived it from the record. */
export interface DerivedChannel {
  index: number;
  /** The last `named` override, else the name the builder wrote. */
  name: string;
  /** The declared kind after the wiring rule: a sink with no consumer is `plugin`. */
  kind: ChannelKind;
  producer: number | null;
  consumers: DerivedEdge[];
  sink: string | null;
}

/** The declaration the core derived: the answer a passing `validate` gives. */
export interface DerivedDecl {
  channels: DerivedChannel[];
  /** `checkpoint` when any checkpoint key was declared, else `per_input`. */
  ack_granularity: 'per_input' | 'checkpoint';
}

function refuse(spec: ClassSpec, handler: string | null, defect: string): never {
  throw new DeclarationError(spec.factoryId, handler, defect);
}

function clockOf(node: ProcNode): string {
  return node.procedure.definition.clock ?? 'wall';
}

function entityMode(node: ProcNode): 'single' | 'static' | 'dynamic' {
  const def = node.procedure.definition;
  if (def.entities) return 'static';
  if (def.dynamicEntities) return 'dynamic';
  return 'single';
}

/**
 * The two defects the record cannot carry, per procedure.
 *
 * An event-time extractor and a migration are host closures: the declaration
 * names neither, so the core has nothing to read and these are the only
 * procedure-level refusals left on this side.
 */
function checkProcedures(spec: ClassSpec): void {
  for (const node of spec.procedures) {
    checkClocks(spec, node);
    checkSchema(spec, node);
  }
}

function checkClocks(spec: ClassSpec, node: ProcNode): void {
  const def = node.procedure.definition;
  const clock = clockOf(node);

  if (clock === 'event_time' && !def.eventTime) {
    refuse(
      spec,
      node.name,
      'declares an event_time clock but no eventTime extractor; the core evaluates the ' +
        'declared clock per input and has nothing to evaluate',
    );
  }
  if (clock !== 'event_time' && def.eventTime) {
    refuse(
      spec,
      node.name,
      `declares an eventTime extractor with clock \`${clock}\`; the extractor is only ` +
        'consulted by an event_time clock, so one of the two is not what was meant',
    );
  }
}

function checkSchema(spec: ClassSpec, node: ProcNode): void {
  const def = node.procedure.definition;
  if (def.minReadableSchemaVersion === undefined) return;

  const floor = def.minReadableSchemaVersion;
  const running = def.schemaVersion ?? 1;
  if (floor > running) {
    refuse(
      spec,
      node.name,
      `declares minReadableSchemaVersion ${floor} above its schemaVersion ${running}`,
    );
  }
  if (floor < running && !def.migrateState && !def.migrateChange) {
    refuse(
      spec,
      node.name,
      `declares it reads back to schema version ${floor} but supplies no migration; a ` +
        'floor without one fails at the first old record instead of at startup',
    );
  }
}

/**
 * The core's verdict as this SDK's own exception. The names are read off the
 * verdict the core answered with — JSON on the thrown error's `cause` — and
 * kept apart on `refusals`, so a caller reads data rather than the prose.
 */
function refusalOf(spec: ClassSpec, error: Error): DeclarationError {
  const verdict = error.cause;
  const issues =
    verdict instanceof Error
      ? (JSON.parse(verdict.message) as { issues: VerdictIssue[] }).issues
      : [];
  return new DeclarationError(
    spec.factoryId,
    null,
    error.message,
    issues.map((issue) => issue.name),
  );
}

/**
 * Submit the record and answer what the core derived from it.
 *
 * On a refusal this throws the verdict — every name the core returned, in one
 * `DeclarationError`. On success the derived channel names are written back,
 * so the name this host addresses a channel by is the one the core registered.
 */
export function validate(spec: ClassSpec, native: Native = loadNative()): DerivedDecl {
  checkProcedures(spec);
  let derived: DerivedDecl;
  try {
    const answer = native.checkClass(Buffer.from(JSON.stringify(toHostClassDecl(spec))));
    derived = JSON.parse(answer.toString('utf8')) as DerivedDecl;
  } catch (error) {
    throw refusalOf(spec, error as Error);
  }
  for (const channel of derived.channels) {
    const declared = spec.channels[channel.index] as ChannelSpec;
    declared.name = channel.name;
    // The runtime edges ARE the core's answer: it says which chains produced a
    // consumer and in what order, and the host attaches the closures that
    // chain declared.
    declared.consumers = channel.consumers.map(
      (edge) => declared.chainEdges.get(edge.chain) as ConsumerEdge,
    );
  }
  return derived;
}

/** Drop the keys whose value is undefined, so the JSON omits them. */
function compact<T extends Record<string, unknown>>(value: T): T {
  for (const key of Object.keys(value)) {
    if (value[key] === undefined) delete value[key];
  }
  return value;
}

function channelJson(channel: ChannelSpec): Record<string, unknown> {
  return compact({
    name: channel.name,
    kind: channel.kind,
    producer: channel.producer,
    chains: channel.chains,
    retry_max: channel.retryMax,
    retry_backoff_ms: channel.retryBackoffMs,
    overflow: channel.overflow,
    keyed: channel.keyed,
  });
}

function procedureJson(spec: ClassSpec, node: ProcNode): Record<string, unknown> {
  const def = node.procedure.definition;
  return compact({
    name: node.name,
    entity_mode: entityMode(node),
    entity_keys: def.entities?.keys ?? [],
    idle_ttl_ms: def.dynamicEntities?.idleTtlMs,
    max_entities: def.dynamicEntities?.maxEntities,
    entities_for_overridden: def.entities?.entitiesFor !== undefined,
    compensate_overridden: def.compensate !== undefined,
    clock: def.clock ?? 'wall',
    mailbox_capacity: def.mailboxCapacity ?? 1024,
    // A procedure that folds no change has the empty Change type, so a logical
    // clock over it would never advance; a class that journals nothing has it
    // for every procedure.
    change_is_nochange: !spec.persistent || def.applyChange === undefined,
    schema_version: def.schemaVersion ?? 1,
    min_readable_schema_version: def.minReadableSchemaVersion,
  });
}

/**
 * The tier, checkpoint and correction keys, which only a stream class puts on
 * the wire. The core refuses `effectively_once` and `correction_window_ms` on
 * a workflow class outright, and the checkpoint family rests on a checkpoint
 * acknowledgement that refusal leaves unreachable there.
 *
 * The spec keeps the flag either way: `Ctx.stampIdentity` reads it to decide
 * whether a keyed plugin frame carries its own identity, so gating the
 * declaration must not gate the in-process tier.
 */
function streamTierJson(spec: ClassSpec): Record<string, unknown> {
  if (spec.kind !== 'stream') return {};
  return {
    effectively_once: spec.effectivelyOnce,
    checkpoint_ack: spec.checkpointAck,
    checkpoint_every: spec.checkpointEvery,
    checkpoint_outputs: spec.checkpointOutputs,
    checkpoint_max_delay_ms: spec.checkpointMaxDelayMs,
    correction_window_ms: spec.correctionWindowMs,
    beyond_window_policy: spec.beyondWindowPolicy,
  };
}

/**
 * The declaration the core deserializes. Every key here is pinned by
 * `test/fixtures/host_class_decl_keys.txt`, which this package's own test
 * reads: a key the core learns that Node cannot spell fails that test.
 */
export function toHostClassDecl(spec: ClassSpec): Record<string, unknown> {
  return compact({
    factory_id: spec.factoryId,
    persistent: spec.persistent,
    sdk_lang: SDK_LANG_NODE,
    sdk_version: SDK_VERSION,
    change_format_id: FORMAT_ID_MSGPACK,
    ...streamTierJson(spec),
    compact_after_bytes: spec.compactAfterBytes,
    sync_durability: spec.syncDurability,
    channels: spec.channels.map(channelJson),
    procedures: spec.procedures.map((node) => procedureJson(spec, node)),
    workflow: spec.workflow
      ? compact({
          loop_mode: spec.workflow.loopMode,
          rollback_mode: spec.workflow.rollbackMode,
          retry_attempts: spec.workflow.retryAttempts,
          retry_backoff_ms: spec.workflow.retryBackoffMs,
          barrier_retry_attempts: spec.workflow.barrierRetryAttempts,
          max_duration_ms: spec.workflow.maxDurationMs,
          wake_after_ms: spec.workflow.wakeAfterMs,
        })
      : undefined,
  });
}
