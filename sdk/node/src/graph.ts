/**
 * Registration-time refusals, and the `HostClassDecl` the core is handed.
 *
 * Static typing catches a wrong edge type before the program runs (a
 * `.into(procedure)` whose producer's value the procedure's declared input
 * does not accept is a compile error, pinned by the fixtures under
 * `types-tests/`). Everything typing cannot see is refused here, naming the
 * handler and the specific defect — the same set, for the same reasons, as the
 * Rust builder's `TopologyIssue`, so an author moving between SDKs meets one
 * vocabulary.
 *
 * The core validates again on its side. That is not redundancy: a
 * hand-written declaration reaching the ABI directly must not slip past, and
 * an author who declared through this surface deserves the error before any
 * data moves.
 */

import type { ChannelSpec, ClassSpec, ProcNode } from './authoring.js';
import { DeclarationError } from './types.js';

const MAX_MAILBOX_CAPACITY = 1_048_576;

/** The Node SDK's byte in the core's `sdk_lang` allocation table. */
export const SDK_LANG_NODE = 4;

/** MessagePack, shared with every SDK whose codec is MessagePack. */
export const FORMAT_ID_MSGPACK = 5;

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

function checkChannels(spec: ClassSpec): void {
  const names = new Set<string>();
  const sinks = new Set<string>();

  for (const channel of spec.channels) {
    if (names.has(channel.name)) {
      refuse(spec, channel.name, 'this channel name is declared more than once');
    }
    names.add(channel.name);

    if (channel.kind === 'plugin') {
      if (sinks.has(channel.name)) {
        refuse(spec, channel.name, 'declares a plugin sink more than once');
      }
      sinks.add(channel.name);
    } else if (channel.keyed) {
      refuse(
        spec,
        channel.name,
        'is declared keyed() but has no plugin wire; output identity stamps only exist ' +
          'on plugin-bound frames, so add a sink(...) or drop the keyed()',
      );
    }

    if (spec.effectivelyOnce && channel.kind === 'plugin' && !channel.keyed) {
      refuse(
        spec,
        channel.name,
        'has a plugin wire but is not declared keyed(); the effectively-once tier ' +
          'deduplicates by output identity, so every plugin-bound channel of a class ' +
          'that declares effectivelyOnce() must opt in with keyed()',
      );
    }

    if (channel.overflow !== 'drop_nothing') {
      if (spec.effectivelyOnce) {
        refuse(
          spec,
          channel.name,
          `declares onFull(${channel.overflow}) on a class that declares effectivelyOnce(); ` +
            "a shed input's offset is still advanced past by the tier's watermark, so the " +
            'loss is permanent and reported nowhere — declare one or the other, not both',
        );
      }
      if (spec.correctionWindowMs !== undefined) {
        refuse(
          spec,
          channel.name,
          `declares onFull(${channel.overflow}) on a class that declares a correction ` +
            'window; a correction re-folds a replayed slice and its answer may not depend ' +
            'on which items a full mailbox happened to keep',
        );
      }
    }

    if (channel.retryMax === 0) {
      refuse(
        spec,
        channel.name,
        'declares retryMax(0); at least one attempt is required — throw a non-retryable ' +
          'error to park without retries',
      );
    }

    if (channel.kind === 'entry' && channel.consumers.length === 0) {
      refuse(spec, channel.name, 'is an entry channel that names no consumer');
    }
    if (channel.kind === 'inter_proc' && channel.consumers.length === 0) {
      refuse(spec, channel.name, 'is an inter-procedure channel that names no consumer');
    }
  }
}

function checkKeying(spec: ClassSpec): void {
  const dynamic = new Set(
    spec.procedures.filter((node) => entityMode(node) === 'dynamic').map((node) => node.index),
  );

  for (const channel of spec.channels) {
    for (const consumer of channel.consumers) {
      const node = spec.procedures[consumer] as ProcNode;
      if (channel.keyBy && entityMode(node) === 'static') {
        refuse(
          spec,
          channel.name,
          `declares keyBy(...) into procedure \`${node.name}\`, which declares a static key ` +
            'set; static keying routes by entitiesFor and may name several entities, edge ' +
            'keying names exactly one — declare one or the other, not both',
        );
      }
      if (!channel.keyBy && dynamic.has(consumer)) {
        refuse(
          spec,
          channel.name,
          `feeds dynamic procedure \`${node.name}\` without a keyBy(...); every edge into a ` +
            'dynamic procedure must name the entity its inputs reach',
        );
      }
    }
  }
}

function checkProcedures(spec: ClassSpec): void {
  for (const node of spec.procedures) {
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
    if (clock === 'logical' && !def.initialState) {
      refuse(
        spec,
        node.name,
        'declares a logical clock but no state; logical time IS the durable change ' +
          'sequence, which never advances for a procedure that records no changes',
      );
    }
    if (def.entities && !def.entities.entitiesFor) {
      refuse(
        spec,
        node.name,
        'declares a static key set without an entitiesFor; static keying needs the ' +
          'function that says which declared entities an input belongs to',
      );
    }
    if (def.mailboxCapacity !== undefined) {
      const capacity = def.mailboxCapacity;
      if (capacity < 1 || capacity > MAX_MAILBOX_CAPACITY) {
        refuse(
          spec,
          node.name,
          `declares mailboxCapacity ${capacity}; capacity must be in 1..=${MAX_MAILBOX_CAPACITY}`,
        );
      }
    }
    if (def.dynamicEntities?.maxEntities === 0) {
      refuse(
        spec,
        node.name,
        'declares maxEntities 0; a dynamic procedure that may hold no ' +
          'entity can process nothing',
      );
    }
    if (def.minReadableSchemaVersion !== undefined) {
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
  }
}

function checkTierAndWindow(spec: ClassSpec): void {
  if (spec.effectivelyOnce && !spec.persistent) {
    refuse(spec, null, 'declares effectivelyOnce() without persistence: the tier IS durable state');
  }
  if (spec.ackGranularity === 'checkpoint' && !spec.effectivelyOnce) {
    refuse(
      spec,
      null,
      'declares checkpoint acknowledgement without effectivelyOnce(); the checkpoint is ' +
        "where the tier's consumed offset advances, and a class without the tier has no " +
        'durable offset to hold back',
    );
  }
  if (spec.checkpointEvery === 0) {
    refuse(
      spec,
      null,
      'declares checkpointEvery(0); an interval that closes every zero inputs is not an ' +
        'interval — omit it to close only on an explicit ctx.checkpoint()',
    );
  }
  if (spec.checkpointOutputs === 'at_checkpoint' && spec.checkpointMaxDelayMs === undefined) {
    refuse(
      spec,
      null,
      'declares outputsAtCheckpoint() with no maximum delay; an interval that never closes ' +
        'would hold its outputs forever on a quiet entity',
    );
  }
  if (spec.correctionWindowMs === undefined) return;

  if (spec.correctionWindowMs === 0) {
    refuse(spec, null, 'declares a correction window of zero; a zero window can correct nothing');
  }
  if (!spec.effectivelyOnce) {
    refuse(
      spec,
      null,
      'declares a correction window without effectivelyOnce(); correction replays the input ' +
        'log against a rewound baseline, and only the tier journals inputs before ' +
        'forwarding them',
    );
  }
  const eventTimed = spec.procedures.find((node) => clockOf(node) === 'event_time');
  if (!eventTimed) {
    refuse(
      spec,
      null,
      'declares a correction window but no procedure declares an event_time clock; ' +
        'correction orders a replayed slice by event time',
    );
  }
}

/** Every procedure reachable from an entry channel, and no cycle among them. */
function checkTopology(spec: ClassSpec): void {
  const entries = spec.channels.filter((c) => c.kind === 'entry');
  if (spec.kind === 'stream' && entries.length === 0) {
    refuse(spec, null, 'declares no entry channel; a stream class is fed by the controller');
  }
  if (spec.procedures.length === 0) {
    if (spec.kind === 'stream') {
      refuse(spec, null, 'declares no procedure');
    }
    return;
  }

  const successors = new Map<number, number[]>();
  for (const channel of spec.channels) {
    if (channel.kind !== 'inter_proc' || channel.producer === undefined) continue;
    const list = successors.get(channel.producer) ?? [];
    list.push(...channel.consumers);
    successors.set(channel.producer, list);
  }

  const reachable = new Set<number>();
  const frontier = entries.flatMap((c) => c.consumers);
  while (frontier.length > 0) {
    const next = frontier.pop() as number;
    if (reachable.has(next)) continue;
    reachable.add(next);
    frontier.push(...(successors.get(next) ?? []));
  }

  // A cycle is found before unreachability is reported: a node on a cycle with
  // no path from an entry is both, and the cycle is the more useful answer.
  const WHITE = 0;
  const GREY = 1;
  const BLACK = 2;
  const colour = new Map<number, number>();
  const visit = (node: number): void => {
    const state = colour.get(node) ?? WHITE;
    if (state === BLACK) return;
    if (state === GREY) {
      refuse(
        spec,
        (spec.procedures[node] as ProcNode).name,
        "is on a cycle; an operative topology is a DAG — a node's output may never reach " +
          'its own input',
      );
    }
    colour.set(node, GREY);
    for (const next of successors.get(node) ?? []) visit(next);
    colour.set(node, BLACK);
  };
  for (const node of spec.procedures) visit(node.index);

  for (const node of spec.procedures) {
    if (!reachable.has(node.index)) {
      refuse(spec, node.name, 'is not reachable from any entry channel');
    }
  }
}

function checkWorkflow(spec: ClassSpec): void {
  if (spec.kind !== 'workflow') {
    if (spec.syncDurability) {
      refuse(
        spec,
        null,
        'declares syncDurability() on a stream class; a stream class acknowledges at input ' +
          'rates, and a remote round trip per acknowledgement is not a guarantee it can keep',
      );
    }
    return;
  }
  if (spec.effectivelyOnce) {
    refuse(spec, null, 'is a workflow class and cannot declare effectivelyOnce()');
  }
  if (spec.correctionWindowMs !== undefined) {
    refuse(spec, null, 'is a workflow class and cannot declare a correction window');
  }
  if (spec.run && spec.channels.length > 0) {
    refuse(
      spec,
      null,
      'declares a run body and channels; a run-sugar workflow is one imperative body',
    );
  }
  if (spec.syncDurability && !spec.persistent) {
    refuse(spec, null, 'declares syncDurability() without persistence');
  }
}

/** Refuse every defect static typing cannot see, naming handler and defect. */
export function validate(spec: ClassSpec): void {
  checkWorkflow(spec);
  checkTierAndWindow(spec);
  checkChannels(spec);
  checkProcedures(spec);
  checkKeying(spec);
  checkTopology(spec);
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
    consumers: channel.consumers,
    retry_max: channel.retryMax,
    retry_backoff_ms: channel.retryBackoffMs,
    overflow: channel.overflow,
    keyed: channel.keyed,
  });
}

function procedureJson(node: ProcNode): Record<string, unknown> {
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
    schema_version: def.schemaVersion ?? 1,
    min_readable_schema_version: def.minReadableSchemaVersion,
  });
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
    change_format_id: FORMAT_ID_MSGPACK,
    effectively_once: spec.effectivelyOnce,
    ack_granularity: spec.ackGranularity,
    checkpoint_every: spec.checkpointEvery,
    checkpoint_outputs: spec.checkpointOutputs,
    checkpoint_max_delay_ms: spec.checkpointMaxDelayMs,
    correction_window_ms: spec.correctionWindowMs,
    beyond_window_policy: spec.beyondWindowPolicy,
    compact_after_bytes: spec.compactAfterBytes,
    sync_durability: spec.syncDurability,
    channels: spec.channels.map(channelJson),
    procedures: spec.procedures.map(procedureJson),
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
