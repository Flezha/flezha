// The refusal table, driven through the Node SDK's own builder.
//
// `test/fixtures/reference_topologies.txt` names one refusal per row, with the
// authoring steps in a language-neutral spelling every SDK maps to its own
// surface. This module is that mapping for Node: it reads the fixture, binds
// the procedure palette the fixture header declares, and submits each row the
// same way `validate()` does — so the Node SDK is measured against the whole
// table rather than against the rows it happens to declare itself.
//
// The palette lives here rather than in `fixtures.mjs`: these procedures exist
// only for the conformance run and are not part of the app's declared set.
//
// `flz_core/src/host/check_tests.rs` is the same interpreter written against
// the record instead of a builder, and is the reference for what each step
// means.

import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  ACTIVATION_SCHEMA_VERSION,
  DeclarationError,
  defineOperative,
  defineProcedure,
  defineWorkflow,
  loadNative,
  validate,
} from '@flezha/sdk';

import { hybridChannel } from './fixtures.mjs';
import { tierCommitBlockWorkflow } from './fixtures_workflow.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const FIXTURE = resolve(HERE, '../../fixtures/reference_topologies.txt');

/** The authoring surfaces this builder carries — `conformance.json` states it. */
const SURFACES = ['branch', 'sync_durability', 'workflow'];

/**
 * The links that leave a channel option PENDING: each answers a new handle and
 * nothing applies it until the next terminator, so the handle they were
 * declared off has to become that answer.
 */
const KNOBS = ['keyed', 'retry_max', 'on_full', 'named'];

/**
 * The two rows the fixture cites rather than re-spells. They are this app's own
 * reference twins: `commit_block_chain` is `tier_commit_block_workflow`'s
 * topology, which is why the fixture names no steps for either.
 */
const CITED = {
  hybrid_channel: hybridChannel,
  commit_block_chain: tierCommitBlockWorkflow,
};

/** The four-column refusal table, as data. */
export function rows() {
  return readFileSync(FIXTURE, 'utf8')
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line !== '' && !line.startsWith('#'))
    .map((line) => line.split('|').map((cell) => cell.trim()))
    .filter((columns) => columns.length === 4)
    .map(([refusal, topology, steps, needs]) => ({ refusal, topology, steps, needs }));
}

/** `verb(arg)` → `['verb', 'arg']`; a bare word → `['word', '']`. */
function call(text) {
  const open = text.indexOf('(');
  if (open === -1) return [text.trim(), ''];
  return [text.slice(0, open).trim(), text.slice(open + 1, text.lastIndexOf(')')).trim()];
}

/** `<handle>.<rest>` → `['handle', 'rest']`; a call with no chain → `[step]`. */
function splitHead(step) {
  const at = step.indexOf('.');
  return at === -1 ? [step, undefined] : [step.slice(0, at), step.slice(at + 1)];
}

/**
 * One fixture procedure per palette role. `mailbox` is the row's
 * `<proc>.mailbox(n)`, which is a procedure knob in this SDK rather than a
 * builder call, so it is read before the graph is declared.
 */
function palette(role, mailbox) {
  const handle = (value) => value;
  const common = { mailboxCapacity: mailbox, handle };
  switch (role) {
    case 'plain':
    case 'second':
      return defineProcedure({ name: role, ...common });
    // A twin is a DIFFERENT procedure claiming `plain`'s name: that is the
    // collision the row names.
    case 'twin':
      return defineProcedure({ name: 'plain', ...common });
    case 'static':
      return defineProcedure({ name: role, entities: { keys: ['a', 'b'] }, ...common });
    case 'routed':
      return defineProcedure({
        name: role,
        entities: { keys: ['a', 'b'], entitiesFor: () => ['a'] },
        ...common,
      });
    case 'dynamic':
      return defineProcedure({ name: role, dynamicEntities: {}, ...common });
    // A logical clock over a procedure that journals nothing: no `applyChange`
    // is what makes the change type empty.
    case 'nochange':
      return defineProcedure({ name: role, clock: 'logical', ...common });
    case 'eventtime':
      return defineProcedure({ name: role, clock: 'event_time', eventTime: () => 0n, ...common });
    default:
      throw new Error(`the fixture names a palette role this interpreter does not bind: ${role}`);
  }
}

/**
 * The facts a row states before its graph is built: which class kind to
 * declare, whether it journals, and each procedure's mailbox capacity — all
 * three are declaration options here, not fluent calls.
 */
function preScan(steps) {
  const mailbox = new Map();
  let workflow = false;
  let persistent = false;
  for (const step of steps) {
    const [head, rest] = splitHead(step);
    if (rest === undefined) continue;
    const [verb, arg] = call(rest);
    if (head === 'class') {
      if (verb === 'workflow') workflow = true;
      if (verb === 'persistent') persistent = true;
    } else if (verb === 'mailbox') {
      mailbox.set(head, Number(arg));
    }
  }
  return { mailbox, workflow, persistent };
}

/** `class.<flag>[(value)]` on the builder the declaration receives. */
function classFlag(builder, rest) {
  const [flag, value] = call(rest);
  switch (flag) {
    // Both are declaration options, read by `preScan`.
    case 'persistent':
    case 'workflow':
      return;
    case 'effectively_once':
      builder.effectivelyOnce();
      return;
    case 'checkpoint_ack':
      builder.checkpointAck();
      return;
    case 'sync_durability':
      builder.syncDurability();
      return;
    case 'checkpoint_every':
      builder.checkpointEvery(Number(value));
      return;
    case 'correction_window':
      builder.correctionWindow(Number(value));
      return;
    case 'outputs_at_checkpoint':
      builder.outputsAtCheckpoint(Number(value));
      return;
    default:
      throw new Error(`the fixture spells a class flag this interpreter does not map: ${flag}`);
  }
}

/** The class one row's steps declare, built through the ordinary builder. */
function specOf(row) {
  const steps = row.steps
    .split(';')
    .map((step) => step.trim())
    .filter(Boolean);
  const { mailbox, workflow, persistent } = preScan(steps);
  const procedures = new Map();
  const procedureFor = (role) => {
    if (!procedures.has(role)) procedures.set(role, palette(role, mailbox.get(role)));
    return procedures.get(role);
  };

  const channels = (c) => {
    const handles = new Map();
    const handle = (id) => {
      const stream = handles.get(id);
      if (!stream) throw new Error(`row \`${row.topology}\` addresses an undeclared handle: ${id}`);
      return stream;
    };

    // One fluent sequence, left to right. `into` answers the consuming
    // procedure's output channel, which the role then addresses; a knob answers
    // this same handle with one more option pending on it, which is why the
    // handle has to become the answer.
    const chain = (id, rest) => {
      const links = rest.split('.');
      let stream = handle(id);
      let bind = KNOBS.includes(call(links[links.length - 1])[0]) ? id : null;
      for (const link of links) {
        const [verb, arg] = call(link);
        switch (verb) {
          case 'map':
            stream = stream.map((value) => value);
            break;
          case 'key':
            stream = stream.keyBy((value) => String(value));
            break;
          case 'named':
            stream = stream.named(arg);
            break;
          case 'keyed':
            stream = stream.keyed();
            break;
          case 'retry_max':
            stream = stream.retryMax(Number(arg));
            break;
          case 'on_full':
            stream = stream.onFull(arg);
            break;
          case 'sink':
            stream = stream.sink(arg);
            break;
          case 'into':
            stream = stream.into(procedureFor(arg));
            bind = arg;
            break;
          default:
            throw new Error(`the fixture spells a link this interpreter does not map: ${verb}`);
        }
      }
      if (bind !== null) handles.set(bind, stream);
    };

    for (const step of steps) {
      const [head, rest] = splitHead(step);
      if (rest === undefined) {
        const [verb, arg] = call(step);
        if (verb === 'entry') {
          handles.set(arg, c.input(arg));
          continue;
        }
        if (verb === 'branch') {
          const [id, arms] = arg.split(',').map((part) => part.trim());
          // Each arm wires itself off this same handle; the fixture's arms are
          // empty, so the arity is all the row states.
          handles.set(id, handle(id).branch(Array.from({ length: Number(arms) }, () => () => {})));
          continue;
        }
        throw new Error(`the fixture spells a step this interpreter does not map: ${step}`);
      }
      if (head === 'class') {
        classFlag(c, rest);
        continue;
      }
      // The mailbox capacity was read by `preScan` and is already on the
      // procedure this row wires.
      if (call(rest)[0] === 'mailbox') continue;
      chain(head, rest);
    }
  };

  return workflow
    ? defineWorkflow({ factoryId: row.topology, persistent, channels })
    : defineOperative({ factoryId: row.topology, persistent, channels });
}

/** The class a topology id declares — the cited twins by name, the rest by steps. */
export function specForTopology(topology) {
  const cited = CITED[topology];
  if (cited) return cited;
  const row = rows().find((candidate) => candidate.topology === topology);
  if (!row) throw new Error(`${FIXTURE} has no row for topology \`${topology}\``);
  return specOf(row);
}

/**
 * Whether a row needs a surface this builder does not carry.
 *
 * `detached`, `raw_flags` and `raw_class_flags` are in no builder's list: a
 * fluent handle cannot name a node nothing wired, `outputsAtCheckpoint(ms)`
 * cannot write the flag without its delay, and a tier key is gated behind the
 * class kind. The fixture header states all three.
 */
function skipped(row) {
  return row.needs !== '-' && !SURFACES.includes(row.needs);
}

/** The derived declaration in the fixture's own columns: consumers are indices. */
function derivedOf(declaration) {
  return declaration.channels.map((channel) => ({
    index: channel.index,
    name: channel.name,
    kind: channel.kind,
    producer: channel.producer ?? null,
    consumers: channel.consumers.map((edge) => edge.procedure),
    sink: channel.sink ?? null,
  }));
}

/** Drive one row: skipped, refused with the core's own names, or accepted. */
function judge(row) {
  if (skipped(row)) return { topology: row.topology, outcome: 'skipped', needs: row.needs };
  try {
    return {
      topology: row.topology,
      outcome: 'accepted',
      derived: derivedOf(validate(specForTopology(row.topology))),
    };
  } catch (error) {
    if (!(error instanceof DeclarationError)) throw error;
    // The names are the verdict's own, never re-derived here. A refusal this
    // SDK made on its own surface carries none, and says so.
    return {
      topology: row.topology,
      outcome: 'refused',
      names: [...error.refusals],
      defect: error.message,
    };
  }
}

/**
 * Drive the whole table and write the report the conformance drill reads.
 *
 * No controller is reached: the context the check needs is created and freed
 * here, and nothing else in the runner is started.
 */
export function writeConformanceReport(path) {
  const native = loadNative();
  native.contextNew(ACTIVATION_SCHEMA_VERSION);
  try {
    const report = { runtime: 'node', rows: rows().map(judge) };
    writeFileSync(path, `${JSON.stringify(report, null, 2)}\n`);
  } finally {
    native.contextFree();
  }
}

/**
 * With `FLEZHA_REFUSAL_FIXTURE` set, the ONE topology this process registers.
 * `undefined` otherwise, so the runner's registry scan ignores this module.
 */
export const refusalFixture = process.env.FLEZHA_REFUSAL_FIXTURE
  ? specForTopology(process.env.FLEZHA_REFUSAL_FIXTURE)
  : undefined;
