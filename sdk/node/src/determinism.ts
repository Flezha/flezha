/**
 * Warnings for ambient non-determinism inside an invocation.
 *
 * A handler that reads the wall clock or generates a random identifier
 * produces a different answer on every replay, and the engine replays: a
 * crash-resume, a retry and a correction all re-enter the same handler with
 * the same inputs and expect the same effects. `ctx.now()` and
 * `ctx.stableId()` are the replay-stable equivalents.
 *
 * This warns, and never aborts. The reads are legitimate outside the
 * determinism contract — logging a timestamp, seeding a cache key — and an SDK
 * that refused them would be wrong more often than the author is. One warning
 * per source location, so a handler in a hot loop reports once.
 */

import { AsyncLocalStorage } from 'node:async_hooks';

import { log } from './logger.js';

/** The procedure whose invocation the current async context belongs to. */
export const invocation = new AsyncLocalStorage<{ procedure: string }>();

const reported = new Set<string>();
let installed = false;

interface Originals {
  dateNow: () => number;
  DateCtor: DateConstructor;
  random: () => number;
  randomUUID?: typeof globalThis.crypto.randomUUID;
}

let originals: Originals | null = null;

/**
 * The first frame outside this module — the author's call site.
 *
 * Matched on this module's own file name rather than a substring, because an
 * author's file may well be called `determinism.test.mjs`, and skipping it
 * would report the SDK's frame instead of theirs.
 */
const OWN_FRAME = /[/\\]determinism\.[cm]?[jt]s:\d+:\d+/;

function callSite(): string {
  const stack = new Error().stack ?? '';
  for (const line of stack.split('\n').slice(1)) {
    if (OWN_FRAME.test(line)) continue;
    const at = line.match(/\(?([^\s()]+:\d+:\d+)\)?\s*$/);
    if (at) return at[1] as string;
  }
  return '<unknown location>';
}

function report(what: string, instead: string): void {
  const context = invocation.getStore();
  if (!context) return;
  const site = callSite();
  const key = `${site}:${what}`;
  if (reported.has(key)) return;
  reported.add(key);
  log().warn(
    `${context.procedure} read ${what} at ${site}. That value differs on every replay, and ` +
      `the engine replays — a crash-resume, a retry and a correction all re-enter this ` +
      `handler expecting the same effects. Use ${instead}.`,
  );
}

/**
 * Install the warnings. Called once when a runner starts; the globals are
 * restored by `uninstallDeterminismWarnings`, which a test suite uses so the
 * patches never outlive the runner that wanted them.
 */
export function installDeterminismWarnings(): void {
  if (installed) return;
  installed = true;

  const RealDate = globalThis.Date;
  originals = {
    dateNow: RealDate.now,
    DateCtor: RealDate,
    random: Math.random,
    randomUUID: globalThis.crypto?.randomUUID?.bind(globalThis.crypto),
  };

  const patchedNow = () => {
    report('Date.now()', 'ctx.now()');
    return (originals as Originals).dateNow.call(RealDate);
  };

  // A Proxy rather than a subclass: `new Date()` with no argument reads the
  // ambient clock, while `new Date(ctx.now())` does not, and only the
  // construct trap can tell them apart.
  const patchedDate = new Proxy(RealDate, {
    construct(target, args, newTarget) {
      if (args.length === 0) report('new Date()', 'ctx.now()');
      return Reflect.construct(target, args, newTarget);
    },
    get(target, property, receiver) {
      if (property === 'now') return patchedNow;
      return Reflect.get(target, property, receiver);
    },
  });
  globalThis.Date = patchedDate as DateConstructor;

  Math.random = () => {
    report('Math.random()', 'ctx.stableId()');
    return (originals as Originals).random.call(Math);
  };

  if (globalThis.crypto?.randomUUID) {
    globalThis.crypto.randomUUID = (() => {
      report('crypto.randomUUID()', 'ctx.stableId()');
      return (originals as Originals).randomUUID?.();
    }) as typeof globalThis.crypto.randomUUID;
  }
}

export function uninstallDeterminismWarnings(): void {
  if (!installed || !originals) return;
  globalThis.Date = originals.DateCtor;
  Math.random = originals.random;
  if (originals.randomUUID && globalThis.crypto) {
    globalThis.crypto.randomUUID = originals.randomUUID as typeof globalThis.crypto.randomUUID;
  }
  installed = false;
  originals = null;
  reported.clear();
}
