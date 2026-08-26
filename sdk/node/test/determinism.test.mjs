// Ambient non-determinism is reported, not refused.
import assert from 'node:assert/strict';
import test from 'node:test';

import { defineProcedure, setLogger } from '../dist/esm/index.js';
import {
  installDeterminismWarnings,
  uninstallDeterminismWarnings,
} from '../dist/esm/determinism.js';
import { ProcedureTestHarness } from '../dist/esm/testing.js';

function capture() {
  const lines = [];
  setLogger({
    debug: (m) => lines.push(m),
    info: (m) => lines.push(m),
    warn: (m) => lines.push(m),
    error: (m) => lines.push(m),
  });
  return lines;
}

test('an ambient clock read names the file, the line and the replacement', async () => {
  const lines = capture();
  installDeterminismWarnings();
  try {
    const Ambient = defineProcedure({
      name: 'ambient_clock',
      handle: () => {
        Date.now();
        return undefined;
      },
    });
    await ProcedureTestHarness.of(Ambient).input(1);
    const warning = lines.find((line) => line.includes('Date.now()'));
    assert.ok(warning, `expected a warning, got ${JSON.stringify(lines)}`);
    assert.match(warning, /ambient_clock/);
    assert.match(warning, /determinism\.test\.mjs:\d+:\d+/);
    assert.match(warning, /ctx\.now\(\)/);
  } finally {
    uninstallDeterminismWarnings();
  }
});

test('new Date() with no argument is reported, and with one is not', async () => {
  const lines = capture();
  installDeterminismWarnings();
  try {
    const Mixed = defineProcedure({
      name: 'mixed_clock',
      handle: (_input, ctx) => {
        const derived = new Date(Number(ctx.now()));
        assert.ok(derived instanceof Date);
        new Date();
        return undefined;
      },
    });
    await ProcedureTestHarness.of(Mixed).input(1);
    assert.equal(lines.filter((line) => line.includes('new Date()')).length, 1);
  } finally {
    uninstallDeterminismWarnings();
  }
});

test('random identifiers are reported and the invocation still completes', async () => {
  const lines = capture();
  installDeterminismWarnings();
  try {
    const Random = defineProcedure({
      name: 'random_id',
      handle: () => {
        Math.random();
        globalThis.crypto.randomUUID();
        return 'finished';
      },
    });
    const applied = await ProcedureTestHarness.of(Random).input(1);
    assert.deepEqual(applied.emits, ['finished'], 'the warning does not abort the invocation');
    assert.ok(lines.some((line) => line.includes('Math.random()')));
    assert.ok(lines.some((line) => line.includes('crypto.randomUUID()')));
    assert.ok(lines.every((line) => line.includes('ctx.')));
  } finally {
    uninstallDeterminismWarnings();
  }
});

test('one site reports once, however hot the loop', async () => {
  const lines = capture();
  installDeterminismWarnings();
  try {
    const Hot = defineProcedure({
      name: 'hot_loop',
      handle: () => {
        for (let i = 0; i < 50; i += 1) Math.random();
        return undefined;
      },
    });
    await ProcedureTestHarness.of(Hot).input(1);
    assert.equal(lines.filter((line) => line.includes('Math.random()')).length, 1);
  } finally {
    uninstallDeterminismWarnings();
  }
});

test('an ambient read outside an invocation is not reported', () => {
  const lines = capture();
  installDeterminismWarnings();
  try {
    Date.now();
    Math.random();
    assert.deepEqual(lines, []);
  } finally {
    uninstallDeterminismWarnings();
  }
});

test('uninstalling restores the originals', () => {
  const before = Date.now;
  installDeterminismWarnings();
  assert.notEqual(Date.now, before);
  uninstallDeterminismWarnings();
  assert.equal(Date.now, before);
});
