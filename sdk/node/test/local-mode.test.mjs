// The Node SDK's local mode: a user's own operative definition, its declared
// two-procedure graph, and no controller anywhere.
//
// The graph and its expected sink output are read from
// `test/fixtures/local_mode_word_count.txt`, which the Rust, Java and Elixir
// local-mode suites read too — so every core is compared against one recorded
// expectation instead of by inspection.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { LocalOperative, defineOperative, defineProcedure } from '../dist/esm/index.js';

const here = dirname(fileURLToPath(import.meta.url));
const utf8 = (bytes) => Buffer.from(bytes).toString('utf8');
const bytes = (text) => Buffer.from(text, 'utf8');

/** One line into its words: the hop to `Tally` is the runtime's job. */
const Split = defineProcedure({
  name: 'split',
  handle: (line, ctx) => {
    for (const word of line.split(/\s+/).filter(Boolean)) {
      ctx.emit(word.toLowerCase());
    }
  },
});

/** One Entity per word, minted on first sight of it. */
const Tally = defineProcedure({
  name: 'tally',
  dynamicEntities: { idleTtlMs: 300_000 },
  initialState: () => ({ seen: 0 }),
  applyChange: (state, change) => {
    state.seen = change.seen;
  },
  handle: (word, ctx) => {
    const seen = ctx.state.seen + 1;
    ctx.change({ seen });
    ctx.emit(`${word} ${seen}`);
  },
});

const WordCount = defineOperative({
  factoryId: 'local_word_count_node',
  channels: (c) => {
    c.input('lines', { codec: 'text' })
      .into(Split)
      .keyBy((word) => word)
      .into(Tally)
      .keyed()
      .sink('totals');
  },
});

/** The scenario every core runs: the lines to feed, and the sink values. */
function scenario() {
  const text = readFileSync(
    resolve(here, '../../../test/fixtures/local_mode_word_count.txt'),
    'utf8',
  );
  const lines = [];
  const totals = new Map();
  for (const row of text.split('\n')) {
    if (row.startsWith('lines: ')) {
      lines.push(row.slice('lines: '.length));
    } else if (row.startsWith('totals: ')) {
      const total = row.slice('totals: '.length);
      totals.set(total, (totals.get(total) ?? 0) + 1);
    }
  }
  return { lines, expected: totals };
}

test('a declared graph runs locally and releases the recorded output', async () => {
  const { lines, expected } = scenario();
  const released = [];
  const run = LocalOperative.start(WordCount);
  try {
    run.onSink('totals', (output) => released.push(output));
    for (const line of lines) {
      await run.feed('lines', line);
    }
    await run.runUntilIdle(20_000);
  } finally {
    await run.close();
  }

  const counted = new Map();
  for (const output of released) {
    const payload = utf8(output.payload);
    counted.set(payload, (counted.get(payload) ?? 0) + 1);
  }
  assert.deepEqual(
    [...counted].sort(),
    [...expected].sort(),
    'both procedures ran, the runtime performed the hop, and one Entity per word counted',
  );

  const first = released[0];
  assert.equal(first.channel, 'totals');
  for (const stamp of ['flz.entity', 'flz.seq', 'flz.key']) {
    assert.ok(
      stamp in first.metadata,
      `a keyed sink carries ${stamp}: ${JSON.stringify(first.metadata)}`,
    );
  }
});

test('the core reads a payload view, at its own offset, exactly as it reads a Buffer', async () => {
  // What the host hands the core is no longer copied into a `Buffer` first, so
  // the reader has to honour a view's `byteOffset` and `byteLength` rather than
  // read its backing store from the start. A wrong reader answers the padding.
  const line = 'alpha beta alpha';
  const padded = Buffer.concat([bytes('PAD'), bytes(line), bytes('TAIL')]);
  const view = new Uint8Array(padded.buffer, padded.byteOffset + 3, line.length);
  assert.notEqual(view.byteOffset, 0, 'the fixture is only a test while the view is offset');

  for (const payload of [view, bytes(line), line]) {
    const released = [];
    const run = LocalOperative.start(WordCount);
    try {
      run.onSink('totals', (output) => released.push(utf8(output.payload)));
      await run.feed('lines', payload);
      await run.runUntilIdle(20_000);
    } finally {
      await run.close();
    }
    assert.deepEqual(released.sort(), ['alpha 1', 'alpha 2', 'beta 1']);
  }
});

test('a channel the class does not declare is refused by name', async () => {
  const run = LocalOperative.start(WordCount);
  try {
    await assert.rejects(
      () => run.feed('nope', '1'),
      (refusal) => refusal.message.includes('nope') && refusal.message.includes('lines'),
      'the refusal names the channel and what is declared',
    );
  } finally {
    await run.close();
  }
});

/** Reads the map an undeclared entry decodes, and emits a string and a map. */
const Defaults = defineProcedure({
  name: 'defaults',
  handle: (value, ctx) => {
    ctx.emit(`a is ${value.a}`);
    ctx.emit({ a: value.a });
  },
});

const DefaultCodecs = defineOperative({
  factoryId: 'local_default_codecs_node',
  channels: (c) => {
    c.input('in').into(Defaults).sink('out');
  },
});

test('an undeclared entry reads JSON and an undeclared sink follows the value', async () => {
  const released = [];
  const run = LocalOperative.start(DefaultCodecs);
  try {
    run.onSink('out', (output) => released.push(utf8(output.payload)));
    await run.feed('in', '{"a":1}');
    await run.runUntilIdle(5_000);
  } finally {
    await run.close();
  }
  assert.deepEqual(released, ['a is 1', '{"a":1}']);
});
