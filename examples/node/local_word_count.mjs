// The two-procedure word count, run in this process with no controller.
//
//   node local_word_count.mjs
//
// `Split` and `Tally` are ordinary procedures with nothing local-mode about
// them: the same definition deploys against a controller unchanged. What
// local mode supplies is the sink — the values `Tally` releases arrive here
// instead of on a socket.
//
// State is held in memory and nothing is durable; the run says so on stderr.

import { LocalOperative, defineOperative, defineProcedure } from '@flezha/sdk';

const Split = defineProcedure({
  name: 'split',
  run: (line, ctx) => {
    for (const word of line.split(/\s+/).filter(Boolean)) {
      ctx.emit(word.toLowerCase());
    }
  },
});

const Tally = defineProcedure({
  name: 'tally',
  dynamicEntities: { idleTtlMs: 300_000 },
  initialState: () => ({ seen: 0 }),
  applyChange: (state, change) => {
    state.seen = change.seen;
  },
  run: (word, ctx) => {
    const seen = ctx.state.seen + 1;
    ctx.change({ seen });
    ctx.emit(`${word} ${seen}`);
  },
});

const WordCount = defineOperative({
  factoryId: 'local_word_count_example',
  channels: (c) => {
    c.input('lines', { codec: 'text' })
      .into(Split)
      // One Entity per word: the hop from `Split` to `Tally` is the
      // runtime's, not this file's.
      .keyBy((word) => word)
      .into(Tally)
      .keyed()
      .sink('totals');
  },
});

const run = LocalOperative.start(WordCount);
try {
  run.onSink('totals', (output) => console.log(output.payload.toString('utf8')));
  await run.feed('lines', 'the quick brown fox');
  await run.feed('lines', 'the lazy dog');
  await run.runUntilIdle(10_000);
} finally {
  await run.close();
}
