import { defineTasklet } from '@flezha/sdk';

/**
 * Counts the visits of one page and answers the new count.
 *
 * The relay keys the channel by page, so each page has a cell of its own, and
 * `{ total }` is that cell's state. The state changes only through the change
 * `run` records, which `applyChange` applies, so a runner that restarts finds
 * the count it had.
 */
export const CountHits = defineTasklet({
  name: 'count', // state is saved under this name, so choose it once
  initState: () => ({ total: 0 }), // what each page's cell remembers
  applyChange: (seen) => {
    seen.total += 1; // how a visit changes it
  },
  run: (hit, ctx) => {
    ctx.change({});
    ctx.emit({ page: hit.page, total: ctx.state.total });
  },
});
