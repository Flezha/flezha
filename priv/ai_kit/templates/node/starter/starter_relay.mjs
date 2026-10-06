import { defineRelay } from '@flezha/sdk';

import { CountHits } from './count.mjs';

/**
 * A visit arrives on `hit`, goes to the cell of its page, and the count leaves
 * on the output `counts`, which also answers a caller. The description is shown
 * by `flz relay info` and to AI agents.
 */
export default defineRelay({
  factoryId: 'starter', // the name `flz relay start` takes
  channels: (c) => {
    c.input('hit', {
      description: 'Counts visits per page',
      schema: { type: 'object', properties: { page: { type: 'string' } }, required: ['page'] },
    })
      .keyBy((hit) => hit.page) // one cell, and one count, per page
      .into(CountHits)
      .output('counts');
  },
});
