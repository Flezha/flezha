import { defineTasklet } from '@flezha/sdk';

import { DAILY_TOKENS, admitted } from './budget.mjs';

/**
 * Answers a message 202 when it goes on to the session, and 429 when its user
 * is over the budget.
 */
export const Admit = defineTasklet({
  name: 'admit',

  run: (verdict, ctx) => {
    const { user } = verdict.message;
    ctx.emit(
      admitted(verdict)
        ? { status: 202, body: `sent to ${user}` }
        : {
            status: 429,
            body: `${user} has used ${verdict.spent} of today's ${DAILY_TOKENS} tokens`,
          },
      { key: user },
    );
  },
});
