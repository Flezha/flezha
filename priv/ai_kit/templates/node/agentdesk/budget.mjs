import { defineTasklet } from '@flezha/sdk';

export const DAILY_TOKENS = 40;

export const admitted = (verdict) => verdict.spent < DAILY_TOKENS;

// The tokens an answer used; a failed turn used none.
export const used = (response) => ({
  user: response.request.user,
  tokens: response.ok ? response.value.tokens : 0,
});

const on = (spent, today) => (spent.day === today ? spent.tokens : 0);

/**
 * Keeps the tokens a user's answers used today, and admits a message while they
 * are under the day's budget. One cell per user.
 *
 * What reaches a user's cell is a message to admit, or the tokens an answer
 * used; what it hands on is a verdict: the message, and the tokens the user had
 * spent when it arrived.
 */
export const Budget = defineTasklet({
  name: 'budget',

  initState: () => ({ day: '', tokens: 0 }), // what each user's cell remembers

  applyChange: (spent, counted) => {
    spent.tokens = on(spent, counted.day) + counted.tokens;
    spent.day = counted.day;
  },

  run: (event, ctx) => {
    const today = new Date(Number(ctx.now())).toISOString().slice(0, 10);
    if ('tokens' in event) {
      // The tokens an answer used: count them.
      ctx.change({ day: today, tokens: event.tokens });
    } else {
      // A message to admit: hand on its verdict.
      ctx.emit({ message: event, spent: on(ctx.state, today) });
    }
  },
});
