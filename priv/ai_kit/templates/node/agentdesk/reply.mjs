import { defineTasklet } from '@flezha/sdk';

import { answer } from './demo_model.mjs';

/**
 * Answers a message with the demo model, sending the answer as it grows, and
 * keeps the conversation so the next answer knows what came before.
 */
export const Reply = defineTasklet({
  name: 'reply',

  initState: () => ({ messages: [] }), // what the session remembers

  applyChange: (conversation, change) => {
    if (change.kind === 'said') conversation.messages.push(change.message);
    else conversation.messages.pop(); // 'unsaid'
  },

  run: async (message, ctx) => {
    // Each piece streams out as it is made; then the whole answer.
    ctx.emit(await answer(ctx.state.messages, message, (part) => ctx.part(part)));
    ctx.change({ kind: 'said', message });
  },

  // The undo, when a turn is taken back: the conversation forgets its message.
  compensate: (_answers, ctx) => {
    console.log(`[undo] forgot "${ctx.state.messages.at(-1)}"`);
    ctx.change({ kind: 'unsaid' });
  },
});
