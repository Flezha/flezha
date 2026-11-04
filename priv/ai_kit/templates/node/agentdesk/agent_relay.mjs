import { defineRelay } from '@flezha/sdk';

import { Reply } from './reply.mjs';

/**
 * One conversation: a relay that runs in steps, one turn per message it is
 * sent, for as long as the session lasts. Every session is an instance of its
 * own, with its own state.
 */
export default defineRelay({
  factoryId: 'agent',
  // one turn per message; a failed turn fails alone
  steps: { each: 'per_input', onFailure: 'keep_progress' },
  durable: { local: { sleepAfter: 10_000 } }, // an idle session sleeps after 10 s
  channels: (c) => {
    c.input('turn', { codec: 'text' }) // entry 1: `flz relay call`
      .into(Reply)
      .output('answer'); // to whoever called, and whoever listens
    c.input('message') // entry 2: the gateway, into the same Reply
      .map((message) => message.text)
      .into(Reply);
  },
});
