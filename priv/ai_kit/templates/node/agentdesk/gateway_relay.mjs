import { defineRelay } from '@flezha/sdk';

import { Admit } from './admit.mjs';
import { Budget, admitted, used } from './budget.mjs';
import { Request } from './request.mjs';
import { Watch } from './watch.mjs';

const DEADLINE_MS = 60_000;
const user = (value) => value.user;

/**
 * The HTTP side of the desk: a flow relay that carries the HTTP
 * plugin, answers every request, and calls the session a message is for.
 */
export default defineRelay({
  factoryId: 'gateway',
  flow: { exactly: 'effectively_once' }, // each request and answer on disk before it runs
  channels: (c) => {
    // every request arrives on "http"; kept as a handle to split it
    const http = c.input('http', Request.decode);
    // route 1: GET .../events gets the session's stream
    http
      .filter((request) => !request.sends())
      .into(Watch)
      .keyed()
      .output(); // a stream, or 404
    // route 2: POST .../messages is checked against the user's budget
    const verdicts = http
      .filter((request) => request.sends())
      .map((request) => request.message())
      .keyBy(user) // one cell per user
      .into(Budget);
    // a verdict: the message plus its user's tokens today; used twice
    // use 1: answer the POST
    verdicts.into(Admit).keyed().output(); // 202, or 429 over the budget
    // use 2: pass an admitted message on to the user's session
    verdicts
      .filter(admitted)
      .map((verdict) => verdict.message)
      .keyBy(user) // the user's session
      // call the "agent" relay; its answer comes back here
      .call({ factory: 'agent', channel: 'message' }, { deadlineMs: DEADLINE_MS })
      .map(used) // the session's answer, as tokens used
      .keyBy(user)
      .into(Budget); // back into the same budget
  },
});
