import { defineOperative } from '@flezha/sdk';

import { EchoProcedure } from './echo_procedure.mjs';

/**
 * The deployable unit. An operative declares the graph a message travels: the
 * entry channel it arrives on, the steps it passes through, and the channel its
 * result leaves on.
 *
 * `factoryId` is the operative's stable identity, and the name
 * `flz operative start` takes. The graph is checked as a whole when the runner
 * declares it, so one that cannot work is refused at startup.
 *
 * Values cross the wire as MessagePack unless a channel says otherwise. This one
 * carries plain text both ways, so `flz operative send --text` reads naturally
 * and the output is the text itself.
 */
export default defineOperative({
  factoryId: 'echo',
  channels: (c) => {
    c.input('in', (bytes) => Buffer.from(bytes).toString('utf8'))
      .into(EchoProcedure)
      .sink('out', (value) => Buffer.from(value, 'utf8'));
  },
});
