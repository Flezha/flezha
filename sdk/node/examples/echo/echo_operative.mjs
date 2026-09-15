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
 * The entry names the text codec, so what `flz operative send --text` sends
 * arrives as a string. The sink names no codec, and a sink sends a string as its
 * UTF-8 bytes, so the output is the text itself.
 */
export default defineOperative({
  factoryId: 'echo',
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(EchoProcedure).sink('out');
  },
});
