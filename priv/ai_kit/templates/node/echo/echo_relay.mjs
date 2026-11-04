import { defineRelay } from '@flezha/sdk';

import { EchoTasklet } from './echo_tasklet.mjs';

/**
 * The deployable unit. A relay declares the graph a message travels: the
 * entry channel it arrives on, the steps it passes through, and the channel its
 * result leaves on.
 *
 * `factoryId` is the relay's stable identity, and the name
 * `flz relay start` takes. The graph is checked as a whole when the runner
 * declares it, so one that cannot work is refused at startup.
 *
 * The entry names the text codec, so what `flz relay send --text` sends
 * arrives as a string. The output names no codec, and an output sends a string as its
 * UTF-8 bytes, so the output is the text itself.
 */
export default defineRelay({
  factoryId: 'echo', // the name `flz relay start` takes
  channels: (c) => {
    c.input('in', { codec: 'text' }) // messages arrive on the entry channel "in"
      .into(EchoTasklet) // each one runs through the echo tasklet
      .output('out'); // the result leaves on "out", for a plugin to pick up
  },
});
