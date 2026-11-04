import { defineTasklet } from '@flezha/sdk';

/**
 * A processing stage of the relay's graph, run once per message. This one
 * prefixes the input and emits the result through its context, `ctx.emit`.
 *
 * `name` is the tasklet's stable identity. State is filed under it, so
 * renaming it later declares a different tasklet rather than renaming this
 * one. With no `initState` and no `applyChange`, nothing is kept between
 * messages and nothing is journaled.
 *
 * Where the result goes is not decided here. The relay decides that, so the
 * graph can be rewired without touching this tasklet.
 */
export const EchoTasklet = defineTasklet({
  name: 'echo', // state is saved under this name, so choose it once
  run: (input, ctx) => ctx.emit(`echo: ${input}`), // where it goes is the relay's business
});
