import { defineTasklet } from '@flezha/sdk';

/**
 * Answers a request for a session's events with its stream, and any other
 * request with 404.
 */
export const Watch = defineTasklet({
  name: 'watch',

  run: (request, ctx) =>
    // keyed by the user: an output of an effectively-once relay needs a key
    ctx.emit(
      request.listens() ? { stream: request.user() } : { status: 404, body: 'no such route' },
      { key: request.user() },
    ),
});
