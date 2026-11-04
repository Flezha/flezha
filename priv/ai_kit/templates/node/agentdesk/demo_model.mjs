import { setTimeout as sleep } from 'node:timers/promises';

const MAX_MESSAGE = 200;
const PER_TOKEN_MS = 50;

/**
 * The demo model: a canned answer, one token at a time, and the number of
 * tokens it used. It stands in for a real model so the example runs anywhere; a
 * real model's client would be called here instead, handing each token it
 * streams to `part` the same way.
 *
 * An answer, and each part of one, is an object of its text and the tokens used.
 */
export async function answer(earlier, message, part) {
  if (message.length > MAX_MESSAGE) {
    const refusal = `the demo model reads at most ${MAX_MESSAGE} characters, and this message has ${message.length}`;
    throw Object.assign(new Error(refusal), { nonRetryable: true });
  }
  const text = `Message ${earlier.length + 1}: you said "${message}". I am a demo model.`;
  const tokens = text.split(/(?<= )/);
  for (const [i, token] of tokens.entries()) {
    part({ text: token, tokens: i + 1 });
    await sleep(PER_TOKEN_MS);
  }
  return { text, tokens: tokens.length };
}
