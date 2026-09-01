// Helpers every fixture module shares: the wire codecs the reference's text
// payloads and plugin frames need, and the key convention keyed fixtures use.

export const text = (bytes) => Buffer.from(bytes).toString('utf8');
export const bytes = (value) => Buffer.from(String(value), 'utf8');

/** The key convention every keyed fixture shares: text before the first ':'. */
export const prefix = (input) => String(input).split(':')[0];
