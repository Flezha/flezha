// Helpers every fixture module shares.

/** The key convention every keyed fixture shares: text before the first ':'. */
export const prefix = (input) => String(input).split(':')[0];
