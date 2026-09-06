/**
 * What the path of an upgrade request means.
 *
 * The testnet feeds input sockets and never feeds output ones: a sink attached
 * to `/out` must not receive the manual traffic the operator is typing, or the
 * loop is fed its own output. One classifier so the broadcast rule and the
 * `/out` handling cannot drift apart.
 */

/** The path a sink connects on. Everything else takes input. */
export const OUTPUT_PATH = '/out';

export type ConnectionRole = 'input' | 'output';

/** The path of an upgrade request, with the query string dropped. */
export function requestPath(url: string | undefined): string {
  const path = (url ?? '/').split('?')[0];
  return path === '' ? '/' : path;
}

export function roleFor(path: string): ConnectionRole {
  return path === OUTPUT_PATH ? 'output' : 'input';
}
