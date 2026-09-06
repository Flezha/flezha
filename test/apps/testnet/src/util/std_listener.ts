/**
 * Owns stdin. Two jobs, and only the first is always on:
 *
 *   - stdin closing means the parent went away, so the server exits. The
 *     controller starts the testnet as an Erlang Port in `:dev` and `:test`
 *     and terminates it by closing the port, which is our stdin.
 *   - when a line handler is given, typed lines are delivered to it. Only the
 *     standalone case passes one, so the port-managed path is unchanged.
 */
export default function (onLine?: (line: string) => void) {
  process.stdin.resume();

  if (onLine) {
    process.stdin.setEncoding('utf8');
    let pending = '';
    process.stdin.on('data', (chunk: string) => {
      pending += chunk;
      let at = pending.indexOf('\n');
      while (at !== -1) {
        onLine(pending.slice(0, at).replace(/\r$/, ''));
        pending = pending.slice(at + 1);
        at = pending.indexOf('\n');
      }
    });
  }

  process.stdin.on('end', () => {
    console.log('Stdin closed, shutting down server.');
    process.exit(0);
  });
}
