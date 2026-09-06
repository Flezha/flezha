/**
 * The manual input driver: lines typed at the testnet become frames on the
 * sockets it is already feeding.
 *
 * `SubscriptionRouter` answers what a client subscribed to; this originates
 * traffic the operator chose, on whatever connections are open. The two share
 * the `Timers` seam and nothing else.
 *
 * Templates use the `{{...}}` spelling the websocket subscription source
 * already uses for `{{items}}` and `{{req_id}}`.
 */

import { REAL_TIMERS, Timers } from '../util/timers';
import { InputSockets } from './server_contract';

export type Command =
  | { kind: 'blank' }
  | { kind: 'send'; message: string }
  | { kind: 'bulk'; count: number; durationMs: number; template: string }
  | { kind: 'stop' }
  | { kind: 'peers' }
  | { kind: 'help' }
  | { kind: 'error'; reason: string };

const HELP = [
  'Type a line to send it to every connected input socket.',
  '',
  '  <any text>                        send this line verbatim',
  '  /bulk <count> <duration> <text>   send <count> copies spread over <duration>',
  '  /stop                             cancel a running bulk',
  '  /peers                            list connected sockets',
  '  /help                             this text',
  '',
  'Durations are 250ms, 30s or 2m; a bare number is milliseconds.',
  'In /bulk text, {{n}} becomes the 1-based index and {{ts}} an ISO timestamp.',
].join('\n');

const BULK = /^\/bulk\s+(\S+)\s+(\S+)\s+([\s\S]+)$/;

/**
 * Whether typed lines are read. Manual input is for a person at a terminal:
 * under the controller the testnet is an Erlang Port and stdin is the shutdown
 * channel, never a keyboard. `override` wins either way, for scripting.
 */
export function manualInputEnabled(override: string | undefined, isTty: boolean): boolean {
  if (override === undefined || override === '') return isTty;
  return override !== '0';
}

/** Milliseconds for `250ms` / `30s` / `2m` / a bare number, or null. */
export function parseDuration(text: string): number | null {
  const match = /^(\d+)(ms|s|m)?$/.exec(text);
  if (!match) return null;
  const value = Number(match[1]);
  switch (match[2]) {
    case 's':
      return value * 1000;
    case 'm':
      return value * 60_000;
    default:
      return value;
  }
}

/**
 * One typed line as a command. Anything not starting with `/` is a payload,
 * so a JSON object needs no escaping.
 */
export function parseCommand(line: string): Command {
  const trimmed = line.trim();
  if (trimmed === '') return { kind: 'blank' };
  if (!trimmed.startsWith('/')) return { kind: 'send', message: line };

  const verb = trimmed.split(/\s+/)[0];
  switch (verb) {
    case '/help':
      return { kind: 'help' };
    case '/peers':
      return { kind: 'peers' };
    case '/stop':
      return { kind: 'stop' };
    case '/bulk': {
      const parts = BULK.exec(trimmed);
      if (!parts) {
        return { kind: 'error', reason: 'usage: /bulk <count> <duration> <text>' };
      }
      const [, rawCount, rawDuration, template] = parts;
      const count = /^\d+$/.test(rawCount) ? Number(rawCount) : 0;
      if (count < 1) {
        return { kind: 'error', reason: `count must be a positive integer, got "${rawCount}"` };
      }
      const durationMs = parseDuration(rawDuration);
      if (durationMs === null) {
        return {
          kind: 'error',
          reason: `duration must be 250ms, 30s, 2m or a bare number, got "${rawDuration}"`,
        };
      }
      return { kind: 'bulk', count, durationMs, template };
    }
    default:
      return { kind: 'error', reason: `unknown command "${verb}" — try /help` };
  }
}

/** `{{n}}` and `{{ts}}` in a bulk template. */
export function render(template: string, index: number, now: () => string): string {
  return template.replace(/\{\{n\}\}/g, String(index)).replace(/\{\{ts\}\}/g, now());
}

export class ManualDriver {
  private running: { handle: unknown; sent: number; count: number } | null = null;

  constructor(
    private readonly sockets: InputSockets,
    private readonly print: (line: string) => void,
    private readonly timers: Timers = REAL_TIMERS,
    private readonly now: () => string = () => new Date().toISOString()
  ) {}

  public handleLine(line: string): void {
    const command = parseCommand(line);
    switch (command.kind) {
      case 'blank':
        return;
      case 'help':
        return this.print(HELP);
      case 'error':
        return this.print(`error: ${command.reason}`);
      case 'peers':
        return this.printPeers();
      case 'stop':
        return this.stop();
      case 'send': {
        const recipients = this.sockets.broadcast(command.message);
        return this.print(`sent to ${recipients} socket(s)`);
      }
      case 'bulk':
        return this.startBulk(command.count, command.durationMs, command.template);
    }
  }

  /** Cancel a running bulk. Idempotent, so the shutdown path can call it. */
  public stop(): void {
    if (!this.running) {
      this.print('no bulk running');
      return;
    }
    const { handle, sent, count } = this.running;
    this.timers.clear(handle);
    this.running = null;
    this.print(`bulk cancelled after ${sent}/${count}`);
  }

  private printPeers(): void {
    const sockets = this.sockets.connected();
    if (sockets.length === 0) {
      this.print('no sockets connected');
      return;
    }
    this.print(
      sockets
        .map(
          ({ path, role }, at) =>
            `${at + 1}. ${path}${role === 'output' ? ' (output, not fed)' : ''}`
        )
        .join('\n')
    );
  }

  private startBulk(count: number, durationMs: number, template: string): void {
    if (this.running) {
      this.print('a bulk is already running — /stop it first');
      return;
    }
    // Spread count sends across the window. A zero window and a window shorter
    // than the send count both collapse to the 1ms floor, which is "as fast as
    // the loop allows" rather than a burst the timer cannot deliver.
    const interval = Math.max(1, Math.floor(durationMs / count));
    this.print(`bulk: ${count} message(s), one every ${interval}ms`);

    const handle = this.timers.set(() => {
      if (!this.running) return;
      this.running.sent += 1;
      const recipients = this.sockets.broadcast(render(template, this.running.sent, this.now));
      if (this.running.sent === 1 && recipients === 0) {
        this.print('warning: no sockets connected, messages are going nowhere');
      }
      if (this.running.sent >= this.running.count) {
        this.timers.clear(this.running.handle);
        this.print(`bulk complete: ${this.running.sent} sent`);
        this.running = null;
      }
    }, interval);

    this.running = { handle, sent: 0, count };
  }
}
