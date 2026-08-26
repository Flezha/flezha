/**
 * The SDK's diagnostics, through a logger the host supplies.
 *
 * The default writes to the console at `warn` and above, which is what the
 * other SDKs' defaults amount to: a runner that says nothing about a wedge is
 * the failure mode the diagnostics exist to close, and one that narrates every
 * activation is noise. No observability dependency — a host that wants spans
 * or structured fields supplies its own logger.
 */

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface Logger {
  debug(message: string): void;
  info(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}

const ORDER: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };

export function consoleLogger(level: LogLevel = 'warn'): Logger {
  const enabled = (at: LogLevel) => ORDER[at] >= ORDER[level];
  const write = (at: LogLevel, message: string) => {
    if (!enabled(at)) return;
    const line = `[flezha] ${message}`;
    try {
      if (at === 'error') console.error(line);
      else if (at === 'warn') console.warn(line);
      else console.log(line);
    } catch {
      // A logger must never take the process down. The case that reaches here
      // is a closed standard stream during shutdown — the harness closes the
      // runner's pipes to stop it — where writing raises EPIPE and an
      // unhandled stream error would turn a clean drain into a crash.
    }
  };
  return {
    debug: (m) => write('debug', m),
    info: (m) => write('info', m),
    warn: (m) => write('warn', m),
    error: (m) => write('error', m),
  };
}

let active: Logger = consoleLogger(
  (process.env.FLEZHA_LOG_LEVEL as LogLevel | undefined) ?? 'warn',
);

export function setLogger(logger: Logger): void {
  active = logger;
}

export function log(): Logger {
  return active;
}

/** The native side's levels, as the addon's log callback reports them. */
export function levelFromNative(level: number): LogLevel {
  if (level >= 3) return 'error';
  if (level === 2) return 'warn';
  if (level === 1) return 'info';
  return 'debug';
}
