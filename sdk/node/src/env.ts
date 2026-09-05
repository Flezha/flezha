/**
 * The one parse rule per knob type, shared with every other Flezha runtime.
 *
 * A knob's name, type and default are declared once in `Flezha.Config.Keys`;
 * what this module owns is the rule that declaration promises, so a Node runner
 * answers `"0"`, `"on"` or a typo exactly as the controller does.
 * `test/fixtures/knob_parse_cases.txt` is that rule written down.
 */

/** A count of one or more, else `undefined` — `"0"` already means unset. */
export function parsePositive(raw: string | undefined): number | undefined {
  if (raw === undefined) return undefined;
  const trimmed = raw.trim();
  if (!/^\d+$/.test(trimmed)) return undefined;
  const value = Number(trimmed);
  return value > 0 ? value : undefined;
}

/** A whole number of milliseconds, zero included. */
export function parseMs(raw: string | undefined): number | undefined {
  if (raw === undefined) return undefined;
  const trimmed = raw.trim();
  return /^\d+$/.test(trimmed) ? Number(trimmed) : undefined;
}

/** A whole number, negative included. */
export function parseInt10(raw: string | undefined): number | undefined {
  if (raw === undefined) return undefined;
  const trimmed = raw.trim();
  return /^-?\d+$/.test(trimmed) ? Number(trimmed) : undefined;
}

/** `1/true/yes/on` and `0/false/no/off`, case-insensitively. */
export function parseBool(raw: string | undefined): boolean | undefined {
  if (raw === undefined) return undefined;
  switch (raw.trim().toLowerCase()) {
    case '1':
    case 'true':
    case 'yes':
    case 'on':
      return true;
    case '0':
    case 'false':
    case 'no':
    case 'off':
      return false;
    default:
      return undefined;
  }
}

/** A count from the environment, with `fallback` for anything that is not one. */
export function envPositive(name: string, fallback: number): number {
  return parsePositive(process.env[name]) ?? fallback;
}

/** A flag from the environment, with `fallback` for anything that is not one. */
export function envBool(name: string, fallback: boolean): boolean {
  return parseBool(process.env[name]) ?? fallback;
}
