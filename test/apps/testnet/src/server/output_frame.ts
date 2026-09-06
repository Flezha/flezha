/**
 * Frames arriving from a Flezha output sink, rendered for a person watching a
 * manual end-to-end run.
 *
 * The sink may put either shape on the wire: an envelope carrying the payload
 * and its metadata, or the bare payload. A websocket text frame has no header
 * slot, so an envelope is the only way the identity stamps (`flz.entity`,
 * `flz.seq`, `flz.key`) and the channel tag survive the hop, but a sink
 * pointed at a third-party service sends bare. This reads both.
 */

interface OutputEnvelope {
  message: string;
  encoding: 'utf8' | 'base64';
  metadata?: Record<string, unknown>;
}

/** An envelope, or null when the frame is a bare payload. */
function asEnvelope(raw: string): OutputEnvelope | null {
  let decoded: unknown;
  try {
    decoded = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof decoded !== 'object' || decoded === null) return null;

  const frame = decoded as Record<string, unknown>;
  if (typeof frame.message !== 'string') return null;

  const metadata =
    typeof frame.metadata === 'object' && frame.metadata !== null && !Array.isArray(frame.metadata)
      ? (frame.metadata as Record<string, unknown>)
      : undefined;

  return {
    message: frame.message,
    encoding: frame.encoding === 'base64' ? 'base64' : 'utf8',
    metadata,
  };
}

const TAB = 9;
const NEWLINE = 10;
const RETURN = 13;
const FIRST_PRINTABLE = 32;
const REPLACEMENT = 0xfffd;

/**
 * Whether a decoded payload can go to a terminal: no C0 control other than
 * tab, newline and carriage return, and no replacement character, which is
 * what a failed UTF-8 decode leaves behind.
 */
function isText(text: string): boolean {
  for (const character of text) {
    const code = character.codePointAt(0) ?? 0;
    if (code === REPLACEMENT) return false;
    if (code < FIRST_PRINTABLE && code !== TAB && code !== NEWLINE && code !== RETURN) {
      return false;
    }
  }
  return true;
}

/**
 * A base64 payload as text when it is printable, and as a byte count when it
 * is not: a msgpack or protobuf payload is noise on a terminal, but its size
 * still says the output arrived.
 */
function decodePayload(message: string, encoding: 'utf8' | 'base64'): string {
  if (encoding !== 'base64') return message;
  const bytes = Buffer.from(message, 'base64');
  const text = bytes.toString('utf8');
  return isText(text) ? text : `<${bytes.length} bytes>`;
}

/** `key=value` pairs in the order the sink wrote them, or '' when there are none. */
function renderMetadata(metadata: Record<string, unknown> | undefined): string {
  const pairs = Object.entries(metadata ?? {}).map(
    ([key, value]) => `${key}=${typeof value === 'string' ? value : JSON.stringify(value)}`
  );
  return pairs.length === 0 ? '' : `  [${pairs.join(' ')}]`;
}

/** One line per frame: the payload, then whatever metadata rode with it. */
export function formatOutputFrame(raw: string): string {
  const envelope = asEnvelope(raw);
  if (!envelope) return `out < ${raw}`;
  const payload = decodePayload(envelope.message, envelope.encoding);
  return `out < ${payload}${renderMetadata(envelope.metadata)}`;
}
