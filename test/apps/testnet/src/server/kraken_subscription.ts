/**
 * Kraken WebSocket **v2** subscription frames, as the `operative_websocket`
 * subscription source sends them:
 *
 *   {"method":"subscribe","params":{"channel":"trade","symbol":["BTC/USD"]},"req_id":7}
 *
 * The source commits a delta into its applied set only when an inbound frame
 * echoes the `req_id` it sent and does not report failure, so the ack shape
 * here is load-bearing: drop the `req_id` and the reconcile loop never
 * commits, leaving the socket permanently re-sending the same delta.
 */

/** A decoded `subscribe` / `unsubscribe` request. */
export interface SubscriptionRequest {
  method: 'subscribe' | 'unsubscribe';
  channel: string;
  symbols: string[];
  reqId: number;
}

/**
 * Decode a Kraken-v2 subscription frame, or `null` for anything else (a
 * legacy `event_type` payload, a ping, malformed JSON). Unknown fields are
 * ignored — `depth` and friends are the caller's business, not the router's.
 */
export function parseSubscriptionRequest(message: string): SubscriptionRequest | null {
  let decoded: unknown;
  try {
    decoded = JSON.parse(message);
  } catch {
    return null;
  }
  if (typeof decoded !== 'object' || decoded === null) return null;

  const frame = decoded as Record<string, unknown>;
  const method = frame.method;
  if (method !== 'subscribe' && method !== 'unsubscribe') return null;

  const params = (frame.params ?? {}) as Record<string, unknown>;
  const channel = params.channel;
  if (typeof channel !== 'string' || channel.length === 0) return null;

  // Kraken accepts a bare string as well as an array; both spellings reach us.
  const rawSymbols = params.symbol;
  const symbols = (Array.isArray(rawSymbols) ? rawSymbols : [rawSymbols]).filter(
    (s): s is string => typeof s === 'string' && s.length > 0
  );

  const reqId = typeof frame.req_id === 'number' ? frame.req_id : 0;

  return { method, channel, symbols, reqId };
}

/**
 * Kraken answers one ack per symbol, each echoing the request's `req_id`.
 * `nowIso` is injected so a test can assert the whole frame.
 */
export function ackFrames(request: SubscriptionRequest, nowIso: string): string[] {
  return request.symbols.map((symbol) =>
    JSON.stringify({
      method: request.method,
      req_id: request.reqId,
      result: { channel: request.channel, symbol },
      success: true,
      time_in: nowIso,
      time_out: nowIso,
    })
  );
}
