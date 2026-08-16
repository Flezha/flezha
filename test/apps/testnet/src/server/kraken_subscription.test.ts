import assert from 'node:assert/strict';
import { test } from 'node:test';

import { ackFrames, parseSubscriptionRequest } from './kraken_subscription';

// fails when: the frame the WS subscription source actually sends stops
// decoding — the testnet would then generate nothing and every dev journey
// reads as a dead pipeline.
test('decodes the subscribe frame the WS source sends', () => {
  const request = parseSubscriptionRequest(
    '{"method":"subscribe","params":{"channel":"trade","symbol":["BTC/DEMO1","ETH/DEMO1"]},"req_id":7}'
  );
  assert.deepEqual(request, {
    method: 'subscribe',
    channel: 'trade',
    symbols: ['BTC/DEMO1', 'ETH/DEMO1'],
    reqId: 7,
  });
});

// fails when: extra params (book depth) break decoding, or unsubscribe stops
// being recognised and a removed symbol keeps streaming forever.
test('decodes unsubscribe and ignores unknown params', () => {
  const request = parseSubscriptionRequest(
    '{"method":"unsubscribe","params":{"channel":"book","symbol":["BTC/DEMO1"],"depth":10},"req_id":9}'
  );
  assert.deepEqual(request, {
    method: 'unsubscribe',
    channel: 'book',
    symbols: ['BTC/DEMO1'],
    reqId: 9,
  });
});

// fails when: the connect-time re-assert (req_id 0) stops decoding — a
// reconnecting source would never re-establish its applied set.
test('accepts the re-assert req_id of 0', () => {
  const request = parseSubscriptionRequest(
    '{"method":"subscribe","params":{"channel":"trade","symbol":["BTC/DEMO1"]},"req_id":0}'
  );
  assert.equal(request?.reqId, 0);
});

// fails when: a data frame or a legacy payload is mistaken for a subscription
// and the legacy generator path stops being reachable.
test('non-subscription frames decode to null', () => {
  assert.equal(parseSubscriptionRequest('{"channel":"trade","type":"update","data":[]}'), null);
  assert.equal(parseSubscriptionRequest('{"event_type":"subscribe","event_data":{}}'), null);
  assert.equal(parseSubscriptionRequest('not json'), null);
  assert.equal(parseSubscriptionRequest('{"method":"subscribe","params":{}}'), null);
});

// fails when: the ack drops its req_id or its success flag — the source
// correlates on exactly those two and would never commit the delta, leaving
// the socket re-sending the same subscribe forever.
test('acks one frame per symbol, echoing req_id', () => {
  const frames = ackFrames(
    { method: 'subscribe', channel: 'trade', symbols: ['BTC/DEMO1', 'ETH/DEMO1'], reqId: 7 },
    '2026-08-16T00:00:00.000Z'
  ).map((f) => JSON.parse(f));

  assert.equal(frames.length, 2);
  assert.deepEqual(frames[0], {
    method: 'subscribe',
    req_id: 7,
    result: { channel: 'trade', symbol: 'BTC/DEMO1' },
    success: true,
    time_in: '2026-08-16T00:00:00.000Z',
    time_out: '2026-08-16T00:00:00.000Z',
  });
  assert.equal(frames[1].result.symbol, 'ETH/DEMO1');
});
