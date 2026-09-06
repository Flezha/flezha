import assert from 'node:assert/strict';
import { test } from 'node:test';

import { formatOutputFrame } from './output_frame';

test('an envelope renders the payload and its metadata', () => {
  const frame = JSON.stringify({
    message: '{"price":42000}',
    encoding: 'utf8',
    metadata: { channel: 'trades', 'flz.entity': 'order-7', 'flz.seq': 41 },
  });
  assert.equal(
    formatOutputFrame(frame),
    'out < {"price":42000}  [channel=trades flz.entity=order-7 flz.seq=41]'
  );
});

test('an envelope with no metadata renders the payload alone', () => {
  assert.equal(formatOutputFrame(JSON.stringify({ message: 'hi' })), 'out < hi');
  assert.equal(
    formatOutputFrame(JSON.stringify({ message: 'hi', metadata: {} })),
    'out < hi'
  );
});

test('encoding defaults to utf8 when the sink omits it', () => {
  assert.equal(formatOutputFrame('{"message":"plain"}'), 'out < plain');
});

test('a base64 payload is decoded when it is text', () => {
  const frame = JSON.stringify({
    message: Buffer.from('{"a":1}').toString('base64'),
    encoding: 'base64',
  });
  assert.equal(formatOutputFrame(frame), 'out < {"a":1}');
});

test('a base64 payload that is not text reports its size instead', () => {
  const frame = JSON.stringify({
    message: Buffer.from([0x82, 0x00, 0x01, 0xff, 0x7f]).toString('base64'),
    encoding: 'base64',
  });
  assert.equal(formatOutputFrame(frame), 'out < <5 bytes>');
});

test('a bare payload passes through, whatever shape it has', () => {
  assert.equal(formatOutputFrame('just text'), 'out < just text');
  // Valid JSON, but not an envelope: no `message` string.
  assert.equal(formatOutputFrame('{"price":42000}'), 'out < {"price":42000}');
  assert.equal(formatOutputFrame('[1,2,3]'), 'out < [1,2,3]');
  assert.equal(formatOutputFrame('null'), 'out < null');
  assert.equal(formatOutputFrame('{"message":42}'), 'out < {"message":42}');
});

test('a metadata array is not treated as metadata', () => {
  assert.equal(
    formatOutputFrame(JSON.stringify({ message: 'x', metadata: ['a'] })),
    'out < x'
  );
});

test('a non-string metadata value keeps its JSON spelling', () => {
  assert.equal(
    formatOutputFrame(JSON.stringify({ message: 'x', metadata: { n: 1, ok: true, o: { a: 1 } } })),
    'out < x  [n=1 ok=true o={"a":1}]'
  );
});
