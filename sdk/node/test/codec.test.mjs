// The JavaScript type mapping, which is specified rather than inherited.
import assert from 'node:assert/strict';
import test from 'node:test';

import {
  assertRepresentable,
  decodeValue,
  encodeValue,
  UnrepresentableValueError,
} from '../dist/esm/index.js';

const roundTrip = (value) => decodeValue(encodeValue(value));

test('an unsigned 64-bit offset survives above the safe integer range', () => {
  const past2p53 = 9_007_199_254_740_993n; // 2^53 + 1
  assert.equal(roundTrip(past2p53), past2p53);
  assert.equal(roundTrip({ offset: past2p53 }).offset, past2p53);
});

test('a value above 2^63 survives, which no int64 encoding could carry', () => {
  const past2p63 = (1n << 64n) - 1n;
  assert.equal(roundTrip(past2p63), past2p63);
  const enormous = 1n << 200n;
  assert.equal(roundTrip(enormous), enormous);
  assert.equal(roundTrip(-past2p63), -past2p63);
});

test('a number stays a number and a bigint stays a bigint', () => {
  assert.equal(typeof roundTrip(42), 'number');
  assert.equal(typeof roundTrip(42n), 'bigint');
  assert.equal(roundTrip(1.5), 1.5);
});

test('a Date rides the timestamp extension', () => {
  const when = new Date('2026-08-26T12:34:56.789Z');
  const back = roundTrip(when);
  assert.ok(back instanceof Date);
  assert.equal(back.getTime(), when.getTime());
});

test('Uint8Array and Buffer both decode to one representation', () => {
  const fromTyped = roundTrip(new Uint8Array([1, 2, 3]));
  const fromBuffer = roundTrip(Buffer.from([1, 2, 3]));
  assert.ok(fromTyped instanceof Uint8Array);
  assert.ok(fromBuffer instanceof Uint8Array);
  assert.deepEqual([...fromTyped], [1, 2, 3]);
  assert.deepEqual([...fromBuffer], [...fromTyped]);
});

test('a byte array decoded from a Buffer is a plain Uint8Array', () => {
  // A `Buffer` subarray is a `Buffer`, and `Buffer` carries a `toJSON` that
  // rewrites it as `{type:'Buffer',data:[…]}` in a live read or a migration
  // handover. Decoding normalises, so a handler never sees two shapes.
  const encoded = Buffer.from(encodeValue({ blob: new Uint8Array([1, 2]) }));
  const decoded = decodeValue(encoded);
  assert.equal(decoded.blob.constructor, Uint8Array, 'not a Buffer subclass');
  assert.deepEqual([...decoded.blob], [1, 2]);
  assert.equal(JSON.stringify(decoded.blob), '{"0":1,"1":2}');
});

test('absent and null stay distinguishable across a round trip', () => {
  const back = roundTrip({ absent: undefined, empty: null, set: 1 });
  assert.equal('empty' in back, true);
  assert.equal(back.empty, null);
  assert.equal('absent' in back, false);
  assert.equal(back.absent, undefined);
});

test('an unrepresentable field is refused naming the type and the field', () => {
  assert.throws(
    () => assertRepresentable('Cart', { total: 1, onCheckout: () => {} }),
    (error) =>
      error instanceof UnrepresentableValueError &&
      error.message.includes('Cart.onCheckout') &&
      error.message.includes('function'),
  );
});

test('a nested unrepresentable field names its path', () => {
  assert.throws(
    () => assertRepresentable('Order', { lines: [{ sku: 'a', tag: Symbol('x') }] }),
    (error) => error.message.includes('Order.lines[0].tag'),
  );
});

test('a cycle is refused rather than encoded', () => {
  const looped = { name: 'a' };
  looped.self = looped;
  assert.throws(
    () => assertRepresentable('Node', looped),
    (e) => e.message.includes('itself'),
  );
});

test('an undefined array element is refused, since a slot cannot be omitted', () => {
  assert.throws(
    () => assertRepresentable('Window', { samples: [1, undefined, 3] }),
    (error) => error.message.includes('Window.samples[1]') && error.message.includes('null'),
  );
});

test('a class instance is refused rather than silently flattened', () => {
  class Money {
    constructor(cents) {
      this.cents = cents;
    }
  }
  assert.throws(
    () => assertRepresentable('Invoice', { due: new Money(5) }),
    (error) => error.message.includes('Invoice.due') && error.message.includes('Money'),
  );
});

test('a Date and a byte array are representable', () => {
  assertRepresentable('Ok', { at: new Date(0), bytes: new Uint8Array([1]), n: 1n });
});
