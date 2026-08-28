/**
 * The MessagePack codec for durable `State` and `Change` bodies, and the
 * JavaScript type mapping that goes with it.
 *
 * The mapping is specified here rather than inherited from a library default,
 * because the defaults are wrong for a format that has to survive a restart:
 *
 * | JavaScript      | MessagePack                  | Why not the default |
 * | --------------- | ---------------------------- | ------------------- |
 * | `bigint`        | extension 0, arbitrary width | `useBigInt64` narrows anything past 2^63, and turns every decoded int64 into a `bigint` — so a plain `number` would not survive a round trip either |
 * | `Date`          | timestamp extension (-1)     | a string or a number loses the type |
 * | `Uint8Array` / `Buffer` | `bin`                | both decode back as `Uint8Array`, so a handler reads one representation |
 * | absent property | omitted key                  | encoding it as nil makes it indistinguishable from `null`, and `applyChange` sees a different value on restore |
 *
 * Offsets, sequences and timestamps are `bigint` throughout the SDK for the
 * same reason: they are unsigned 64-bit in the core, and a `number` loses
 * precision above 2^53 — which is inside the range the core uses.
 */

import { decode as msgpackDecode, encode as msgpackEncode, ExtensionCodec } from '@msgpack/msgpack';

/** A hand-written codec, for a type the default cannot carry. */
export interface Codec<T> {
  encode(value: T): Uint8Array;
  decode(bytes: Uint8Array): T;
}

/** Extension type for an arbitrary-width integer. */
const EXT_BIGINT = 0;

function bigIntToBytes(value: bigint): Uint8Array {
  const negative = value < 0n;
  let magnitude = negative ? -value : value;
  const digits: number[] = [];
  while (magnitude > 0n) {
    digits.unshift(Number(magnitude & 0xffn));
    magnitude >>= 8n;
  }
  return Uint8Array.from([negative ? 1 : 0, ...digits]);
}

function bytesToBigInt(bytes: Uint8Array): bigint {
  let magnitude = 0n;
  for (let i = 1; i < bytes.length; i += 1) {
    magnitude = (magnitude << 8n) | BigInt(bytes[i] as number);
  }
  return bytes[0] === 1 ? -magnitude : magnitude;
}

const extensionCodec = new ExtensionCodec();
extensionCodec.register({
  type: EXT_BIGINT,
  encode: (value: unknown) => (typeof value === 'bigint' ? bigIntToBytes(value) : null),
  decode: (data: Uint8Array) => bytesToBigInt(data),
});

/**
 * `ignoreUndefined` is what keeps an absent property distinguishable from a
 * null one: the key is omitted rather than written as nil. `useBigInt64` stays
 * off so every `bigint` goes through the extension above and every `number`
 * comes back a `number`.
 */
export function encodeValue(value: unknown): Uint8Array {
  return msgpackEncode(value, { extensionCodec, ignoreUndefined: true, useBigInt64: false });
}

export function decodeValue<T>(bytes: Uint8Array): T {
  // Decoded through a plain `Uint8Array` view, never the caller's `Buffer`.
  // MessagePack hands out `bin` values as subarrays of whatever it was given,
  // and a subarray of a `Buffer` is a `Buffer` — which carries a `toJSON` that
  // rewrites it as `{type:'Buffer',data:[…]}` the moment the state is
  // serialized for a live read or a migration handover. One representation
  // means one, and this is where it is decided.
  const view =
    bytes.constructor === Uint8Array
      ? bytes
      : new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return msgpackDecode(view, { extensionCodec, useBigInt64: false }) as T;
}

/** The MessagePack codec, as the default for a declared State or Change. */
export const msgpack: Codec<unknown> = {
  encode: encodeValue,
  decode: decodeValue,
};

/**
 * A value the codec cannot represent, named by the declaring type and the
 * path to the offending field.
 */
export class UnrepresentableValueError extends Error {
  constructor(
    readonly typeName: string,
    readonly path: string,
    readonly reason: string,
  ) {
    super(`${typeName}.${path} cannot be encoded for durable state: ${reason}`);
    this.name = 'UnrepresentableValueError';
  }
}

function describe(value: unknown): string | null {
  if (typeof value === 'function') {
    return 'a function has no durable form; hold the data it closes over instead';
  }
  if (typeof value === 'symbol') {
    return 'a symbol has no durable form';
  }
  if (value instanceof Map || value instanceof Set) {
    return (
      `a ${value.constructor.name} decodes as a plain object or array; ` +
      `declare a plain object or array, or supply a hand-written codec`
    );
  }
  if (value !== null && typeof value === 'object') {
    const proto = Object.getPrototypeOf(value);
    const plain = proto === Object.prototype || proto === null || Array.isArray(value);
    const known = value instanceof Date || ArrayBuffer.isView(value);
    if (!plain && !known) {
      return (
        `an instance of ${proto?.constructor?.name ?? 'an unnamed class'} decodes as a ` +
        `plain object, losing its prototype; declare a plain object or supply a ` +
        `hand-written codec`
      );
    }
  }
  return null;
}

/**
 * Walk a declared sample and refuse what the codec cannot carry, naming the
 * type and the field. Run once at registration against the value
 * `initialState()` produces, so an author learns at startup rather than at the
 * first restore that their state does not survive one.
 */
export function assertRepresentable(typeName: string, sample: unknown): void {
  const seen = new WeakSet<object>();

  const walk = (value: unknown, path: string): void => {
    const reason = describe(value);
    if (reason) throw new UnrepresentableValueError(typeName, path || '<root>', reason);

    if (value === null || typeof value !== 'object') return;
    if (value instanceof Date || ArrayBuffer.isView(value)) return;

    if (seen.has(value)) {
      throw new UnrepresentableValueError(
        typeName,
        path || '<root>',
        'the value refers back to itself; MessagePack has no representation for a cycle',
      );
    }
    seen.add(value);

    if (Array.isArray(value)) {
      value.forEach((item, index) => {
        if (item === undefined) {
          // An absent property is representable — the key is omitted. An
          // absent array element is not: the slot must hold something, and nil
          // would decode as `null`, which is a different value.
          throw new UnrepresentableValueError(
            typeName,
            `${path}[${index}]`,
            'an array element is undefined; use null for an empty slot, since ' +
              'an element cannot be omitted the way a property can',
          );
        }
        walk(item, `${path}[${index}]`);
      });
      return;
    }

    for (const [key, item] of Object.entries(value)) {
      walk(item, path ? `${path}.${key}` : key);
    }
  };

  walk(sample, '');
}

/**
 * The JSON form of a state value, for a live read and a migration handover.
 *
 * JSON has no form for two things this codec carries, so both are marked
 * rather than lost: a `bigint` would either overflow or become a plain number,
 * and a byte array would become an object of indices — or, worse, `Buffer`'s
 * own `toJSON` shape, which is why `decodeValue` normalises byte arrays before
 * they can reach here.
 */
export function toStateJson(value: unknown): Buffer {
  const replacer = (_key: string, item: unknown): unknown => {
    if (typeof item === 'bigint') return { $bigint: item.toString() };
    if (item instanceof Uint8Array) return { $bytes: Buffer.from(item).toString('base64') };
    return item;
  };
  return Buffer.from(JSON.stringify(value === undefined ? null : value, replacer) ?? 'null');
}

export function fromStateJson(bytes: Uint8Array): unknown {
  const reviver = (_key: string, item: unknown): unknown => {
    if (item && typeof item === 'object') {
      const record = item as Record<string, unknown>;
      if (typeof record.$bigint === 'string') return BigInt(record.$bigint);
      if (typeof record.$bytes === 'string') {
        return new Uint8Array(Buffer.from(record.$bytes, 'base64'));
      }
    }
    return item;
  };
  return JSON.parse(Buffer.from(bytes).toString('utf8'), reviver);
}
