// The addon's object shapes are declared twice — as the veneer's key table
// and as interfaces in `src/native.ts` — because generating the second from
// the first would mean adopting a napi-rs-specific build, which the binding
// deliberately does not.
//
// So the two are pinned against each other here, the way
// `test/fixtures/host_class_decl_keys.txt` pins the declaration keys across
// SDKs: a field the veneer gains that TypeScript cannot spell would otherwise
// arrive as `undefined` at runtime, on a path no unit test reaches.
//
// The activation and result records come from `veneer/shape.rs`'s `record`
// tables, which are what the veneer actually writes and reads; the host's own
// objects are still `#[napi(object)]` structs and are parsed as such.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const shape = readFileSync(resolve(here, '../../../flz_core_ffi/src/veneer/shape.rs'), 'utf8');
const rust = readFileSync(resolve(here, '../../../flz_core_ffi/src/napi_glue.rs'), 'utf8');
const host = readFileSync(resolve(here, '../../../flz_core_ffi/src/napi_host/mod.rs'), 'utf8');
const typescript = readFileSync(resolve(here, '../src/native.ts'), 'utf8');

const camel = (snake) => snake.replace(/_([a-z])/g, (_m, c) => c.toUpperCase());

/**
 * The camelCase spelling of every key `shape.rs` names, by constant. The
 * `Key::of` arm carries both spellings; `Key::flat` carries one.
 */
function keySpellings() {
  const spellings = new Map();
  for (const [, name, camel] of shape.matchAll(
    /^pub const (\w+): Key = Key::of\("[\w]+", "(\w+)"\);/gm,
  )) {
    spellings.set(name, camel);
  }
  for (const [, name, flat] of shape.matchAll(/^pub const (\w+): Key = Key::flat\("(\w+)"\);/gm)) {
    spellings.set(name, flat);
  }
  return spellings;
}

/** Every `record::` table in `shape.rs`, as the camelCase fields it names. */
function shapeRecords() {
  const spellings = keySpellings();
  const records = new Map();
  const at = shape.indexOf('pub mod record {');
  assert.notEqual(at, -1, 'shape.rs declares no `record` module');
  const body = shape.slice(at, shape.indexOf('\n}', at));
  for (const [, name, fields] of body.matchAll(/pub const (\w+): &\[Key\] = &\[([\s\S]*?)\];/g)) {
    const named = fields
      .split(',')
      .map((field) => field.trim().replace(/^super::/, ''))
      .filter(Boolean);
    records.set(
      name,
      named.map((constant) => {
        const camel = spellings.get(constant);
        assert.ok(camel, `shape.rs names ${constant} in a record but declares no such key`);
        return camel;
      }),
    );
  }
  return records;
}

/** Every `#[napi(object)]` struct in the veneer, with its field names. */
function napiObjects(source) {
  const objects = new Map();
  const pattern = /#\[napi\(object\)\]\s*pub struct (\w+) \{([\s\S]*?)\n\}/g;
  for (const [, name, body] of source.matchAll(pattern)) {
    const fields = [...body.matchAll(/^\s*pub (\w+):/gm)].map(([, field]) => camel(field));
    objects.set(name, fields);
  }
  return objects;
}

/** The declared members of one TypeScript interface. */
function members(name) {
  const at = typescript.indexOf(`export interface ${name} {`);
  assert.notEqual(at, -1, `src/native.ts declares no interface ${name}`);
  const body = typescript.slice(at, typescript.indexOf('\n}', at));
  return new Set([...body.matchAll(/^\s{2}(\w+)\??:/gm)].map(([, member]) => member));
}

/** Each shape record and the interface that mirrors it. */
const RECORDS = {
  META: 'NativeMeta',
  WORK_ITEM: 'NativeWorkItem',
  RESTORE_BODY: 'NativeRestoreBody',
  KEY_REQUEST: 'NativeKeyRequest',
  ACTIVATION: 'NativeActivation',
  OUTPUT: 'NativeOutput',
  TIMER_OP: 'NativeTimerOp',
  DLQ_ENTRY: 'NativeDlqEntry',
  ITEM_EFFECT: 'NativeItemEffect',
  PREPARE_ANSWER: 'NativePrepareAnswer',
  RESULT: 'NativeActivationResult',
};

/** Each host struct the veneer still declares as an object, and its mirror. */
const MIRRORS = {
  JsNativeExport: 'NativeExport',
  JsHostStats: 'NativeHostStats',
  JsLogLine: 'NativeLogLine',
};

/** Every mirrored shape, whichever side declares it. */
function declaredFields() {
  const declared = new Map([...napiObjects(rust), ...napiObjects(host)]);
  const named = new Map();
  for (const [struct, mirror] of Object.entries(MIRRORS)) {
    named.set(mirror, declared.get(struct) ?? []);
  }
  const records = shapeRecords();
  for (const [record, mirror] of Object.entries(RECORDS)) {
    assert.ok(records.has(record), `shape.rs declares no record::${record}`);
    named.set(mirror, records.get(record));
  }
  return named;
}

test('every object the veneer exports has a TypeScript mirror', () => {
  const structs = new Map([...napiObjects(rust), ...napiObjects(host)]);
  const unmirrored = [...structs.keys()].filter((name) => !MIRRORS[name]);
  assert.deepEqual(
    unmirrored,
    [],
    `these #[napi(object)] structs have no entry in MIRRORS: ${unmirrored.join(', ')}`,
  );
  for (const name of Object.keys(MIRRORS)) {
    assert.ok(structs.has(name), `MIRRORS names ${name}, which the veneer no longer declares`);
  }
  const records = shapeRecords();
  const unnamed = [...records.keys()].filter((name) => !RECORDS[name]);
  assert.deepEqual(
    unnamed,
    [],
    `these shape records have no entry in RECORDS: ${unnamed.join(', ')}`,
  );
});

test('every field of every mirrored object is spelled in TypeScript', () => {
  for (const [mirror, fields] of declaredFields()) {
    assert.ok(fields.length > 0, `${mirror} parsed as having no fields`);
    const spelled = members(mirror);
    const missing = fields.filter((field) => !spelled.has(field));
    assert.deepEqual(
      missing,
      [],
      `${mirror} does not spell ${missing.join(', ')}, which the veneer carries`,
    );
  }
});

test('TypeScript invents no field the veneer never carries', () => {
  for (const [mirror, fields] of declaredFields()) {
    const carried = new Set(fields);
    const invented = [...members(mirror)].filter((member) => !carried.has(member));
    assert.deepEqual(
      invented,
      [],
      `${mirror} declares ${invented.join(', ')}, which the veneer does not carry`,
    );
  }
});
