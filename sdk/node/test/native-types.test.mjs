// The addon's object shapes are declared twice — as `#[napi(object)]` structs
// in Rust and as interfaces in `src/native.ts` — because generating the second
// from the first would mean adopting a napi-rs-specific build, which the
// binding deliberately does not.
//
// So the two are pinned against each other here, the way
// `test/fixtures/host_class_decl_keys.txt` pins the declaration keys across
// SDKs: a field the veneer gains that TypeScript cannot spell would otherwise
// arrive as `undefined` at runtime, on a path no unit test reaches.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const rust = readFileSync(resolve(here, '../../../flz_core_ffi/src/napi_glue.rs'), 'utf8');
const host = readFileSync(resolve(here, '../../../flz_core_ffi/src/napi_host.rs'), 'utf8');
const typescript = readFileSync(resolve(here, '../src/native.ts'), 'utf8');

const camel = (snake) => snake.replace(/_([a-z])/g, (_m, c) => c.toUpperCase());

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

/** Each veneer struct and the interface that mirrors it. */
const MIRRORS = {
  JsMeta: 'NativeMeta',
  JsWorkItem: 'NativeWorkItem',
  JsRestoreBody: 'NativeRestoreBody',
  JsKeyRequest: 'NativeKeyRequest',
  JsActivation: 'NativeActivation',
  JsOutput: 'NativeOutput',
  JsTimerOp: 'NativeTimerOp',
  JsDlqEntry: 'NativeDlqEntry',
  JsItemEffect: 'NativeItemEffect',
  JsPrepareAnswer: 'NativePrepareAnswer',
  JsActivationResult: 'NativeActivationResult',
  JsNativeExport: 'NativeExport',
  JsHostStats: 'NativeHostStats',
  JsLogLine: 'NativeLogLine',
};

test('every object the veneer exports has a TypeScript mirror', () => {
  const declared = new Map([...napiObjects(rust), ...napiObjects(host)]);
  const unmirrored = [...declared.keys()].filter((name) => !MIRRORS[name]);
  assert.deepEqual(
    unmirrored,
    [],
    `these #[napi(object)] structs have no entry in MIRRORS: ${unmirrored.join(', ')}`,
  );
  for (const name of Object.keys(MIRRORS)) {
    assert.ok(declared.has(name), `MIRRORS names ${name}, which the veneer no longer declares`);
  }
});

test('every field of every mirrored object is spelled in TypeScript', () => {
  const declared = new Map([...napiObjects(rust), ...napiObjects(host)]);
  for (const [struct, mirror] of Object.entries(MIRRORS)) {
    const fields = declared.get(struct) ?? [];
    assert.ok(fields.length > 0, `${struct} parsed as having no fields`);
    const spelled = members(mirror);
    const missing = fields.filter((field) => !spelled.has(field));
    assert.deepEqual(
      missing,
      [],
      `${mirror} does not spell ${missing.join(', ')}, which ${struct} carries`,
    );
  }
});

test('TypeScript invents no field the veneer never carries', () => {
  const declared = new Map([...napiObjects(rust), ...napiObjects(host)]);
  for (const [struct, mirror] of Object.entries(MIRRORS)) {
    const fields = new Set(declared.get(struct) ?? []);
    const invented = [...members(mirror)].filter((member) => !fields.has(member));
    assert.deepEqual(
      invented,
      [],
      `${mirror} declares ${invented.join(', ')}, which ${struct} does not carry`,
    );
  }
});
