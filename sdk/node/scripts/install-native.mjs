#!/usr/bin/env node
// Copy the locally built addon to where `loadNative()` looks for a source
// build. This is the fallback path a platform with no prebuilt package takes,
// and the path this repository's own suites use.

import { copyFileSync, existsSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const workspace = resolve(root, '../..');
const profile = process.argv.includes('--debug') ? 'debug' : 'release';

const library = {
  linux: 'libflz_core_ffi.so',
  darwin: 'libflz_core_ffi.dylib',
  win32: 'flz_core_ffi.dll',
}[process.platform];

if (!library) {
  console.error(`no native library name known for ${process.platform}`);
  process.exit(1);
}

const built = resolve(workspace, 'target', profile, library);
if (!existsSync(built)) {
  console.error(
    `${built} does not exist; build it first:\n` +
      `  cargo build -p flz_core_ffi --features napi${profile === 'release' ? ' --release' : ''}`,
  );
  process.exit(1);
}

const out = resolve(root, 'native');
mkdirSync(out, { recursive: true });
copyFileSync(built, resolve(out, 'flz_core_ffi.node'));
console.log(`installed ${profile} addon to native/flz_core_ffi.node`);
