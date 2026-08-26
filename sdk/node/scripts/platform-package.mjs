#!/usr/bin/env node
// Write the per-platform binary package around an already-built addon.
//
// `@flezha/sdk` resolves its native binary through `optionalDependencies`, so
// each platform is its own tiny package holding one `.node` file. npm installs
// the one that matches, and skips the rest — which is what makes installing
// the SDK fetch one binary rather than five, and need no Rust toolchain.
//
//   node scripts/platform-package.mjs <package-suffix> <addon> <out-dir>
//   node scripts/platform-package.mjs linux-x64-gnu ./flz_core_ffi.node ./out

import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const [suffix, addon, outDir] = process.argv.slice(2);

if (!suffix || !addon || !outDir) {
  console.error('usage: platform-package.mjs <package-suffix> <addon> <out-dir>');
  process.exit(1);
}

/** The `os`/`cpu`/`libc` npm resolves a per-platform package by. */
const PLATFORMS = {
  'linux-x64-gnu': { os: ['linux'], cpu: ['x64'], libc: ['glibc'] },
  'linux-arm64-gnu': { os: ['linux'], cpu: ['arm64'], libc: ['glibc'] },
  'darwin-x64': { os: ['darwin'], cpu: ['x64'] },
  'darwin-arm64': { os: ['darwin'], cpu: ['arm64'] },
  'win32-x64-msvc': { os: ['win32'], cpu: ['x64'] },
};

const platform = PLATFORMS[suffix];
if (!platform) {
  console.error(
    `unknown platform ${suffix}; the published set is ${Object.keys(PLATFORMS).join(', ')}`,
  );
  process.exit(1);
}

const sdk = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8'));
const name = `@flezha/sdk-${suffix}`;
const declared = sdk.optionalDependencies?.[name];
if (declared !== sdk.version) {
  console.error(
    `@flezha/sdk declares ${name}@${declared ?? '(nothing)'}, but this build is ` +
      `${sdk.version}; the optional dependency and the package it resolves must be one version`,
  );
  process.exit(1);
}

const out = resolve(outDir, name.replace('/', '-'));
mkdirSync(out, { recursive: true });
copyFileSync(addon, resolve(out, 'flz_core_ffi.node'));
writeFileSync(
  resolve(out, 'package.json'),
  `${JSON.stringify(
    {
      name,
      version: sdk.version,
      description: `Flezha SDK native binary for ${suffix}`,
      license: sdk.license,
      repository: sdk.repository,
      main: 'flz_core_ffi.node',
      files: ['flz_core_ffi.node'],
      engines: sdk.engines,
      ...platform,
    },
    null,
    2,
  )}\n`,
);
writeFileSync(
  resolve(out, 'README.md'),
  `# ${name}\n\nThe \`${suffix}\` native binary for [\`@flezha/sdk\`](https://www.npmjs.com/package/@flezha/sdk).\n` +
    `Installed automatically as an optional dependency; there is nothing to import from it directly.\n`,
);

console.log(`wrote ${out}`);
