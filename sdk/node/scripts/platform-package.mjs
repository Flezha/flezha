#!/usr/bin/env node
// Write the per-platform binary packages around already-built addons.
//
// `@flezha/sdk` resolves its native binary through `optionalDependencies`, so
// each platform is its own tiny package holding one `.node` file. npm installs
// the one that matches and skips the rest, which is what makes installing the
// SDK fetch one binary rather than five, and need no Rust toolchain.
//
// The platforms come from the table the loader resolves against, so a platform
// cannot be declared in one and forgotten in the other.
//
//   node scripts/platform-package.mjs <addon-dir> <out-dir> [--require-all]
//
// <addon-dir> holds one directory per platform suffix, each carrying that
// platform's addon as `flz_core_ffi.node`. A platform with no addon is skipped,
// unless `--require-all` is given, which is what a release passes: publishing a
// parent that declares a package the build did not produce is the install that
// fails at first load.

import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
const requireAll = args.includes('--require-all');
const [addonDir, outDir] = args.filter((arg) => !arg.startsWith('--'));

const refuse = (message) => {
  console.error(message);
  process.exit(1);
};

if (!addonDir || !outDir) {
  refuse('usage: platform-package.mjs <addon-dir> <out-dir> [--require-all]');
}

// The loader's own table, read from the built package rather than copied: the
// two readers of a shared table cannot share it if one keeps a transcription.
const built = resolve(root, 'dist/esm/platforms.js');
if (!existsSync(built)) {
  refuse(`${built} does not exist; build the package first:\n  npm run build`);
}
const { NATIVE_PLATFORMS, platformPackageName } = await import(pathToFileURL(built));

const sdk = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8'));
const declared = sdk.optionalDependencies ?? {};
const expected = NATIVE_PLATFORMS.map((platform) => platformPackageName(platform.suffix));

// The declared set and the table are one set, at one version. A name in only
// one of them is a platform package nothing resolves or nothing produces.
for (const name of expected) {
  if (declared[name] !== sdk.version) {
    refuse(
      `${sdk.name} declares ${name}@${declared[name] ?? '(nothing)'}, but this build is ` +
        `${sdk.version}; the optional dependency and the package it resolves are one version`,
    );
  }
}
for (const name of Object.keys(declared)) {
  if (!expected.includes(name)) {
    refuse(`${sdk.name} declares ${name}, which no platform in the table produces`);
  }
}

const missing = [];
const written = [];

for (const platform of NATIVE_PLATFORMS) {
  const addon = resolve(addonDir, platform.suffix, 'flz_core_ffi.node');
  if (!existsSync(addon)) {
    missing.push({ suffix: platform.suffix, addon });
    continue;
  }

  const name = platformPackageName(platform.suffix);
  const out = resolve(outDir, name.replace('/', '-'));
  mkdirSync(out, { recursive: true });
  copyFileSync(addon, resolve(out, 'flz_core_ffi.node'));
  writeFileSync(
    resolve(out, 'package.json'),
    `${JSON.stringify(
      {
        name,
        version: sdk.version,
        description: `Flezha SDK native binary for ${platform.suffix}`,
        license: sdk.license,
        repository: sdk.repository,
        main: 'flz_core_ffi.node',
        files: ['flz_core_ffi.node'],
        engines: sdk.engines,
        os: [platform.os],
        cpu: [platform.cpu],
        ...(platform.libc ? { libc: [platform.libc] } : {}),
      },
      null,
      2,
    )}\n`,
  );
  writeFileSync(
    resolve(out, 'README.md'),
    `# ${name}\n\nThe \`${platform.suffix}\` native binary for [\`${sdk.name}\`](https://www.npmjs.com/package/${sdk.name}).\n` +
      'Installed automatically as an optional dependency; there is nothing to import from it directly.\n',
  );
  written.push(out);
}

if (missing.length > 0 && requireAll) {
  refuse(
    `${missing.length} of ${NATIVE_PLATFORMS.length} platform packages have no addon:\n  ` +
      `${missing.map((entry) => `${entry.suffix}: ${entry.addon}`).join('\n  ')}\n` +
      `${sdk.name}@${sdk.version} declares all of them, so publishing this build would ` +
      'declare a package that cannot resolve.',
  );
}

for (const out of written) console.log(`wrote ${out}`);
if (missing.length > 0) {
  console.log(`skipped, no addon staged: ${missing.map((entry) => entry.suffix).join(', ')}`);
}
