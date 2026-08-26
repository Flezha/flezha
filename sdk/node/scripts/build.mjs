#!/usr/bin/env node
// Build both module systems from one TypeScript source tree.
//
// Two `tsc` runs rather than a bundler: the package's only dependency should
// be the one it actually uses at runtime, and a dual build is two compiler
// invocations plus a `type` marker in each output directory.

import { execFileSync } from 'node:child_process';
import { cpSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const tsc = resolve(root, 'node_modules/.bin/tsc');

const run = (args) =>
  execFileSync(tsc, args, { cwd: root, stdio: 'inherit' });

rmSync(resolve(root, 'dist'), { recursive: true, force: true });

// Types once, from the ESM pass; both outputs share them.
run([
  '--project', 'tsconfig.build.json',
  '--module', 'ESNext',
  '--moduleResolution', 'bundler',
  '--outDir', 'dist/esm',
  '--declaration', '--declarationDir', 'dist/types',
  '--noEmit', 'false',
]);
run([
  '--project', 'tsconfig.build.json',
  '--module', 'CommonJS',
  '--moduleResolution', 'node10',
  '--outDir', 'dist/cjs',
  '--declaration', 'false',
  '--noEmit', 'false',
]);

for (const [format, type] of [['esm', 'module'], ['cjs', 'commonjs']]) {
  const out = resolve(root, 'dist', format);
  mkdirSync(out, { recursive: true });
  writeFileSync(resolve(out, 'package.json'), `${JSON.stringify({ type }, null, 2)}\n`);
  // The one file that cannot be written once: `__dirname` and `import.meta`
  // have no common spelling.
  cpSync(resolve(root, 'runtime', `runtime-shim.${format}.js`), resolve(out, 'runtime-shim.js'));
}

console.log('built dist/esm, dist/cjs and dist/types');
