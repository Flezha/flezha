#!/usr/bin/env node
// The negative control for the compile-time refusals.
//
// `@ts-expect-error` already fails the build when a refusal stops working —
// the directive goes unused and `tsc` says so. What it cannot tell you is
// whether the line failed for the reason the fixture meant: a typo compiles
// just as badly as a wrong edge type. So this strips every directive and
// requires each of those lines to produce a diagnostic, printing them for
// review when they change.

import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const fixtures = resolve(root, 'types-tests');
const tsc = resolve(root, 'node_modules/.bin/tsc');
const DIRECTIVE = '@ts-expect-error';

let expected = 0;
const scratch = mkdtempSync(join(tmpdir(), 'flezha-expected-failures-'));
try {
  for (const name of readdirSync(fixtures).filter((file) => file.endsWith('.ts'))) {
    const source = readFileSync(resolve(fixtures, name), 'utf8');
    // Only a `//` directive counts; the prose above mentions the same string.
    const isDirective = (line) => line.trim().startsWith(`// ${DIRECTIVE}`);
    const stripped = source
      .split('\n')
      .map((line) => (isDirective(line) ? '' : line))
      .join('\n')
      // The fixtures import through the source tree; from the scratch copy
      // that is one directory further up.
      .replace("from '../src/index.js'", `from '${resolve(root, 'src/index.js')}'`);
    expected += source.split('\n').filter(isDirective).length;
    writeFileSync(resolve(scratch, name), stripped);
  }

  let diagnostics = '';
  try {
    execFileSync(
      tsc,
      ['--noEmit', '--strict', '--target', 'ES2022', '--module', 'ESNext',
       '--moduleResolution', 'bundler', '--skipLibCheck',
       ...readdirSync(scratch).map((name) => resolve(scratch, name))],
      { cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
    );
  } catch (failure) {
    diagnostics = `${failure.stdout ?? ''}${failure.stderr ?? ''}`;
  }

  const errors = diagnostics.split('\n').filter((line) => / error TS\d+:/.test(line));
  if (errors.length < expected) {
    console.error(
      `expected at least ${expected} compile errors with the ${DIRECTIVE} directives ` +
        `stripped, saw ${errors.length}. A pinned refusal is not refusing.\n${diagnostics}`,
    );
    process.exit(1);
  }
  console.log(`${expected} pinned refusals, ${errors.length} diagnostics without the directives`);
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
