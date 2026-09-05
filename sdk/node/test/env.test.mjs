import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import {
  parsePositive,
  parseMs,
  parseInt10,
  parseBool,
  envPositive,
  envBool,
} from '../dist/esm/env.js';

const FIXTURE = fileURLToPath(
  new URL('../../../test/fixtures/knob_parse_cases.txt', import.meta.url),
);

function cases() {
  return readFileSync(FIXTURE, 'utf8')
    .split('\n')
    .filter((line) => line.trim() !== '' && !line.startsWith('#'))
    .map((line) => {
      const [kind, rawInput, expected] = line.split('|');
      return { kind, input: rawInput === '<empty>' ? '' : rawInput, expected };
    });
}

const rule = {
  positive_int: parsePositive,
  ms: parseMs,
  int: parseInt10,
  bool: parseBool,
};

test('every case parses the way the shared fixture says', () => {
  const seen = new Set();
  for (const { kind, input, expected } of cases()) {
    seen.add(kind);
    const parse = rule[kind];
    assert.ok(parse, `the fixture names a type this runtime has no rule for: ${kind}`);
    const got = parse(input);
    assert.equal(got === undefined ? 'error' : String(got), expected, `${kind}|${input}`);
  }
  assert.deepEqual([...seen].sort(), ['bool', 'int', 'ms', 'positive_int']);
});

test('an unset variable leaves the default standing', () => {
  const absent = 'FLZ_TEST_ABSENT_VARIABLE';
  assert.equal(envPositive(absent, 7), 7);
  assert.equal(envBool(absent, true), true);
});
