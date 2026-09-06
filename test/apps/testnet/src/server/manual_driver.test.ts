import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  ManualDriver,
  manualInputEnabled,
  parseCommand,
  parseDuration,
  render,
} from './manual_driver';
import { ConnectedSocket, InputSockets } from './server_contract';
import { FakeTimers } from '../util/fake_timers.test-support';
import { Timers } from '../util/timers';

function driver(peers: ConnectedSocket[] = []): {
  driver: ManualDriver;
  sent: string[];
  printed: string[];
  timers: FakeTimers;
  recipients: { count: number };
} {
  const sent: string[] = [];
  const printed: string[] = [];
  const timers = new FakeTimers();
  const recipients = { count: 1 };
  const sockets: InputSockets = {
    broadcast: (message) => {
      sent.push(message);
      return recipients.count;
    },
    connected: () => peers,
  };
  const instance = new ManualDriver(sockets, (line) => printed.push(line), timers, () => 'TS');
  return { driver: instance, sent, printed, timers, recipients };
}

test('parseDuration reads every suffix and refuses the rest', () => {
  assert.equal(parseDuration('250'), 250);
  assert.equal(parseDuration('250ms'), 250);
  assert.equal(parseDuration('30s'), 30_000);
  assert.equal(parseDuration('2m'), 120_000);
  assert.equal(parseDuration('0'), 0);
  assert.equal(parseDuration('30h'), null);
  assert.equal(parseDuration('-1'), null);
  assert.equal(parseDuration('abc'), null);
});

test('a line that does not start with a slash is a payload, verbatim', () => {
  assert.deepEqual(parseCommand('{"a": 1}'), { kind: 'send', message: '{"a": 1}' });
  // Interior spacing survives, because the payload is taken from the raw line.
  assert.deepEqual(parseCommand('a  b'), { kind: 'send', message: 'a  b' });
  assert.deepEqual(parseCommand('   '), { kind: 'blank' });
});

test('parseCommand reads the verbs', () => {
  assert.deepEqual(parseCommand('/help'), { kind: 'help' });
  assert.deepEqual(parseCommand('/peers'), { kind: 'peers' });
  assert.deepEqual(parseCommand('/stop'), { kind: 'stop' });
  assert.equal(parseCommand('/nope').kind, 'error');
});

test('parseCommand takes the bulk template from after the duration', () => {
  assert.deepEqual(parseCommand('/bulk 4 1s {"n": {{n}}}'), {
    kind: 'bulk',
    count: 4,
    durationMs: 1000,
    template: '{"n": {{n}}}',
  });
  // A template whose leading token repeats the count or the duration.
  assert.deepEqual(parseCommand('/bulk 10 10s 10s later'), {
    kind: 'bulk',
    count: 10,
    durationMs: 10_000,
    template: '10s later',
  });
});

test('parseCommand refuses a malformed bulk', () => {
  assert.match((parseCommand('/bulk 4 1s') as { reason: string }).reason, /usage/);
  assert.match((parseCommand('/bulk 0 1s x') as { reason: string }).reason, /positive integer/);
  assert.match((parseCommand('/bulk x 1s y') as { reason: string }).reason, /positive integer/);
  assert.match((parseCommand('/bulk 4 1h y') as { reason: string }).reason, /duration/);
});

test('render substitutes the index and the timestamp', () => {
  assert.equal(render('{{n}}/{{ts}}/{{n}}', 3, () => 'T'), '3/T/3');
  assert.equal(render('nothing', 1, () => 'T'), 'nothing');
});

test('a plain line broadcasts once and reports the recipients', () => {
  const { driver: d, sent, printed } = driver();
  d.handleLine('hello');
  assert.deepEqual(sent, ['hello']);
  assert.deepEqual(printed, ['sent to 1 socket(s)']);
});

test('bulk spreads the count over the window and stops itself', () => {
  const { driver: d, sent, printed, timers } = driver();
  d.handleLine('/bulk 4 1s msg-{{n}}');
  assert.equal(timers.onlyInterval(), 250);

  timers.tick(4);
  assert.deepEqual(sent, ['msg-1', 'msg-2', 'msg-3', 'msg-4']);
  assert.equal(timers.active.size, 0, 'the timer is cleared on the last send');
  assert.ok(printed.includes('bulk complete: 4 sent'));

  // The cleared timer cannot deliver a fifth.
  timers.tick(2);
  assert.equal(sent.length, 4);
});

test('a window shorter than the count collapses to the 1ms floor', () => {
  const { driver: d, timers } = driver();
  d.handleLine('/bulk 100 10ms x');
  assert.equal(timers.onlyInterval(), 1);
});

test('a second bulk is refused while one runs, and /stop frees it', () => {
  const { driver: d, sent, printed, timers } = driver();
  d.handleLine('/bulk 4 1s first-{{n}}');
  d.handleLine('/bulk 2 1s second-{{n}}');
  assert.ok(printed.includes('a bulk is already running — /stop it first'));
  assert.equal(timers.active.size, 1);

  timers.tick(1);
  d.handleLine('/stop');
  assert.ok(printed.includes('bulk cancelled after 1/4'));
  assert.equal(timers.active.size, 0);

  d.handleLine('/bulk 2 1s second-{{n}}');
  timers.tick(2);
  assert.deepEqual(sent, ['first-1', 'second-1', 'second-2']);
});

test('/stop with nothing running says so', () => {
  const { driver: d, printed } = driver();
  d.handleLine('/stop');
  assert.deepEqual(printed, ['no bulk running']);
});

test('a bulk into an empty room warns once', () => {
  const { driver: d, printed, timers, recipients } = driver();
  recipients.count = 0;
  d.handleLine('/bulk 3 3s x');
  timers.tick(3);
  assert.equal(
    printed.filter((line) => line.startsWith('warning: no sockets')).length,
    1
  );
});

test('/peers numbers the sockets and marks the ones not fed', () => {
  const empty = driver([]);
  empty.driver.handleLine('/peers');
  assert.deepEqual(empty.printed, ['no sockets connected']);

  const two = driver([
    { path: '/in', role: 'input' },
    { path: '/out', role: 'output' },
  ]);
  two.driver.handleLine('/peers');
  assert.deepEqual(two.printed, ['1. /in\n2. /out (output, not fed)']);
});

test('a stopped bulk sends nothing even if its timer fires again', () => {
  // `/stop` is authoritative whatever the timer does, so the driver's own guard
  // has to be what stops the send. This stub deliberately ignores `clear`.
  const sent: string[] = [];
  let fire!: () => void;
  const leaky: Timers = {
    set: (fn) => {
      fire = fn;
      return 1;
    },
    clear: () => undefined,
  };
  const d = new ManualDriver(
    {
      broadcast: (message) => {
        sent.push(message);
        return 1;
      },
      connected: () => [],
    },
    () => undefined,
    leaky
  );

  d.handleLine('/bulk 5 5s x');
  fire();
  assert.deepEqual(sent, ['x']);

  d.handleLine('/stop');
  fire();
  fire();
  assert.deepEqual(sent, ['x'], 'the guard, not the clear, is what stops it');
});

test('manualInputEnabled follows the terminal unless overridden', () => {
  assert.equal(manualInputEnabled(undefined, true), true);
  assert.equal(manualInputEnabled(undefined, false), false);
  assert.equal(manualInputEnabled('', false), false);
  assert.equal(manualInputEnabled('1', false), true);
  assert.equal(manualInputEnabled('0', true), false);
});

test('/help and an unknown verb both print', () => {
  const { driver: d, printed } = driver();
  d.handleLine('/help');
  assert.match(printed[0], /\/bulk <count> <duration> <text>/);
  d.handleLine('/nope');
  assert.match(printed[1], /unknown command "\/nope"/);
});

test('a blank line does nothing at all', () => {
  const { driver: d, sent, printed } = driver();
  d.handleLine('');
  d.handleLine('   ');
  assert.deepEqual(sent, []);
  assert.deepEqual(printed, []);
});
