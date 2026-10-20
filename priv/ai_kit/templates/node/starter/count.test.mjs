import assert from 'node:assert/strict';
import { test } from 'node:test';

import { TaskletTestHarness } from '@flezha/sdk/testing';

import { CountHits } from './count.mjs';

// Runs the tasklet with no controller: one harness per page stands in for that
// page's cell.
test('counts each page on its own, and the count survives a restart', async () => {
  const cells = new Map();
  const totals = [];
  for (const page of ['home', 'home', 'about', 'home']) {
    if (!cells.has(page)) cells.set(page, TaskletTestHarness.of(CountHits, { cellKey: page }));
    const { emits } = await cells.get(page).feed({ page });
    totals.push(emits[0].total);
  }

  assert.deepEqual(totals, [1, 2, 1, 3]);
  // Replaying the recorded changes rebuilds the state each cell had, as a
  // restart does; it throws when they differ.
  for (const cell of cells.values()) cell.replayEquals();
});
