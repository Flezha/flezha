// The builders the dispatch-shaped suites share: what one operative's dispatch
// depends on, and the activation the core hands out.
import { encodeValue } from '../dist/esm/index.js';
import { Registry } from '../dist/esm/dispatch.js';

/** The dispatch dependencies over `specs`, registered in the order given. */
export function depsFor(...specs) {
  const registry = new Registry();
  for (const spec of specs) registry.add(spec);
  return { registry, bridge: { spawnWorkflow: async () => 'spawned' }, workflowCtx: () => ({}) };
}

/** An activation builder bound to one factory; `overrides` names what varies. */
export const activationFor =
  (factory) =>
  (overrides = {}) => ({
    token: 1n,
    operative: 'op-1',
    procedure: 0,
    entityKey: 'k',
    changeSeq: 0n,
    factory,
    kind: 'work',
    ...overrides,
  });

/** One input item of a work batch. */
export const inputItem = (value, overrides = {}) => ({
  kind: 'input',
  channel: 0,
  payload: Buffer.from(encodeValue(value)),
  metadata: [],
  replay: false,
  now: 1_000n,
  ...overrides,
});
