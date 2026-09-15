// The builders the dispatch-shaped suites share: what one operative's dispatch
// depends on, and the activation the core hands out.
import { validate } from '../dist/esm/index.js';
import { Registry } from '../dist/esm/dispatch.js';

/**
 * The dispatch dependencies over `specs`, registered in the order given.
 *
 * Submitted first, exactly as a runner registers: the consumer edges a
 * dispatch routes on are what the core derived, so a spec the core has not
 * judged has none.
 */
export function depsFor(...specs) {
  const registry = new Registry();
  for (const spec of specs) {
    validate(spec);
    registry.add(spec);
  }
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

/** One input item of a work batch, as the JSON an entry naming no codec reads. */
export const inputItem = (value, overrides = {}) => ({
  kind: 'input',
  channel: 0,
  payload: Buffer.from(JSON.stringify(value)),
  metadata: [],
  replay: false,
  now: 1_000n,
  ...overrides,
});

/** A plugin frame read back as the JSON a sink naming no codec sends. */
export const jsonOf = (bytes) => JSON.parse(Buffer.from(bytes).toString('utf8'));
