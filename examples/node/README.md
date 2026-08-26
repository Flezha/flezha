# Flezha Node example

One operative: a per-user session with journaled state, an idle timer and an
emit on close. It exercises the four things every Flezha operative is built
from — a change fold, the deterministic clock, keying declared on the wiring,
and an emit — and it is the same code under both dispatch configurations.

```bash
npm install
npm test          # the handler, with no runner and no controller
npm start         # dispatch on the main event loop
npm run start:workers   # dispatch on four worker threads
```

Running it needs a Flezha controller on the configured transport. The test does
not: a defined handler is an ordinary function, and `ProcedureTestHarness`
supplies the parts a runtime would otherwise own.

See [`docs/sdk/node_sdk.md`](../../docs/sdk/node_sdk.md).
