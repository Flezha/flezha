# @flezha/sdk

A poll host over `flz_core`, the runner kernel the Rust and Java SDKs also
drive. The kernel owns transport, ordering, per-entity exclusivity, durability,
retries, timers, credit, routing and the workflow driver. This package supplies
threads and user code, and nothing else.

```bash
npm install @flezha/sdk
```

TypeScript-first, usable from plain JavaScript with no build step, and importable
from both module systems: `import` from an ECMAScript module and `require` from
CommonJS resolve to the same surface.

## Supported Node versions

Node **20 or newer**. The binding targets N-API version 9, which every release in
that window speaks. N-API is ABI-stable across Node versions, so one binary per
platform serves every supported release and upgrading Node needs no rebuild.

## The native binary

The binary is not in this package. It ships as a per-platform package resolved
through `optionalDependencies`, so installing fetches one binary rather than
five and needs no Rust toolchain:

| Platform            | Package                       |
| ------------------- | ----------------------------- |
| Linux x64 (glibc)   | `@flezha/sdk-linux-x64-gnu`   |
| Linux arm64 (glibc) | `@flezha/sdk-linux-arm64-gnu` |
| macOS x64           | `@flezha/sdk-darwin-x64`      |
| macOS arm64         | `@flezha/sdk-darwin-arm64`    |
| Windows x64         | `@flezha/sdk-win32-x64-msvc`  |

On any other platform, or where npm resolved no platform package, the SDK
refuses at the first load naming the host it detected, the package it looked for
and how to build the binary. It does not fall through to a loader error, and it
has no bundled binary to fall back on: a source build is a checkout of the
repository with a stable Rust toolchain.

```bash
cargo build -p flz_core_ffi --features napi --release
node sdk/node/scripts/install-native.mjs
```

`FLEZHA_NATIVE_BINARY` points the loader at an already-built addon from
anywhere, and is consulted before the platform package.

One caution about installs: npm treats an optional dependency it cannot resolve
as a non-event, so an install that fetched no binary succeeds and prints
nothing. That is why the refusal at load is explicit about what is missing.

## Writing an operative

```ts
import { createRunner, defineOperative, defineProcedure } from '@flezha/sdk';

interface Trade {
  symbol: string;
  price: number;
}

const Ohlc = defineProcedure({
  name: 'ohlc',
  initialState: () => ({ close: 0 }),
  applyChange: (state, change: { close: number }) => {
    state.close = change.close;
  },
  handle: (trade: Trade, ctx) => {
    ctx.change({ close: trade.price });
    return { symbol: trade.symbol, close: ctx.state.close };
  },
});

export default defineOperative({
  factoryId: 'candles',
  channels: (c) => {
    c.input<Trade>('trades')
      .keyBy((trade) => trade.symbol)
      .into(Ohlc)
      .keyed()
      .sink('candles');
  },
});
```

```ts
await createRunner('default').defineStream('./candles.js').run();
```

State is mutated only through declared changes: declared state is rebuilt from
its recorded changes on restart, so assigning to a field of `ctx.state` throws
rather than being silently lost. A mismatched `.into(...)` is a compile error.

## Licence

Apache-2.0. See the repository for the full text and third-party notices.
