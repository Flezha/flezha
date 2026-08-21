# Elixir SDK + native BEAM transport (`elixir-sdk-native-transport`)

ONE Elixir SDK core, TWO deployment modes. In-node (`:native`) message-passing
and standalone UDS/QUIC binary protocol differ ONLY in a client-transport seam
(`Flezha.SDK.Transport`); everything above it is shared. Zero controller changes
for the standalone mode — the controller cannot tell an Elixir runner from a Rust
one.

## Load-bearing decisions

- **`Flezha.Transport.Stream` is a per-HANDLE dispatch seam**, not per-node-config.
  It pattern-matches `%Flezha.Transport.Local.Stream{}` (native) vs any other term
  (the NIF resource) and delegates the NIF arm to `Flezha.Transport.Native`. The
  standalone client NIFs return the SAME `StreamHandleArc` the server drivers
  produce, so client handles take the NIF arm with zero new Elixir code. Five call
  sites switched; the invariant `no per-stream Native. call outside native.ex and
  the seam` is a grep gate.
- **The control channel frames ONCE, in Rust.** `send_data/2` is deliberately NOT
  in the seam — carrying pre-framed bytes would need an Elixir frame *parser* for
  the native arm (the duplicate codec this change refuses). The three control
  write sites moved to `send_frame(ref, "control_command"|"control_ping", bytes,
  %{})`; the NIF builds the identical frame, so wire bytes are unchanged.
  `Payloader.build_from_bin/3` + `serialize_metadata/1` were DELETED. The test-only
  `write_fun` seam is now arity-4 `(ref, payload_id, message, metadata)`; 8
  control_channel test files updated, `ControlFrameTestHelper.decode_request`
  decodes raw protobuf now (no frame to peel).
- **`Local.Stream` = one process per stream, two sides** (`:controller` + `:sdk`).
  A frame on one side reaches the OTHER side's current owner; frames buffered while
  a side is inactive flush in order on `set_active`; owner death (monitor) or
  `close/1` synthesizes `{:flz_closed, id, reason}` with a **binary** reason
  (matching the NIF). `message` is passed by term identity — never assumed a
  binary (D4).
- **`StreamHandle.waker` became a `StreamWaker` enum** (`Mio(Arc<Waker>)` |
  `Channel`) so ONE handle type serves the mio server drivers AND the
  channel-driven client writer thread — the only touch to the server driver path.
  The client (`flz_nif/src/client.rs`) runs a blocking reader thread (payloader +
  `driver_msg` builders) and a writer thread draining the handle's command channel;
  `controlling_process/2` updates a shared `Arc<Mutex<LocalPid>>` the reader reads,
  so `UpdateOwner`/`SetActive`/`SetPassive` are inert for a client.
- **`:block` is the overflow default for EVERY edge kind**, in lockstep with the
  Rust/Java SDKs and the FFI host (`close-silent-data-loss-defaults`). The Entity's
  block path is a `GenServer.call` the router blocks on until there is room — the
  room case MUST `{:reply, :ok, ...}`, NOT `{:noreply, ...}` (a noreply hangs the
  router forever). A dead Entity releases the caller (the call monitors it).
- **Ctx emits ride the process dictionary.** An immutable struct can't accumulate
  emits through `Ctx.emit(ctx, v)`, so emits are stored under a per-invocation ref
  in the running (Entity) process's pdict; the Entity flushes on `{:ok, _}`,
  discards on error.
- **control_ping is SDK-INITIATED** keepalive; the controller ECHOES it. The SDK
  sends it every 15 s and does NOT re-echo the echo (that loops). `data_heartbeat`
  rides secondaries every 10 s with `dispatches` (completed only — the
  `runner_pool_wedged` verdict's input) + `blocked_parks`.
- **Runner-only boot** gates the WHOLE controller tree: `controller_enabled`
  (`FLEZHA_CONTROLLER_ENABLED`, default true) — false starts an EMPTY
  `Flezha.Supervisor`, skips the state-path migration and the boot-ready gate.
  Parsed as a LOCAL CLOSURE in `config/runtime.exs` (never a project module — the
  cold-checkout rule), same as the `[:uds, :quic, :native]` transport whitelist.
- **Packaging boundary**: `scripts/check-sdk-boundary.sh` (naming.yml) keeps
  `lib/flezha/sdk/` touching the controller only through named seams (Codec →
  CommandsProto; `Wire.encode_park_body/1`; `Partition.group_of/1`;
  `Transport.Server`/`Local.Stream`/`Transport.Stream`; the `flz_nif` client
  entry points). Protox/`RunnerCommands` appear only in `Flezha.SDK.Codec`.

## Gotchas

- **`RUSTLER_SKIP_COMPILATION=true` compiles `native.ex` to STUBS** (no NIF
  `@on_load`). Native-mode tests pass with it (pure-Elixir `Local.Stream`) but
  SOCKET-mode tests call the real client NIFs and get `:not_loaded`. After any skip
  build, `rm _build/<env>/lib/flezha/priv/native/flz_nif.so && mix compile --force`
  to reinstall the real NIF. The FULL suite must run withOUT the skip flag.
- **`function_exported?` does NOT auto-load a module.** `into(module)` stores the
  atom without loading it, so entity-mode derivation saw Static procedures as
  Single until `Procedure.exports?/3` (`Code.ensure_loaded?` first) was used
  everywhere.
- **`elixirc_paths` was narrowed** from `["lib", "test"]` to the explicit test
  dirs (`test/fixtures`, `test/helpers`, `test/support`) so the recursive `test`
  glob no longer pulls `test/apps/elixir_runner/lib/*.ex` into the controller.
- **The spawned `elixir_runner` runs in its own cwd**, so the controller's relative
  socket/cert paths miss it — `build_command(:elixir, ...)` passes them ABSOLUTE.
  A runner node also disables the KafkaEx default worker (it owns no plugins).
- **`SDK.close/1` catches `:exit`** — `GenServer.stop` can race the caller-link
  teardown and see `:shutdown`.
- **Bench**: `mix bench` has no `--transport` flag; transport rides `FLEZHA_TRANSPORT`
  via `scripts/bench.sh -t`. `Mix.Tasks.Bench.Fleet.setup/3` starts ONE in-process
  SDK for `{:elixir, :native}` (the slot is process-wide) and spawns
  `elixir_runner` for `{:elixir, :uds|:quic}`. Small-scale smoke numbers only; no
  CI floor gate for the elixir runtime.

## Test map

- `test/flezha/transport/local/stream_test.exs`, `native_transport_test.exs` — the
  native handle + `:native` `Transport.Server` clauses (remote-node rejection is
  `:multi_node`).
- `test/flezha/sdk/sdk_unit_test.exs` — behaviours, builder refusals (each named
  issue), fusion, entity modes, Ctx, Codec, KeyGroup fixtures, PersistentId.
- `test/flezha/sdk/dispatch_test.exs` — the dispatch runtime (full path, ordering,
  ladder recover/park, drop_newest, partition mis-route).
- `test/flezha/sdk/in_node_e2e_test.exs` — native connect → place → io_in → run/3.
- `test/flezha/sdk/socket_roundtrip_test.exs` — the client NIFs end-to-end (same
  node, UDS).
- `test/flezha/sdk/liveness_reconnect_e2e_test.exs` — no-stall, close→reconnect,
  UpdateFactories + removal-refusal.
- `test/system/elixir_runner_e2e_test.exs` — standalone separate-process e2e (UDS +
  QUIC, `:system`, nightly).
- `flz_nif/src/client.rs` `#[cfg(test)]` — connect retry + preamble constants.
