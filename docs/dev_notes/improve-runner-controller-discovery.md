# Runner ↔ controller discovery

Implementation notes for `improve-runner-controller-discovery`. Gotchas only;
the normative text is in `openspec/specs/runner-controller-connection-healing`,
`transport-authentication` and `sdk-conformance`, and the operator-facing
material is `docs/configuration.md` § The socket-directory ladder and
`docs/operations/deployment.md` § Bare metal.

## `System.put_env/2` never reaches the NIF

This is the fact that shaped the Elixir arm, and it is measured, not assumed:

```
FLEZHA_SOCKET_FILES_PATH=/tmp/ladder_probe mix run -e '...'
  os env:        {:ok, "/tmp/ladder_probe", [...]}
  after put_env: {:ok, "/tmp/ladder_probe", [...]}   # put_env("/tmp") ignored
```

OTP keeps its own environment table; `std::env::var` reads the C `environ`
populated at exec. So moving the ladder into `flz_transport` (D1) silently took
`System.put_env/2` away from every Elixir caller — which is how the standalone
SDK's own round-trip test sets the path. `Native.resolve_connect/3` therefore
takes the CALLER's reading of the two variables
(`resolve_socket_dir_with/2`, `quic_addresses_with/1`): the rule stays in one
place, and a host whose runtime has its own environment view still wins. A
standalone runner process is unaffected either way — its environment is real.

## `SUN_LEN` is 108 bytes, and absolutising the directory spends them

Task 1.3 makes every resolved path absolute, which is the whole point (two
processes in two working directories must land on one place). It also lengthens
every socket path, and a UDS path including the file name must fit 108 bytes.
The test configuration was already at the edge:

```
/home/moon/Code/flezha/app_data/sockets/test_agent_node1@127.0.0.1   66
  + /primary_stream_smoke_13573_725555714819.sock                   111  ✗
```

So `config/test.exs` moved the base to `System.tmp_dir!()/flezha_sockets/<node>`
and `NodeTestHelper` derives each peer node's beside it, from that one value.
This also retired `PortPilot`'s `{socket_dir_rel}` template variable and the
`FLEZHA_SOCKET_FILES_PATH=` assignment inside the Elixir test app's command,
which existed only to dodge the same limit — a second writer of the variable
the ladder now owns. Prefer a short runtime directory over a deep checkout;
`docs/configuration.md` says so.

## `FLEZHA_QUIC_ADDRESS` was two variables wearing one name

The controller's LISTEN address (`config/runtime.exs`, `kubernetes/flezha-configmap.yaml`)
and the runner's DIAL address were the same spelling. "SHALL NOT be honoured"
cannot be true while the controller honours it, and a colocated runner would
refuse to start on the controller's own environment — so the listen side is now
`FLEZHA_QUIC_LISTEN_ADDRESS`. Task 8.1's "removal from **both** tables" is what
forces this; it is not incidental scope.

`:socket_files_path` was likewise deleted from `config/runtime.exs` rather than
made to read the variable: leaving it there makes that file a second reader of
what the ladder owns. `Flezha.Socket.Support.socket_dir/0` is the single Elixir
reader — it resolves through the NIF when the key is unset, expands what it
finds, and `PortPilot` and the tests call it rather than `fetch_env!`.

## What "live" means, and what happens when nothing is

`probe/1` calls a candidate live only when a connect SUCCEEDS. `AccessDenied`
is deliberately not live: a directory this user cannot traverse would otherwise
manufacture a phantom ambiguity and refuse a runner that has exactly one
reachable controller.

When no candidate is live the resolver still returns one — the one with the most
to say, socket present > directory present > last rung — so the diagnosis names
the real obstacle instead of the tail of the ladder. Every candidate walked is
carried on `SocketDir.walked` and named either way.

The ambiguity refusal would otherwise fire on a RECONNECT the moment a second
controller appeared, so `first_live` prefers a recorded last-good directory when
it is still live. That is the same requirement the QUIC side states as
"a reconnect returns to the same controller", applied to the UDS directory:
`note_connected/1` records it on a successful control connect, and
`stream_socket_path/2` reads it, so data sockets can never land in a different
directory than the control channel reached.

## `QuicClient::connect` takes a `SocketAddr`

Not a `&str`. It was the last `str::parse::<SocketAddr>()` in the dial path, and
deleting it makes "no DNS, no list, no failover" impossible to reintroduce:
resolution is the caller's, per attempt (`QuicEntry::resolve`), and the caller
flattens a name's several addresses into the candidate set. An entry that fails
to resolve is one failed candidate carried into the error text, never fatal.

`flz_nif`'s `client_connect_quic/2` resolves the single entry it is handed, for
the same reason.

## The gate has to exempt the file that defines the refusal

`flz_transport/src/transport/config.rs` names `FLEZHA_QUIC_ADDRESS` — it is
where the retirement is enforced. Both halves of the gate need to know:
the "unpinned variable" check treats the fixture's `retired` rows as pinned, and
the "no SDK honours a retired name" check skips that one file. Without either,
the gate fails on its own refusal.

Proved by deleting `ladder 3` from `test/fixtures/runner_connect_env.txt`: two
gate tests go red, one naming `["Explicit", "XdgRuntime", "Run", "Repository"]`
against the truncated fixture.

## Two defects the cross-SDK drill found, that nothing else could

Running the ladder scenarios under `FLEZHA_E2E_RUNTIME=elixir` turned up both:

1. **The Elixir SDK reported nothing at all** when it could not reach a
   controller — the reason went into a `{:stop, {:connect_failed, reason}}`
   tuple and no further. It now emits the SAME text a Rust runner logs, through
   `Native.connect_diagnosis/1` → `path_helper::connect_diagnosis`, and calls
   `Logger.flush/0` immediately after: the VM is coming down with that process
   and Logger is async, so without the flush the one line that explains the
   failure is dropped on the way out.
2. **The Elixir test app's stdin watcher treated a read ERROR as EOF** and
   `System.halt(0)`. A read error means the group leader is gone because the VM
   is already shutting down, so halting there raced the shutdown and turned a
   failed boot into a clean exit — the runner reported nothing and exited 0.
   Exactly the trap `docs/dev_notes/node-sdk.md` records for `fd 0`. The watcher
   now returns on an error and halts only on real EOF.

A third, quieter one: the Elixir runner's `_build` copy of the `flezha` dep does
not pick up a rebuilt `flz_nif.so` from an ordinary `mix compile`. A missing NIF
surfaces as `:erlang.nif_error(:not_loaded)` at the call site;
`MIX_ENV=dev mix deps.compile flezha --force` inside `test/apps/elixir_runner` is
what refreshes it.

## A shared default means two controllers can collide

`prepare_socket_dir/0` has always begun with `File.rm_rf/1`. That was harmless
while every controller resolved `app_data/sockets` against its OWN working
directory; with one shared default, a second controller booting on the same host
would wipe the first's live sockets and strand its whole fleet. So
`first_creatable` steps over a candidate whose control socket answers a connect —
the mirror of the runner's refusal, and the reason `Role::Controller` probes at
all. The explicit rung is exempt: it is what the operator named.

## What the refine passes changed

- **One `LastGood<T>`** (`flz_transport/src/utility/last_good.rs`) replaces the
  two copies of "the endpoint that last worked": the UDS socket directory and
  the QUIC address. It owns `note`/`get`/`prefer`, and `prefer` is the whole of
  "last-good first, remainder shuffled" — the shuffle moved in with it, so
  `liveness` exports only the shared `noise()` the jitter already used.
- **One `connect_diagnosis`** in `path_helper`, taking the transport: the QUIC
  candidate summary used to be rendered once in `flz_core::control_unit` and
  again in `Flezha.SDK.Transport` — a parallel mechanism across languages, in a
  change whose whole point is that there is one. Rust, the NIF and
  `flz runner check` now render from the same function.
- **`Flezha.Transport.Native.resolve_connect/1` and `connect_diagnosis/1`**
  supply the BEAM's environment view once, so neither the controller nor the SDK
  spells the two `System.get_env/1` calls.
- **`flz_nif::client_connect_uds` records the directory it reached**, the same
  `note_connected/1` slot the Rust client writes, so an Elixir SDK data socket
  cannot be resolved into a different directory than its control channel.

## The rest, in one line each

- **`cargo test --workspace` is red at HEAD** — `sdk/rust`'s `nonparking_*`
  tests need `--features bench-internals`. Task 9.1's narrower invocation
  (`-p flz_transport -p flz_core -p flz -p flz_nif`) is what is green.
- **Java and Node embed a compiled `flz_core`**, so a ladder change is invisible
  to their e2e until `scripts/build-dist.sh --host` and
  `scripts/build-node-sdk.sh` run. A green run against stale artifacts proves
  the OLD code.
- **`PortPilot.open_port/2` env entries now REPLACE the base entry** of the same
  name (`List.keystore/4`) instead of trailing it. A scenario cannot blank
  `FLEZHA_SOCKET_FILES_PATH` to make a runner walk the ladder if the duplicate's
  winner is up to the OS.
- **The system scenario points `$XDG_RUNTIME_DIR/flezha` at the controller's
  directory with a symlink**; `absolutise` canonicalises, so the runner reports
  and dials the real path. The two-live-candidate scenario binds its decoy at
  the repository rung with `:gen_tcp.listen(0, [{:ifaddr, {:local, path}}])`.
- **The attach line is `Logger.info`** and the primary logger level is
  `:warning` outside prod (`config/prod.exs` sets `:info`), so it is visible in a
  release and not in the dev loop. It follows the convention already at
  `lib/flezha/application.ex:28`; a level change would be a different change.
- **`libc` is a new direct dependency of `flz_transport`**, for one call —
  `getuid` on the access-denied diagnosis. It was already in the graph via
  `mio`, so nothing is added to the build.
- **`cli` now depends on `flz_transport`** so `flz runner check` runs the
  runner's probe rather than a second copy of it.
- **OWED:** `flz reset --include-sockets` still clears `<data root>/sockets` and
  knows nothing about the ladder. Harmless (sockets are runtime state a restart
  replaces) but stale; `docs/operations/reset.md` says so rather than pretending
  otherwise.
- **Not proven here:** a controller whose NAME resolves to a new address. DNS
  cannot be moved under a test without root, so per-attempt resolution is proved
  by unit test and the list arm by the e2e scenario whose first entry does not
  resolve.
