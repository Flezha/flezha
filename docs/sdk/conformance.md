# The cross-SDK conformance suite

One end-to-end suite, one SDK per run. `FLEZHA_E2E_RUNTIME` selects which SDK's
test app the harness spawns; nothing under `test/system/` names a runtime.

```bash
mix test --include system                              # rust, the default
FLEZHA_E2E_RUNTIME=java mix test --include system      # the whole suite on Java
FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke   # the two-minute inner loop

# A runtime whose dispatch topology is a run-time choice is run under each. The
# Node SDK's is the only one today: the fixtures are identical, the run is not.
FLEZHA_E2E_RUNTIME=node mix test --include system
FLEZHA_E2E_RUNTIME=node FLEZHA_NODE_WORKERS=4 mix test --include system
```

A full cross-SDK check is one run per runtime. `mix test` on its own spawns no
runner and still runs the static gate below.

`FLEZHA_E2E_RUNTIME` steers the conformance suite only. A controller or
integration test outside `test/system/` that merely needs *a* runner keeps the
reference SDK: it is written against the reference fixtures, so pointing a run
at an SDK that carries a subset of them would turn it red for a reason that is
not about that SDK. `FlezhaTest.SystemCase` is what opts a module in.

## The manifest

Each SDK's test app carries `test/apps/<sdk>_runner/conformance.json`:

| Field | What |
| --- | --- |
| `runtime` | The name `FLEZHA_E2E_RUNTIME` selects it by |
| `command` | Launch template per profile, over `{artifact}`, `{app_dir}`, `{repo}`, `{namespace}`, `{socket_dir}`, `{ca_cert}`, `{client_cert}`, `{client_key}` |
| `artifacts` | Ordered candidate paths per profile, relative to the app dir — the first that exists is used, a later one warns, none raises |
| `build` | The one command that produces each profile's artifact |
| `connect_timeout_ms` | How long the harness waits for the runner's handshake |
| `transports` | The subset of `uds`, `quic` the standalone runner supports |
| `dispatch` | How this host executes activations: `model` (one of `thread-pool`, `event-loop`, `process-per-entity`), `detail` (one sentence), and `env` (each environment variable that selects the topology → what it selects; `{}` when fixed) |
| `toolchain` | The shell command that prints this SDK's toolchain version, e.g. `rustc --version` |
| `deferred` | Capabilities this SDK does not carry, by name |
| `fixtures` | Factory ids the app registers, grouped by capability |
| `docs` | The page documenting this SDK's scope, which must point back here |

Two profiles, two spawners, one file: the e2e harness renders `test` and
`mix bench` renders `bench`. Neither carries a per-runtime launch clause —
`Flezha.Service.PortPilot.launch_command/3` is the only renderer.

The Rust manifest is the **reference**: a fixture id exists for a capability if
and only if `test/apps/rust_runner/conformance.json` lists it there.

`dispatch` and `toolchain` exist so a bench header can say what a number was
taken under. A throughput figure from one dispatch topology and one from
another are different measurements of the same fixture, and only the manifest
knows which — so a reporter reads them from here and carries **no per-runtime
clause of its own**, the same rule the launch line already follows. A
per-runtime clause is a list a new SDK has to remember to join; the manifest is
where a runtime already says what it is. The gate fails a manifest that omits
either, or that names a `model` outside the vocabulary.

## Capabilities

The vocabulary is one list in `FlezhaTest.Helper.NodeTestHelper.capabilities/0`:

`runner_lifecycle`, `graph_builder`, `liveness`, `pool_wedge`, `retry_ladder`,
`state_persistence`, `effectively_once`, `entity_timers`, `correction_window`,
`keyed_channels`, `workflow`, `dynamic_entity`, `entry_idle`, `partitioned`.

A capability can carry no fixtures at all — `pool_wedge` and `entry_idle` are
`[]` everywhere. `pool_wedge` is the wedged-dispatch signature the convergence
classifier reads: a host whose dispatch is a shared pool can be wedged by user
code, and a host that runs a process per Entity cannot produce the signature by
construction. It is optional for exactly that reason.

Every module under `test/system/` declares one:
`use FlezhaTest.SystemCase, capability: :<atom>`. A module whose scenarios span
two carries the second per test, with its own skip:
`@tag capability: :effectively_once, skip: FlezhaTest.SystemCase.deferral(:effectively_once)`. Every capability appears in every
manifest exactly once — under `deferred` or as a `fixtures` key — so one can be
neither forgotten nor claimed twice. The **required set**
(`runner_lifecycle`, `graph_builder`, `liveness`, `retry_ladder`,
`partitioned`) can never be deferred.

A deferred capability is **skipped by name**, not filtered out:
`FlezhaTest.SystemCase` sets `:skip` on the module, because `--include system`
is how this suite runs and an ExUnit include overrides an exclude — a filter
would run the very tests the manifest says the SDK does not carry. The
transport axis needs no skip: a test enumerates
`NodeTestHelper.transports/0`, so a transport the manifest omits never becomes
a test. `test/test_helper.exs` prints one line naming both.

A capability that is not deferred must pass — there is no third state.

## Writing a scenario

A module under `test/system/` declares the capability it proves, and gets the
`:system` tag, the deferral skip and the runtime wiring with it:

```elixir
defmodule FlezhaTest.System.MyThingE2ETest do
  use FlezhaTest.SystemCase, capability: :retry_ladder
```

Three overrides exist, each for a scenario the module-level declaration does not
fit. All three are enforced or allowed by the gate, so reach for the one that
says what you mean:

| Tag | For | Effect |
| --- | --- | --- |
| `@tag capability: :x, skip: FlezhaTest.SystemCase.deferral(:x)` | a scenario in this module that proves a *second* capability | runs only where `:x` is carried. The skip is not optional — the gate rejects the tag without it |
| `@tag reference_runtime: true` | a scenario that proves **controller** mechanics *through* a runner — a journal cut, an outbox re-drive, a rewind | always spawns the reference SDK, whatever the run selects. Say why in a comment |
| `@tag interop: true` | a scenario that deliberately places two SDKs at once | the only place a `runtime:` literal is allowed |

`reference_runtime` is the honest home for a scenario that is not about the
runner at all, and for one a named SDK is known not to satisfy yet — the comment
carries the follow-up. It is not a way to make a red run green: a capability an
SDK claims must pass on that SDK.

Drive fixtures by the ids in the reference manifest; the gate fails a file that
uses an id belonging to a capability it did not declare.

## Environment

| Variable | Read by | Meaning |
| --- | --- | --- |
| `FLEZHA_E2E_RUNTIME` | the harness | Which SDK the conformance suite runs against (default `rust`). Unknown values raise, naming the manifests that exist |
| `FLEZHA_E2E_SNAPSHOT=update` | the smoke tier | Rewrite the golden declaration snapshots. Refused unless the runtime is the reference |
| `CONFORMANCE_OUT` | the formatter | Path to write the capability report as JSON |
| `FLEZHA_SOCKET_FILES_PATH`, `FLEZHA_TRANSPORT`, `FLEZHA_CA_CERT_PATH`, `FLEZHA_QUIC_ADDRESS` | every test app | Injected by `PortPilot.open_port/2`; a test app must honour them |
| `FLEZHA_RUNNER_ID_PATH` | every test app | Where the runner persists its stable identity. Two runners in one namespace need two paths, or the second replaces the first |
| `FLEZHA_WORKFLOW_E2E_RELEASE` | workflow fixtures | Releases a parked crash-resume fixture — see the contract below |

## The report

With `FLEZHA_E2E_RUNTIME` set, or after any `:system` test ran, the
`FlezhaTest.Helper.TimingFormatter` prints a capability table — `pass`, `fail`,
`deferred` or `untested` — and writes it to `$CONFORMANCE_OUT` as JSON. The nightly uploads one per runtime.

## The static gate

`test/sdk_conformance/gate_test.exs` is untagged, so it rides plain `mix test`
on every pull request with no runner. It asserts every rule above, plus: no
`runtime:` literal under `test/system/` outside an `interop: true` scenario;
every fixture id a file drives belongs to a capability that file declares; every manifest runtime is a `workflow_dispatch` option of
`nightly-e2e.yml`; every `docs` page exists and names its manifest. A
vocabulary entry with no tagged module is printed, not failed.

## Golden declaration snapshots

The smoke tier binds one fixture per declaration key the controller reads and
compares the decoded `AddOperativeResponse` metadata — intercepted at
`Flezha.Operative.ClassRegistry.decode_declarations/1`, the one place the wire
form is decoded — against `test/fixtures/declarations/<fixture>.json`. One more
snapshot pins the `flz.*` identity stamps on a keyed output. A key one SDK adds,
drops or spells differently is a diff on every other.

Regenerate with `FLEZHA_E2E_SNAPSHOT=update` under the reference runtime; any
other runtime refuses.

## The test-app contract

Every SDK test app SHALL:

- take its namespace from `argv[1]`;
- exit when stdin reaches EOF — that is how the harness tears a runner down;
- honour the harness-injected environment (`FLEZHA_SOCKET_FILES_PATH`,
  `FLEZHA_TRANSPORT`, `FLEZHA_CA_CERT_PATH`, `FLEZHA_QUIC_ADDRESS`, plus
  whatever its own template renders);
- park a **pool worker** — never the driver thread — until
  `FLEZHA_WORKFLOW_E2E_RELEASE` appears, in every workflow fixture a
  crash-resume scenario drives, so the runner keeps answering control pings;
- cap its own logging at error level unless overridden;
- be buildable by its manifest's `build` commands and nothing else.

## When a run is red

Read the failure against the manifest before reading it against the SDK:

- **A capability's tests fail on one SDK only.** That SDK claims a capability it
  does not carry. Either fix the SDK, or — if it genuinely cannot carry it —
  move the capability to that manifest's `deferred` and say so in its scope
  page. The required set is not deferrable; a required capability that fails is
  a defect, not a scope decision.
- **The live-factory-set assertion fails.** The app and its manifest disagree.
  The message names the ids on each side; the manifest is the declaration, so
  either register the fixture or stop listing it.
- **A golden snapshot fails.** The SDK's declaration differs from the
  reference's — the diff names the key. Regenerate the golden only when the
  *reference* deliberately changed, and then every other SDK must follow.
- **The gate fails.** It never needs a runner, so it reproduces instantly with
  plain `mix test`; the message names the file, the capability and the id, and
  ends with this page.
- **It passes alone but fails in the suite.** This repo has a known
  load-sensitive flake population, and the e2e tests share a controller. Re-run
  the file by itself before treating it as a regression, and check for orphaned
  `beam.smp` processes from a killed earlier run — they hold the shared node
  name and the transport ports.

## Adding an SDK to the conformance suite

What the gate enforces, in the order it checks — a pull request adding
`test/apps/<sdk>_runner/` is red until all of it holds:

1. The app directory carries `conformance.json`.
2. The manifest declares every field, with both `test` and `bench` profiles.
3. The manifest declares an `sdk_lang`, distinct from every other manifest's, and
   equal to the value the SDK actually compiles in.
4. Every capability is accounted for — deferred or carried, exactly once.
5. No required capability is deferred.
6. Every carried capability's fixture ids equal the Rust reference's.
7. The runtime is a `workflow_dispatch` option of `.github/workflows/nightly-e2e.yml`.
8. The `docs` page exists and references the manifest by path.
9. Where the SDK spells its language byte is added to the gate's one
   source-of-truth table (`test/sdk_conformance/gate_test.exs`), so the manifest
   can be checked against the constant rather than trusted. That row is data,
   not a clause: nothing else about admitting a runtime touches the harness.

### `sdk_lang`

The allocated language byte your SDK stamps at byte 0 of every durable record
and advertises on every factory. Take the next free value from the table of
record in `flz_core/src/header.rs` — one value per SDK, assigned when the SDK
lands, never asserted by a proposal in advance.

An SDK that persists nothing still declares one. The byte identifies the runtime
at the *placement* seam, not only in a record header: the controller places an
operative only on a runner advertising its factory's established language, and
the conformance fleet gives different SDKs the same factory ids on purpose so
one workload can be measured on each runtime. Without the byte those shared ids
would be a cross-language restore waiting to happen; with it they are safe.

The gate fails on a missing value, a duplicate, or a manifest that has drifted
from the constant its SDK compiles in. The `interop`-tagged scenario proves the
other half — that the runner advertises the byte on connect and that placement
never crosses languages.

`:interop` is excluded from `mix test` **and** from `--include system`: it is
the one scenario that places two runtimes in a single run, and the tiers are
one-runtime-per-run by contract. Invoke it on its own:

```
mix test --include system --include interop \
  test/system/cross_sdk_placement_e2e_test.exs
```

The nightly runs it once, on the `java` matrix job — the job that has both a
JDK and the pre-built `rust_runner`.

Then the test-app contract above, which the smoke tier exercises.

What the gate cannot check, and you still owe:

- the SDK's own unit-test workflow;
- [`naming_conventions.md`](naming_conventions.md) compliance;
- a dev note under `docs/dev_notes/` and its `CLAUDE.md` index line.

The loop: get `--include sdk_smoke` green with every optional capability
deferred, then un-defer one capability at a time until `--include system` is
green.

## See also

- [`authoring_contract.md`](authoring_contract.md) — what a conforming authoring surface expands to
- [`../architecture/runner.md`](../architecture/runner.md) — the kernel split and the "one change, every SDK" rule
- [`elixir_sdk.md`](elixir_sdk.md), [`java_sdk.md`](java_sdk.md), [`node_sdk.md`](node_sdk.md) — the per-SDK scope pages
