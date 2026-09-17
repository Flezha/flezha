# The cross-SDK conformance suite

One end-to-end suite, one SDK per run. `FLEZHA_E2E_RUNTIME` selects which SDK's
test app the harness spawns; nothing under `test/system/` names a runtime.

```bash
mix test --include system                              # rust, the default
FLEZHA_E2E_RUNTIME=java mix test --include system      # the whole suite on Java
FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke   # the two-minute inner loop

# A runtime whose dispatch topology is a run-time choice is run under each: the
# fixtures are identical, the run is not. Two SDKs have one today.
FLEZHA_E2E_RUNTIME=node mix test --include system
FLEZHA_E2E_RUNTIME=node FLEZHA_NODE_WORKERS=4 mix test --include system
FLEZHA_E2E_RUNTIME=php mix test --include system
FLEZHA_E2E_RUNTIME=php FLEZHA_PHP_EXECUTOR=swoole mix test --include system
FLEZHA_E2E_RUNTIME=python mix test --include system
FLEZHA_E2E_RUNTIME=python FLEZHA_PYTHON_EXECUTOR=asyncio mix test --include system
```

A full cross-SDK check is one run per runtime. `mix test` on its own spawns no
runner and still runs the static gate below.

`FLEZHA_E2E_RUNTIME` steers the conformance suite only. A controller or
integration test outside `test/system/` that merely needs *a* runner keeps the
reference SDK: it is written against the reference fixtures, so pointing a run
at an SDK that carries a subset of them would turn it red for a reason that is
not about that SDK. `FlezhaTest.SystemCase` is what opts a module in.

## What an arm needs on the host

Two manifests launch through a bare interpreter name, so those arms are a `PATH`
question the tree cannot answer for you.

**Ruby** — `test/apps/ruby_runner/conformance.json` runs `ruby {artifact}`, and
the SDK's floor is `>= 3.3.0` (`sdk/ruby/flezha.gemspec`). A distro Ruby under
that fails at load, not at a test, so the arm runs against an interpreter put in
front of it:

```bash
PATH="$HOME/.rbenv/versions/3.4.10/bin:$PATH" \
  FLEZHA_E2E_RUNTIME=ruby mix test --include system
```

**PHP** — `test/apps/php_runner/conformance.json` runs `php {artifact}`, and the
SDK needs `ext-ffi` (`sdk/php/composer.json`); `FLEZHA_PHP_EXECUTOR=swoole`
additionally needs a ZTS build carrying `Swoole\Thread` with `ffi.enable=true`,
which `docker/dev/Dockerfile.php --target zts` builds. A host whose `php` does
not satisfy the arm runs it through a `php` shim on `PATH` that execs that
image:

```sh
#!/bin/sh
exec docker run --rm -i --init --network host \
  <the repo, /tmp/flezha_sockets and $XDG_RUNTIME_DIR bind-mounted at the same
   absolute paths, and every FLEZHA_* variable forwarded by name> \
  flezha-php-zts php "$@"
```

Four parts of that line are load-bearing:

- `--network host`. Without it the container has its own loopback, the UDS half
  of the suite passes and every `transport=quic` test fails with "temporary test
  runner not connected" — the runner dials `127.0.0.1:4433` and reaches itself.
  Seven failures, all QUIC, is the signature.
- `-i`. The harness stops a runner by closing its port's stdin. With `-i` the
  attach stream drops, the PHP inside sees EOF and drains; without it the
  container outlives the run.
- The three mounts at their **same absolute paths**. A UDS path has to mean one
  thing on both sides of the container boundary.
- Every `FLEZHA_*` variable forwarded by name. A runner's transport environment
  comes from `PortPilot.open_port/2`'s `:env` and from no file, so a shim that
  drops the environment produces a runner that cannot find the control socket.

A fixture whose handler never returns — `wedge_probe`, driven by the `liveness`
and `pool_wedge` capabilities — is not stopped by closing the port, because the
port holds only the `docker run` client. Give the containers a common `--name`
prefix and `docker rm -f` them after the arm.

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

`runner_lifecycle`, `connect_discovery`, `graph_builder`, `liveness`,
`pool_wedge`, `retry_ladder`, `state_persistence`, `effectively_once`,
`entity_timers`, `correction_window`, `keyed_channels`, `workflow`,
`dynamic_entity`, `partitioned`, `schema_evolution`.

A capability can carry no fixtures at all, where the scenarios that prove it
need none of their own. Carrying fixtures and being required are independent:
`pool_wedge` does both — its `wedge_probe` fixture is what exercises the abandon
path, the worker replacement and the classifier's verdict on that SDK rather
than on the reference alone, so no manifest may defer it.

**No capability is claimed without being proved.** Every vocabulary entry is
tagged by at least one module or test under `test/system/`, and the gate fails
otherwise — so a capability a manifest lists under `fixtures` is one a scenario
exercises, and a capability nothing exercises must be deferred by every
manifest. An SDK that carries a mechanism proves it here rather than resting on
its own unit tests.

Every module under `test/system/` declares one:
`use FlezhaTest.SystemCase, capability: :<atom>`. A module whose scenarios span
two carries the second per test, with its own skip:
`@tag capability: :effectively_once, skip: FlezhaTest.SystemCase.deferral(:effectively_once)`. Every capability appears in every
manifest exactly once — under `deferred` or as a `fixtures` key — so one can be
neither forgotten nor claimed twice. The **required set**
(`runner_lifecycle`, `connect_discovery`, `graph_builder`, `liveness`,
`retry_ladder`, `partitioned`, `pool_wedge`, `state_persistence`,
`keyed_channels`, `dynamic_entity`, `schema_evolution`, `entity_timers`,
`effectively_once`, `correction_window`) can never be deferred, and it holds
**every capability no manifest defers**: a capability every SDK carries is one
none of them may drop. The gate refuses drift in both directions, with a
different message for each — a manifest deferring a required capability names
that manifest, a universally carried capability missing from the list names the
capability and the declaration it belongs in
(`FlezhaTest.Helper.NodeTestHelper.required_capabilities/0`). The list is
written, never derived: an SDK that genuinely cannot carry one defers it by name
and leaves the list in the same change, so a mandate is never dropped as a side
effect of a manifest edit.

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
| `FLEZHA_SOCKET_FILES_PATH`, `FLEZHA_TRANSPORT`, `FLEZHA_CA_CERT_PATH`, `FLEZHA_QUIC_ADDRESSES` | every test app | Injected by `PortPilot.open_port/2`; a test app must honour them, and an entry a scenario supplies replaces the injected one of the same name. `FLEZHA_QUIC_ADDRESS` is retired: an app reading it fails the gate |
| `FLEZHA_RUNNER_ID_PATH` | every test app | Where the runner persists its stable identity. Two runners in one namespace need two paths, or the second replaces the first |
| `FLEZHA_WORKFLOW_E2E_RELEASE` | workflow fixtures | Releases a parked crash-resume fixture — see the contract below |

## The test tiers

A tier is a slice of the suite plain `mix test` does not run. `FlezhaTest.Tiers`
(`test/helpers/tiers.ex`) is where one is declared: the tag, the workflow that
runs it or the reason a person has to, what a host needs first, the transports
it covers and the section of this page that describes it. `test/test_helper.exs`
derives its `exclude:` from that table, and `test/sdk_conformance/gate_test.exs`
fails when a selector names a tier the table does not declare, when a declared
tier is run by nothing and says no reason, or when a tier names a section of
this page that is not here.

| Tier | Runs in | Blocks merge | Needs |
| --- | --- | --- | --- |
| `:system` | `nightly-e2e.yml` | no | an SDK runtime named by `FLEZHA_E2E_RUNTIME`, built as an OS process |
| `:sdk_smoke` | `java-sdk.yml`, a subset of `:system` | yes | the same runtime; the two-minute inner loop |
| `:workflow_crash_resume` | `nightly-e2e.yml`, a subset of `:system` | no | the same runtime; the crash-and-resume slice |
| `:interop` | `nightly-e2e.yml` | no | two SDK runtimes in one run, and a namespace no other tier is using |
| `:multi_node` | `nightly-e2e.yml` | no | a peer BEAM this tier boots and stops mid-suite |
| `:testnet` | a person | no | `mix test.setup`, then a free port `Flezha.Testnet.ws_port/0` derives |
| `:kafka_integration` | a person | no | a reachable Kafka broker |
| `:postgres_integration` | a person | no | the `postgres` compose service |
| `:s3_integration` | a person | no | the `minio` compose service and `S3_INTEGRATION=1` |
| `:memory_drift` | a person | no | `MIX_RUN_MEMORY_DRIFT=1` and the time the harness takes |
| `:credit_soak` | a person | no | the time the soak takes |

`Blocks merge` is derived, never declared: a tier blocks merge when the workflow
that runs it has a `pull_request` trigger. Everything above answering `no` is
proven nightly or by hand, which is a fact this table prints rather than one a
reader reconstructs from an exclude list plus a workflow trigger.

A tier is selected by name, and only by name: an ExUnit **include beats an
exclude**, so `--include system` runs an excluded tier however it is excluded.
A tier that must not ride another one's include declares `self_skip` and its
modules call `FlezhaTest.Tiers.skip_unless_included/1`, because a skip is
honoured whatever the filters say. `:interop` is the one tier that needs it.

A scenario is NOT selected by a tag of its own. The axis a `test/system` module
declares is its `capability:`, which the manifests read; a second tag naming the
same scenario reads like a filter and selects nothing. Run one module by naming
its file.

## An SDK repository's own tiers

An SDK developed in a repository of its own runs three tiers, and none of them
starts a controller.

| Tier | What it proves | What it needs |
| --- | --- | --- |
| unit | the package's own suite, including every test that reads a shared fixture | the contract set |
| local mode | the five capabilities the language layer owns, run in process on the production dispatch path | the contract set and the native binary |
| smoke | the SDK connects, is given work, completes it and shuts down | the native binary and the fake controller |

All three run against artefacts one core release published: the native binary,
`flz-fake-controller`, and `flezha-fixtures-<version>.tar.gz`. None of them
builds the core, and none needs a checkout of it.

What an SDK repository cannot see is everything in the capability vocabulary
the controller owns: persistence, effectively-once, correction, partitioning,
workflows, liveness, restarts. Every one of those is an end-to-end scenario and
runs here, against the commit this tree pins the SDK at. Bumping that pin is
what brings an SDK change into end-to-end.

### The fake controller

`flz-fake-controller` is the control-socket peer a runner meets when there is
no controller. It is a binary of `flz_core_ffi` behind its `fake-controller`
feature, over the same module the crate's own ABI scenario drives, so the
binary and the suite here cannot read the wire two different ways. It is
published for every supported triple beside the native libraries, with its
checksum in the same manifest.

```
flz-fake-controller --factory <id> [--operative <id>] [--transport uds]
                    [--channel <name>] [--inputs <file>] [--input <payload>]...
                    [--runner-id <id>] [--expect-outputs <n>] [--timeout-ms <ms>]
                    [--socket-dir <dir>] [-- <runner command>]
```

Given the runner command after `--`, it launches the runner against a socket
directory of its own, holds the runner's standard input open for the run, and
prints the runner's output when the run fails; without one, the runner is
started by hand against `--socket-dir`. It binds the control socket and the two
data sockets, waits for a runner to connect and declare `--factory`, walks the
handshake, starts the class, feeds each line of `--inputs` and then each
`--input`, counts `--expect-outputs` frames back, and then disconnects and waits
for the runner to go. It prints
each frame as it awaits it and exits non-zero naming the first that is wrong or
late, so a failing tier says which end stopped. The tier's verdict is this
binary's exit code, never the runner's: a terminal disconnect ends a runner
non-zero on purpose.

`--transport` takes `uds`. QUIC is refused by name rather than ignored,
because its peer needs this repository's test certificates.

### An extracted SDK is a submodule at its old path

An SDK that has moved to its own repository is present here as a git submodule
at `sdk/<lang>`, pinned to a commit, with `test/apps/<lang>_runner` a symlink to
the test app inside it. Nothing in the harness learns about repositories: the
gate globs the same paths, the build scripts `cd` into the same directories,
and `PortPilot` reads the manifest where it always was, launching the app from
the directory the symlink points at. An uninitialised submodule fails the gate
on the missing manifest, as any absent test app would.

The static gates that walk `git ls-files`, which does not enter a submodule,
read what each submodule carries as well, so they judge an extracted SDK's
source here too.

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
vocabulary entry no module or test is tagged for fails the gate by name.

It also holds the administration surface to one declared set. Every SDK spells
the same verb names over one shared entry point (see [`admin.md`](admin.md)),
and nothing else compares them: the gate names the set once and measures each
SDK's own wrapper against it, and refuses a wrapper that names a verb it will
not call or a refusal reason of its own. Reachability is the controller's
answer, so an SDK that decided for itself would be a second policy point.

### The contract set

The cross-language fixtures are a named set, and `test/fixtures/contract.txt`
is the name. Every member is listed there with its repository-relative path and
what reads it: the declaration keys, the knob parse rule, the reference
topologies, the channel codecs, the local-mode scenario, the durable frame, the
layout rows, the connect environment, the C ABI surface,
`protobufs/runner_commands.proto`, and the few files that are still written as
Rust tables and read as data. The gate holds the list and the directory equal,
so a fixture the tree gains and the list does not name fails by name.

A release publishes the set as one archive,
`flezha-fixtures-<version>.tar.gz`, whose entries are those paths verbatim. An
SDK developed outside this tree unpacks it at `contract/` in its own package.

Every SDK's tests resolve the directory those paths hang off **once**, in this
order:

1. the directory `FLEZHA_CONTRACT_DIR` names,
2. else `contract/` at the package root,
3. else the nearest enclosing tree that carries the manifest.

None of the three resolving is a **failure naming all three**, never a skip. A
skip is how twelve of the PHP package's tests stopped running outside a
checkout without anyone noticing.

`test/fixtures/abi_surface.txt` is the member that exists so that a host
binding declaring the C surface in its own language asserts against data rather
than against the crate's source. A `flz_core_ffi` test holds the file and the
crate's exports equal in both directions.

It also pins the half a fixture cannot express. `test/fixtures/runner_connect_env.txt`
holds the environment contract every SDK's connect path reads — the
socket-directory variable, the QUIC address-list variable, the group variable
and the ladder candidates **in order** — in the shape
`test/fixtures/host_class_decl_keys.txt` uses for declaration keys. Three
readers hold it: a `flz_transport` test asserts the ladder's own constants and
rung order against it, and the gate asserts that no SDK's connect path reads a
socket or address variable the file does not pin, that the one implementation
walks the rungs in the pinned order, and that no SDK and no test app honours a
retired name. The one file allowed to name a retired variable is the one that
refuses it.

`connect_discovery` is in the required set, because every SDK carries it — Rust,
Java, Node, Ruby, PHP and Python through `flz_core`, Elixir through `flz_nif` —
and `test/system/connect_discovery_e2e_test.exs` drives it under whichever
runtime the run selects.

An SDK whose transport is not the shared `flz_transport` is the one case that
cannot honour this contract, and it says so **in its own manifest** rather than
through a standing exemption: it lists `connect_discovery` under `deferred`, the
gate fails naming that manifest and the required set, and the same change removes
the capability from the required set. Both halves land in one reviewed diff. The
live example is the Elixir SDK's in-node `:native` mode
(`docs/sdk/elixir_sdk.md`), which passes BEAM messages inside the controller's own
node and walks no socket ladder; it does not arise here because
`test/apps/elixir_runner/conformance.json` declares `transports: ["uds", "quic"]`,
so the conformance app runs the standalone binary protocol like the other six.

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
  `FLEZHA_TRANSPORT`, `FLEZHA_CA_CERT_PATH`, `FLEZHA_QUIC_ADDRESSES`, plus
  whatever its own template renders);
- park until `FLEZHA_WORKFLOW_E2E_RELEASE` appears, in every workflow fixture a
  crash-resume scenario drives, so the runner keeps answering control pings and
  a sibling procedure the driver has not reached stays dispatchable. The
  obligation is one; its spelling follows the dispatch model the manifest
  declares. A worker-pool host parks a **pool worker** and never the driver
  thread. An event-loop host **suspends the activation and yields the loop** —
  a synchronous hold there fails this contract rather than implementing it,
  because it freezes every other activation the loop owns, which is the wedge
  signature `pool_wedge`'s fixtures exist to produce;
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
5. No required capability is deferred, and no capability that every manifest
   carries is missing from the required set.
6. Every carried capability's fixture ids equal the Rust reference's.
7. The runtime is a `workflow_dispatch` option of `.github/workflows/nightly-e2e.yml`.
8. The `docs` page exists and references the manifest by path.
9. Where the SDK spells its language byte, and where it spells its own released
   version, are added to the gate's source-of-truth tables
   (`test/sdk_conformance/gate_test.exs`), so the manifest can be checked against
   the constant rather than trusted and the constant against the SDK's package
   metadata. An SDK whose reader *is* its manifest — the Rust compiler, the
   Elixir app spec, Ruby's single `VERSION` — is recorded as having nothing to
   compare. Those rows are data, not clauses: nothing else about admitting a
   runtime touches the harness.
10. No bench task's help text enumerates the runtimes. `mix bench` and
    `mix bench.roundtrip` resolve `--runtime` against the manifests, so a new
    SDK joins them by existing; the gate fails a list that reappears in docs.
11. The app answers `--conformance-report <path>` by driving every row of
    `test/fixtures/reference_topologies.txt` through its own builder, and the
    manifest declares `topology_surfaces` — the authoring surfaces that builder
    carries. See "The refusal table" below.

### From outside this tree

An SDK developed in a repository of its own joins the suite the same way, with
two differences. Its test app is a directory of its own repository rather than
of this one, and it arrives here as a submodule at `sdk/<lang>` with
`test/apps/<lang>_runner` a symlink to it, so every path above is the path the
gate already checks. And its own suite carries the three tiers described under
"An SDK repository's own tiers", which is what makes a change to it provable
before this tree bumps the pin.

The manifest's `build` line names the command that builds the runner in this
tree, because this tree is where it is read: `PortPilot` quotes it when an
artifact is missing, and `mix bench` runs the bench line from the root.

### The refusal table

Judging a topology is one job, and `flz_core::host::register::check` is where it
is done. A builder records the links its author declared — a `transform`, a
`key`, a `named`, a `sink`, an `into`, a `merge`, a `branch` — and submits the
record; the core answers with the derived declaration or with a list of named
refusals. The C ABI entry is `flz_core_check_class`, read-only beside
`flz_core_declare_class`; the Elixir SDK is its own core and re-expresses the
same check against the same fixture.

`test/fixtures/reference_topologies.txt` carries one row per refusal: the name,
a topology id, the authoring steps in a language-neutral spelling, and the
surface the row needs. `test/sdk_conformance/topology_check_test.exs` fails when
a name has no row or a row has no name — a refusal that exists is a refusal
every SDK is measured against.

A new SDK's arm is one row interpreter in its test app. It maps the step
vocabulary to that SDK's builder, binds the palette roles to its own fixture
procedures, and writes a report:

```json
{"runtime": "node",
 "rows": [{"topology": "dup_key_by", "outcome": "refused", "names": ["DuplicateKeyBy"]},
          {"topology": "branch_one_arm", "outcome": "skipped", "needs": "branch"},
          {"topology": "fan_in_noop", "outcome": "accepted",
           "derived": [{"index": 0, "name": "in", "kind": "entry",
                        "producer": null, "consumers": [0], "sink": null}]}]}
```

`test/system/topology_refusal_e2e_test.exs` reads it: ran plus skipped must equal
the table, every skip must name a surface the manifest's `topology_surfaces`
does not list, every malformed row must refuse with the fixture's name, and every
well-formed row must derive to the fixture's lines. The same module boots each
app under `FLEZHA_REFUSAL_FIXTURE=<topology id>` — the app registers only that
row's topology — and asserts the runner dies with the refusal's name in its
output. A runner that refuses at boot never connects, so the assertion reads the
exit status and the output, not a control frame.

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

### `sdk_version`

Bytes 1:5 of the same header name the *writing* SDK's own released version, not
the kernel's: six SDKs release on their own cadences over a core that may not
move, and "which SDK build wrote this record" is the question a migration asks.

Declare it as the version string your package metadata already spells — the core
packs it to `(major << 16) | (minor << 8) | patch`, each component clamped at
255, so every SDK's header carries one packing rather than one per language. A
value that packs to zero is refused at declaration time naming the field.

The shape is one literal in the language's package metadata, one reader, and the
gate failing when the two disagree. Where the reader *is* the manifest — the Rust
compiler's `env!`, the Elixir app spec, Ruby's single `VERSION` — there is
nothing to compare and the gate's table says so. The PHP SDK is the one with
nothing to compare *against*: `composer.json` declares no `version` key by
design, so its constant is that SDK's sole source of truth and the release
checklist owes the bump.

The field is advisory. `sdk_lang` decides which codec wrote a body and
`schema_version` decides its shape; no reader decides how to read a record from
`sdk_version`, and the packing is lossy — `0.2.0-rc1` and `0.2.0` are the same
value — so nothing may be built on an equality it cannot hold.

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

The loop: get `--include sdk_smoke` green with every optional capability
deferred, then un-defer one capability at a time until `--include system` is
green.

## See also

- [`authoring_contract.md`](authoring_contract.md) — what a conforming authoring surface expands to
- [`../architecture/runner.md`](../architecture/runner.md) — the kernel split and the "one change, every SDK" rule
- [`elixir_sdk.md`](elixir_sdk.md), [`java_sdk.md`](java_sdk.md), [`node_sdk.md`](node_sdk.md),
  [`ruby_sdk.md`](ruby_sdk.md), [`php_sdk.md`](php_sdk.md),
  [`python_sdk.md`](python_sdk.md) — the per-SDK scope pages
