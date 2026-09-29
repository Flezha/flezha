# sdk-core-abi Specification

## Purpose
TBD - created by archiving change extract-sdk-poll-core. Update Purpose after archive.
## Requirements
### Requirement: C ABI surface and lifecycle

`flz_core_ffi` SHALL expose the core over a C ABI: context create/destroy, configure,
start, `poll` (blocking, with timeout), `complete`, `abandon`, shutdown, `last_error`,
and a log callback. Ownership follows the existing FFI convention: whoever allocates
frees; buffers returned by the core are freed via a core-provided free function; the
context pointer is opaque.

`abandon` takes a polled activation's token and re-hands its untouched items,
releasing the cell as if it had never been polled. It exists because no unwind
crosses the ABI: a veneer whose host language throws, or whose thread dies, has
no way to reach `complete`, and without an explicit call the cell would be
retired for the lifetime of the process. A veneer SHALL answer every polled
activation exactly once — `complete`, the failure path, or `abandon` — and SHALL
make that structural in its own language (a `finally`, a destructor, a scope
guard), not a discipline at each return site.

A context handle SHALL be dead from the moment the destroy entry point is called on it.
No veneer SHALL pass a destroyed handle back into any ABI entry point, `last_error`
included, and each veneer SHALL make that structural in its own language rather than a
discipline at each call site — by dropping the handle it stores, by an owned object whose
accessor refuses once freed, or by a guard on the shared handle — so a host call arriving
after the destroy is refused in the host's own vocabulary instead of dereferencing freed
memory.

#### Scenario: Shutdown wakes blocked pollers
- **WHEN** host workers are blocked in `poll` and shutdown is requested
- **THEN** every blocked `poll` SHALL return promptly with a distinguishable "shutting down" result, and no activation SHALL be lost (unpolled work follows the existing durability rules)

#### Scenario: Poll honors its timeout
- **WHEN** `poll` is called with a 100 ms timeout and no work arrives
- **THEN** it SHALL return an empty/timeout result within a bounded margin of 100 ms rather than blocking indefinitely

#### Scenario: An abandoned activation releases its cell
- **WHEN** a veneer polls an activation for cell E and calls `flz_core_abandon` with its token instead of completing it
- **THEN** E SHALL become pollable again with its items intact and in order, and no retry-ladder attempt SHALL have been consumed

#### Scenario: An unknown or already-answered token is rejected, not silently accepted
- **WHEN** `abandon` or `complete` is called with a token that was never handed out, or that was already answered
- **THEN** the call SHALL return an error retrievable through `last_error`, and SHALL NOT disturb any live handout

#### Scenario: A host call after the context is destroyed is refused, not dereferenced
- **WHEN** host code reaches a context-taking entry point through a veneer — a class declaration, a poll, a completion — after that veneer has destroyed its context
- **THEN** the veneer SHALL raise its own language's refusal naming the freed context, and SHALL NOT call the ABI with the destroyed handle

### Requirement: Versioned activation schema with boot-time refusal

The Activation/ActivationResult schema SHALL carry a version. Core and host shim SHALL
negotiate at context creation and SHALL refuse to start on an incompatible version with
an error naming both versions. Silent runtime skew is forbidden. Every activation kind
SHALL be pinned by a cross-language byte fixture shared between core tests and each
shim's tests, extending the existing fixture discipline (`flusher.rs` ↔ `wire_test.exs`).

#### Scenario: Version mismatch fails at boot
- **WHEN** a shim built against schema version N connects to a core speaking N+1 incompatibly
- **THEN** context creation SHALL fail with an error naming N and N+1, and no connection to the controller SHALL be attempted

### Requirement: Thread-safety contract

The core SHALL accept `poll` from any host thread and SHALL accept `complete` from any
host thread, including a different thread than the one that polled the activation. The
core SHALL NOT require thread affinity from the host.

The converse binds the host. A host whose unit of concurrency cannot migrate between threads, such
as an event loop, SHALL keep every blocking ABI entry point off that thread. The blocking entry
points are the poll, the starvation wait, the completion, the bounded operator receive, the spawn result
read with a nonzero timeout, and the runner start, and such a host SHALL reach them only from threads
whose blocking cannot stall its dispatch. The spawn request, and the spawn result read with a timeout
of zero, never wait and MAY be called from any thread.

The completion is what makes this a correctness rule rather than a latency one: under the default
overflow policy it waits until the consumer's mailbox drains, and the consumer drains only when its
handler runs. A host that blocks the one thread able to run that handler cannot recover, because the
inline-drain escape the crate host uses takes a host function pointer and is by construction
unreachable over the ABI.

#### Scenario: Complete from a different thread
- **WHEN** worker A polls an activation and hands it to worker B, which completes it
- **THEN** the completion SHALL be accepted and the cell released, identically to same-thread completion

#### Scenario: The rule is recorded for the next binding
- **WHEN** a new host binding is designed whose concurrency primitive is bound to one thread
- **THEN** the ABI documentation SHALL state this constraint and name the completion entry point as the reason

#### Scenario: A spawn request never blocks its caller
- **WHEN** a host calls `flz_core_spawn_request` from an event-loop thread while the controller is slow to answer
- **THEN** the call SHALL return a handle without waiting, and only a `flz_core_spawn_result` read with a nonzero timeout SHALL wait

### Requirement: The C ABI cannot lag the crate API

The Rust SDK SHALL consume `flz_core`'s Rust API (the same crate the C ABI wraps), and
CI SHALL drive at least one full runner scenario through `flz_core_ffi` itself. A
capability reachable through the crate API but not through the C ABI is a defect of
this capability, not a backlog item.

Every host language binding SHALL be built from this crate and SHALL be a one-to-one veneer over its
entry points, including a binding added under its own feature on the crate. A binding SHALL NOT
re-express dispatch, ordering, overflow, the retry ladder, durability or the bounded driver, SHALL
NOT reach a kernel capability the C ABI does not expose, and SHALL NOT alter the artifacts the other
bindings consume when its feature is enabled.

Every host binding SHALL declare its exported surface as a table pairing each export with the ABI
entry point it reaches, and a test SHALL assert that the binding's actual exports are exactly that
table. The assertion SHALL hold for every binding without exception, including a binding whose host
reaches the core through a language-specific native interface rather than the C ABI directly. A
binding whose surface is compared by review rather than by a test does not satisfy this requirement.

#### Scenario: FFI parity is tested, not assumed
- **WHEN** the SDK test suite runs in CI
- **THEN** it SHALL include a scenario that connects, processes input activations, fires a timer, and shuts down entirely through the C ABI, failing if any required entry point is missing

#### Scenario: A binding adds no ABI surface
- **WHEN** a host binding's exported surface is compared with the C ABI's
- **THEN** each export SHALL correspond to an existing entry point or SHALL reach none at all, and the comparison SHALL be asserted rather than reviewed

#### Scenario: A binding's feature is additive
- **WHEN** the crate is built with and without a binding's feature
- **THEN** the existing artifacts SHALL be unchanged and the other bindings' exports SHALL be unaffected

#### Scenario: Every binding declares its surface, the JNI one included
- **WHEN** the bindings are audited for a declared surface table and its assertion
- **THEN** each of them SHALL have both, and removing one export from a binding's host-side declaration SHALL fail that binding's surface test

#### Scenario: An entry point added to one binding and not its declaration is a red build
- **WHEN** a host-side declaration and the binding's exports disagree in either direction
- **THEN** the surface test SHALL fail naming the export and the direction of the disagreement, rather than the disagreement surfacing as a missing method at the first call

### Requirement: The declaration surface covers the full host layer

`flz_core_declare_class` SHALL accept the complete host-class declaration:
cell mode (single/static with keys, dynamic with TTL and cap), clock
kind, correction window and beyond-window policy, delivery tier
(effectively-once, checkpoint ack-granularity), inter-tasklet channels
with edge ids and keying markers, per-channel retry and overflow
policies, the record-header lang/format bytes, the declaring SDK's own
version, persistence, and bounded operator
lifecycle facts (loop mode, rollback mode, retry, wake-after,
compensable tasks). Timer caps stay host-layer policy — in both SDKs the
cap is enforced where the registration API lives, not in the core. A declaration
the core cannot honor SHALL be refused at declaration time with an error
naming the field — never accepted and degraded silently.

The record-header language byte is **mandatory and allocated**: a declaration
carrying no `sdk_lang`, or `sdk_lang = 0`, SHALL be refused at declaration time
naming the field. The core SHALL NOT substitute a default, because a veneer that
reaches placement untagged is a veneer whose state another language can be
handed. The core SHALL expose the declared value so the runner's factory
advertisement can carry it without the veneer restating it.

The record-header **version** is mandatory on the same terms: a veneer SHALL
declare `sdk_version` as the version string its own package metadata spells, and
a declaration carrying no `sdk_version`, or one that packs to zero, SHALL be
refused at declaration time naming the field and the offending value. The core
SHALL NOT substitute its own package version, because the header records which
SDK wrote a record and a value the kernel supplies answers a different question.
The core — not the veneer — SHALL perform the packing, so that every SDK's header
carries one packing rather than one per language. The core SHALL stamp the packed
value into every durable header it writes for that class, and SHALL NOT publish it
in the operator's declaration metadata: it describes the writing runtime, not the
class, and a shared golden snapshot of that metadata could not hold a value that
differs per SDK.

#### Scenario: A full declaration round-trips into controller metadata
- **WHEN** a shim declares a checkpoint-ack, correction-windowed, dynamic-mode class through the ABI and the controller sends `AddOperator`
- **THEN** the `AddOperatorResponse` metadata SHALL carry the same declarations the typed Rust path would produce for an identical class

#### Scenario: An unsupported declaration is refused loudly
- **WHEN** a shim declares a field combination the host layer cannot serve
- **THEN** `flz_core_declare_class` SHALL return an error naming the offending field, and the class SHALL NOT be registered

#### Scenario: A class declaring no language is refused
- **WHEN** a veneer declares a class with `sdk_lang` absent or zero
- **THEN** `flz_core_declare_class` SHALL return an error naming `sdk_lang`, and the class SHALL NOT be registered

#### Scenario: A class declaring no version is refused
- **WHEN** a veneer declares a class whose `sdk_version` is absent, empty, all-zero or not a version
- **THEN** `flz_core_declare_class` SHALL return one error naming `sdk_version` and the offending value, and the class SHALL NOT be registered

#### Scenario: The declared version reaches the header and nothing else
- **GIVEN** a veneer that declared its classes with a version different from the kernel's own package version
- **WHEN** a Cell of that class writes a durable record and the controller reads the operator's declaration metadata
- **THEN** bytes 1:5 of the record SHALL carry the declared version, and the declaration metadata SHALL NOT carry it

#### Scenario: The advertisement reads the declared value from the core
- **GIVEN** a veneer that declared its classes with `sdk_lang = 2`
- **WHEN** the runner builds its connect init frame and any later `UpdateFactories`
- **THEN** each factory entry SHALL carry the language declared for the class registered under that id, resolved from the core rather than from a constant in the veneer

### Requirement: Widened host capabilities are reachable through the ABI

Every capability the widened host layer supports SHALL be exercisable
through `flz_core_ffi` alone — the five command activation kinds, the
prepare crossing, cell modes, tiers, timers, inter-tasklet routing,
and bounded operator lifecycles. The CI scenario suite SHALL grow to drive at least:
a restore fold, a live state read, a keyed dynamic route via the prepare
crossing, an effectively-once completion with offsets, and a bounded operator
tasklet's `complete` ending its instance, all through the C entry points,
failing if any required surface is missing. A step answer SHALL carry a
tasklet's completion request and its result, and no entry point SHALL exist
for a one-function bounded body, which no host declares.

Every shim is a consumer of the C entry points, never a fork of them —
whether it is a language veneer built from this crate (the JNI veneer behind
the `jni` feature, the N-API one behind `napi`, the Ruby one behind `ruby`, the
PyO3 one behind `python`, and any later one) or a host that binds the exported
symbols directly with no compiled veneer of its own, as the PHP SDK does through
`ext-ffi`. A shim SHALL add no kernel path of its own, and a capability one
shim's language can reach that bypasses the ABI SHALL be a defect of this
capability.

A shim that binds the ABI directly SHALL declare the exported functions it
calls in one place, so its declared surface can be compared against the crate's
exported symbols the way a veneer's exports are.

#### Scenario: A restore fold crosses the ABI
- **WHEN** the scenario suite ships a persisted change log for an ABI-declared cell and the controller path delivers `RestoreInitState`
- **THEN** a `RestoreFold` activation SHALL be pollable through `flz_core_poll`, its completion SHALL unblock the cell, and the cell's next input activation SHALL observe the folded state's effects

#### Scenario: The Java shim is a consumer, not a fork
- **WHEN** the Java SDK's JNI bridge is compared against the ABI's exported symbol set
- **THEN** it SHALL call only `flz_core_ffi` entry points — any Java-reachable kernel capability that bypasses the ABI is a defect of this capability

#### Scenario: The Ruby veneer is a consumer, not a fork
- **WHEN** the Ruby veneer's exported functions are compared against the ABI's exported symbol set
- **THEN** each SHALL map onto a `flz_core_ffi` entry point — any Ruby-reachable kernel capability that bypasses the ABI is a defect of this capability

#### Scenario: The PHP shim is a consumer, not a fork
- **WHEN** the PHP SDK's FFI declarations are compared against the ABI's exported symbol set
- **THEN** each SHALL name a `flz_core_ffi` entry point — any PHP-reachable kernel capability that bypasses the ABI is a defect of this capability

#### Scenario: The Python veneer is a consumer, not a fork
- **WHEN** the Python veneer's exported functions are compared against the ABI's exported symbol set
- **THEN** each SHALL map onto a `flz_core_ffi` entry point — any Python-reachable kernel capability that bypasses the ABI is a defect of this capability

#### Scenario: A host declares its language and change format
- **WHEN** any shim declares a class through `flz_core_declare_class`
- **THEN** the declaration SHALL carry that shim's `sdk_lang` and `change_format_id`, and the core SHALL stamp durable record headers with them so a record is self-describing to the host that later restores it

#### Scenario: A host that cannot drain inline is still safe
- **WHEN** a shim-based host completes an activation whose emit blocks on a full `DropNothing` consumer mailbox, and the inline-drain hook the in-process Rust host installs is unavailable to it
- **THEN** the core SHALL report the condition through `flz_core_wait_starvation`, so a host that services that wait can recover without a per-host kernel path

#### Scenario: A step answer ends its bounded operator
- **WHEN** a host answers a bounded operator step activation whose tasklet called `ctx.complete(result)`
- **THEN** the core SHALL end the instance with that result once the iteration commits, with no host-side path of its own

### Requirement: The restore crossing carries each record's stamped schema version

The host restore activation SHALL carry, for the snapshot and for every change
body it crosses, the schema version stamped in that record's header. A host SDK
SHALL NOT be required to parse the 16-byte header itself to learn it.

The core SHALL resolve the version decision (decode / migrate / refuse) before
crossing, so a host SDK receives only items it is expected to decode at its own
version or to migrate from a named `from_version`. A refusal SHALL be produced
by the core and SHALL NOT cross.

The class declaration SHALL carry `schema_version` and
`min_readable_schema_version` per Tasklet, not per class.

#### Scenario: a host receives the version it must migrate from

- **GIVEN** a host-run Tasklet declaring `schema_version = 2` and `min_readable_schema_version = 1`
- **WHEN** a restore payload carries a snapshot stamped version 1
- **THEN** the crossing SHALL present the snapshot bytes tagged `from_version = 1`, and the host SHALL decode them with its version-1 migration rather than its current codec

#### Scenario: an unreadable record never reaches the host

- **GIVEN** a host-run Tasklet declaring `schema_version = 2` and `min_readable_schema_version = 2`
- **WHEN** a restore payload carries a record stamped version 1
- **THEN** the core SHALL fail the restore with a schema error and SHALL NOT cross the record to the host

### Requirement: The C ABI exposes the topology check as a read-only call

The C ABI SHALL expose `flz_core_check_class`, which takes the same declaration record
`flz_core_declare_class` takes and registers nothing: on acceptance it SHALL hand out the
derived declaration as JSON. On refusal it SHALL answer the failing status, and
`flz_core_last_error` SHALL carry the JSON error document of the error model. Its code SHALL
be the first issue's `topology.*` code, and its `details.issues` SHALL list every issue that
fired, each with its own `code`, `message` and `details`, so that no SDK parses prose to learn
which refusals fired. A caller that ignores the error refuses rather than registers.
`flz_core_declare_class` SHALL be that check followed by registration, and its refusal
SHALL be the same error document. The record SHALL carry each channel's ordered chains and
the raw checkpoint flags, and the key set SHALL be pinned by
`test/fixtures/host_class_decl_keys.txt` as today.

#### Scenario: A test asks for the verdict without registering

- **WHEN** an SDK's conformance run submits a fixture row's record to `flz_core_check_class`
- **THEN** it receives the derived declaration for a well-formed row, or an error document
  whose `details.issues` names every refusal by its `topology.*` code for a malformed one, and
  no class is registered

#### Scenario: A stale builder is refused, not degraded

- **WHEN** a builder still sends `channel.consumers` or `class.ack_granularity`
- **THEN** the core refuses the declaration at `deny_unknown_fields` naming the key

### Requirement: The standard-input end-of-file watch SHALL be an ABI entry, not a per-host reimplementation

`flz_core` SHALL own the standard-input end-of-file watch beside the SIGTERM
handler it already installs, and `flz_core_ffi` SHALL export it as one ABI entry
taking a grace in milliseconds. Every erased host SHALL reach it through that
entry rather than reading descriptor 0 itself, so that the rule "a read error is
not end of file, and conflating the two shuts the runner down moments after
connecting" is written once instead of once per host. The watch SHALL end the
process through the same `run_active_graceful_shutdown` path the SIGTERM handler
uses, so a host needs no thread of its own and no callback into its own virtual
machine.

The entry SHALL appear in the C ABI table of every host that carries one, and
the existing Node-only export SHALL become a caller of it rather than a second
implementation.

#### Scenario: A host asks the core to watch its standard input

- **WHEN** an erased host starts its runner
- **THEN** one ABI call SHALL install the watch, and the host SHALL carry no
  reader of descriptor 0 of its own

#### Scenario: The watch and the signal handler share one exit

- **WHEN** either standard-input end-of-file or SIGTERM is observed
- **THEN** the same registered shutdown bridge SHALL run, so the two facts have
  one remedy and a host cannot drain differently for one than for the other

#### Scenario: The ABI table names the entry in every host

- **WHEN** the C ABI table is compared across the hosts that carry one
- **THEN** the entry SHALL be present in each, and the ABI-cannot-lag check SHALL
  fail if a host is missing it

### Requirement: The fake controller is a feature and a binary of the ABI crate

`flz_core_ffi` SHALL carry its fake controller, the control-socket peer that
captures every runner frame and pushes commands, as a public module behind a
`fake-controller` feature and as a `[[bin]]` requiring that feature. The ABI
tests SHALL drive the crate through the module, and the binary SHALL be a
command line over the same module: a socket directory, a transport, a class
to start, inputs to feed, an expected output count and a timeout. It SHALL
exit non-zero naming the first frame that is wrong or late. The release SHALL
publish it for every supported triple beside the native libraries, with its
checksum in the same manifest. The binary SHALL NOT be tracked.

#### Scenario: The tests and the binary cannot drift
- **WHEN** the module's frame handling changes
- **THEN** both the ABI lifecycle test and the binary SHALL see the change, because neither holds a copy

#### Scenario: A runner outside this tree is driven to completion
- **GIVEN** the binary started on a socket directory and a runner launched against it with the pinned connect environment
- **WHEN** the runner connects, starts the named class, completes the inputs and is told to shut down
- **THEN** the binary SHALL exit zero

#### Scenario: A wrong frame is named
- **WHEN** the runner sends a frame the binary did not await, or sends nothing within the timeout
- **THEN** the binary SHALL exit non-zero naming the frame it awaited and what it received

### Requirement: The C ABI surface is a fixture the crate asserts against

`test/fixtures/abi_surface.txt` SHALL list every `extern "C"` entry point the
crate exports, and a test in the crate SHALL assert the exports equal the file
in both directions. A host binding that declares the C surface in its own
language SHALL assert its declaration against that file and SHALL NOT read the
crate's source.

#### Scenario: An entry point added without the file is red here
- **WHEN** an `extern "C" fn flz_core_*` is added to `lib.rs` or `local.rs` and not to the file
- **THEN** the crate's test SHALL fail naming the entry point

#### Scenario: A host declaration reads the file, not the source
- **WHEN** the PHP surface test runs from a tree that carries the fixtures and no Rust source
- **THEN** it SHALL run and pass against the file rather than skip

### Requirement: A spawn is a request and a result read through a handle

The C ABI SHALL expose a tasklet's spawn as two entry points and SHALL NOT expose a blocking
spawn-and-wait entry (`flz_core_spawn_bounded` is removed):

- `flz_core_spawn_request(ctx, factory_id, id, payload, if_running, handle_out)` SHALL send the
  request and return a handle number without waiting for the controller. An empty `id` SHALL ask the
  controller to generate one. A replayed invocation SHALL send nothing and SHALL return a handle
  already resolved to `skipped_on_replay`, carrying the given id or an empty id.
- `flz_core_spawn_result(ctx, handle, timeout_ms, out)` SHALL answer pending, or the resolved result
  carrying the instance id the request resolved to and the outcome (`spawned`, `delivered` or
  `skipped_on_replay`), or the failure, reported the way every other entry reports one: the status
  `FLZ_ERR` and the JSON error document in `flz_core_last_error`, whose code is the
  controller's (`operator.already_running`, `operator.factory_unknown`,
  `operator.factory_not_bounded`, or any other refusal's own code) or the core's
  (`spawn.timeout`, `spawn.unavailable`). A timeout of zero
  SHALL never wait, including while another thread's read of the same handle is waiting. A handle
  read to resolution SHALL answer every later read the same while its activation is outstanding.

The spawn handle, its outcome mapping and the reaper that logs an unread failure SHALL be one
implementation in `flz_core`, which the Rust SDK uses directly and every veneer reaches through
these two entries, so no host keeps an answer of its own. Every handle SHALL be released when the
activation that made it is completed or abandoned: a failure no read collected SHALL be logged once,
by the reaper for a handle still pending, and a later read of it SHALL answer that the handle was
released rather than wait.

#### Scenario: A request returns before the answer
- **WHEN** a host calls `flz_core_spawn_request` for factory `worker` with an empty id and the controller has not answered
- **THEN** the call SHALL return a handle, and `flz_core_spawn_result` with a timeout of zero SHALL answer pending

#### Scenario: A resolved read carries the generated id
- **WHEN** the controller answers that request with `spawned` under a generated id
- **THEN** `flz_core_spawn_result` SHALL answer that id and the outcome `spawned`, and a further read of the same handle SHALL answer the same until its activation completes, after which it SHALL answer that it was released

#### Scenario: A failure nobody read is logged once
- **WHEN** a handle's request is refused and the host completes the activation without reading the handle
- **THEN** the reaper SHALL log the refusal once, naming the instance id (empty when none was given), the factory id and the reason, and a later `flz_core_spawn_result` on that handle SHALL answer that it was released

#### Scenario: An unbounded factory is a named failure through the ABI
- **WHEN** the controller refuses a request with the code `operator.factory_not_bounded`
- **THEN** `flz_core_spawn_result` SHALL report that code, distinct from `operator.factory_unknown`, and every veneer SHALL surface it as the `code` of its one error type

#### Scenario: The blocking spawn entry is gone
- **WHEN** the ABI surface fixture is compared with the exported symbols
- **THEN** it SHALL list `flz_core_spawn_request` and `flz_core_spawn_result` and SHALL NOT list `flz_core_spawn_bounded`

### Requirement: The ABI SHALL carry every failure as the one error document

Every C ABI entry that fails SHALL answer its failing status and SHALL leave in
`flz_core_last_error` the JSON error document `{"error": {"code", "class", "message",
"details", "hint"}}` rendered from the catalogue. No entry SHALL hand a failure out through
a second buffer. Each veneer SHALL parse that document into its SDK's one error type, and
SHALL NOT match on message text.

#### Scenario: One channel for every failure

- **WHEN** an administration call, a spawn result, a class check and a context creation
  each fail through the ABI
- **THEN** each SHALL leave a document in `flz_core_last_error` that parses as JSON with an
  `error.code` found in the catalogue, and no veneer SHALL read a second failure buffer

#### Scenario: A veneer does not scrape text

- **WHEN** the veneers' sources are searched for a match on the text of a core refusal
- **THEN** none SHALL be found; every veneer SHALL branch on `code` or `class`

### Requirement: The ABI SHALL render a refusal a host makes itself

`flz_core_error_document(code, details_json)` SHALL answer the JSON error document the
catalogue renders for `code` with its details filled from `details_json`, borrowed until the
next call on the same thread, and NULL when an argument is not UTF-8 or the details are not a
JSON object. A host that makes a refusal itself SHALL build its one error type from this
document, or from its glue's wrapper of the same renderer, and SHALL NOT hold a copy of the
catalogue. The Java SDK's API module is the one exception: it compiles and tests without the
native module, so it renders from the catalogue packaged into it, held to the shared rendering
fixture.

#### Scenario: A host refusal reads as the core's

- **WHEN** a host asks for `runtime.identity_too_long` with
  `{"part": "tasklet name", "value": "t", "limit": 255}`
- **THEN** the document's message SHALL be `the tasklet name 't' is longer than 255 bytes`

#### Scenario: Details that are not an object render nothing

- **WHEN** a host passes `[]` as the details
- **THEN** the entry SHALL answer NULL

### Requirement: A part is one entry point that never waits

The C ABI SHALL expose a tasklet's part as one entry point,
`flz_core_part(ctx, token, item, payload, payload_len)`, where `token` is the live
activation's token and `item` is the index, within that activation, of the work item whose
invocation sends the part. The entry SHALL number the part, stamp it, apply the drop rules
of `output-parts` and write its frame, and SHALL return without waiting for the controller.
It SHALL answer `FLZ_OK` for a part that left and for a part it dropped, and `FLZ_ERR` with
the error document in `flz_core_last_error` for a refusal (`part.no_sink`,
`part.outside_invocation`, an item index the activation does not hold). The per-item
sequence counters SHALL live with the outstanding activation and SHALL be released with it,
so a token that is no longer outstanding is `part.outside_invocation`.

The entry SHALL be listed in `test/fixtures/abi_surface.txt`, and every host binding
(the JNI, napi, Python and Ruby glue and the PHP declaration) SHALL declare and export it
through its surface table, asserted by that binding's surface test. The Rust SDK SHALL reach
the same function through the crate API. The fake controller SHALL record every `output_part`
frame it receives, so each veneer's tests can assert what a part carried.

#### Scenario: A part returns before the controller answers anything

- **WHEN** a host calls `flz_core_part` for the second item of an outstanding activation
  whose tasklet has a sink
- **THEN** the call SHALL answer `FLZ_OK`, and the fake controller SHALL record one
  `output_part` frame carrying `flz.part` `0`, that item's attempt and its Cell's label

#### Scenario: A completed activation's token is refused

- **WHEN** a host calls `flz_core_part` with a token whose activation it has completed
- **THEN** the call SHALL answer `FLZ_ERR` and `flz_core_last_error` SHALL hold the document
  with code `part.outside_invocation`

#### Scenario: Every binding exports the entry

- **WHEN** the surface tests of the JNI, napi, Python, Ruby and PHP bindings run
- **THEN** each SHALL find `flz_core_part` in its declaration and in its exports, and the
  crate's own test SHALL find it in `test/fixtures/abi_surface.txt`

