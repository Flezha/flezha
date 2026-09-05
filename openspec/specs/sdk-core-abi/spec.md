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
releasing the entity as if it had never been polled. It exists because no unwind
crosses the ABI: a veneer whose host language throws, or whose thread dies, has
no way to reach `complete`, and without an explicit call the entity would be
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

#### Scenario: An abandoned activation releases its entity
- **WHEN** a veneer polls an activation for entity E and calls `flz_core_abandon` with its token instead of completing it
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
points are the poll, the starvation wait, the completion, the workflow receive, the workflow spawn
and the runner start, and such a host SHALL reach them only from threads whose blocking cannot stall
its dispatch.

The completion is what makes this a correctness rule rather than a latency one: under the default
overflow policy it waits until the consumer's mailbox drains, and the consumer drains only when its
handler runs. A host that blocks the one thread able to run that handler cannot recover, because the
inline-drain escape the crate host uses takes a host function pointer and is by construction
unreachable over the ABI.

#### Scenario: Complete from a different thread
- **WHEN** worker A polls an activation and hands it to worker B, which completes it
- **THEN** the completion SHALL be accepted and the entity released, identically to same-thread completion

#### Scenario: The rule is recorded for the next binding
- **WHEN** a new host binding is designed whose concurrency primitive is bound to one thread
- **THEN** the ABI documentation SHALL state this constraint and name the completion entry point as the reason

### Requirement: The C ABI cannot lag the crate API

The Rust SDK SHALL consume `flz_core`'s Rust API (the same crate the C ABI wraps), and
CI SHALL drive at least one full runner scenario through `flz_core_ffi` itself. A
capability reachable through the crate API but not through the C ABI is a defect of
this capability, not a backlog item.

Every host language binding SHALL be built from this crate and SHALL be a one-to-one veneer over its
entry points, including a binding added under its own feature on the crate. A binding SHALL NOT
re-express dispatch, ordering, overflow, the retry ladder, durability or the workflow driver, SHALL
NOT reach a kernel capability the C ABI does not expose, and SHALL NOT alter the artifacts the other
bindings consume when its feature is enabled.

#### Scenario: FFI parity is tested, not assumed
- **WHEN** the SDK test suite runs in CI
- **THEN** it SHALL include a scenario that connects, processes input activations, fires a timer, and shuts down entirely through the C ABI, failing if any required entry point is missing

#### Scenario: A binding adds no ABI surface
- **WHEN** a host binding's exported surface is compared with the C ABI's
- **THEN** each export SHALL correspond to an existing entry point or SHALL reach none at all, and the comparison SHALL be asserted rather than reviewed

#### Scenario: A binding's feature is additive
- **WHEN** the crate is built with and without a binding's feature
- **THEN** the existing artifacts SHALL be unchanged and the other bindings' exports SHALL be unaffected

### Requirement: The declaration surface covers the full host layer

`flz_core_declare_class` SHALL accept the complete host-class declaration:
entity mode (single/static with keys, dynamic with TTL and cap), clock
kind, correction window and beyond-window policy, delivery tier
(effectively-once, checkpoint ack-granularity), inter-procedure channels
with edge ids and keying markers, per-channel retry and overflow
policies, the record-header lang/format bytes, the declaring SDK's own
version, persistence, and workflow
lifecycle facts (loop mode, rollback mode, retry, wake-after,
compensable procs). Timer caps stay host-layer policy — in both SDKs the
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
in the operative's declaration metadata: it describes the writing runtime, not the
class, and a shared golden snapshot of that metadata could not hold a value that
differs per SDK.

#### Scenario: A full declaration round-trips into controller metadata
- **WHEN** a shim declares a checkpoint-ack, correction-windowed, dynamic-mode class through the ABI and the controller sends `AddOperative`
- **THEN** the `AddOperativeResponse` metadata SHALL carry the same declarations the typed Rust path would produce for an identical class

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
- **WHEN** an Entity of that class writes a durable record and the controller reads the operative's declaration metadata
- **THEN** bytes 1:5 of the record SHALL carry the declared version, and the declaration metadata SHALL NOT carry it

#### Scenario: The advertisement reads the declared value from the core
- **GIVEN** a veneer that declared its classes with `sdk_lang = 2`
- **WHEN** the runner builds its connect init frame and any later `UpdateFactories`
- **THEN** each factory entry SHALL carry the language declared for the class registered under that id, resolved from the core rather than from a constant in the veneer

### Requirement: Widened host capabilities are reachable through the ABI

Every capability the widened host layer supports SHALL be exercisable
through `flz_core_ffi` alone — the five command activation kinds, the
prepare crossing, entity modes, tiers, timers, inter-procedure routing,
and workflow lifecycles. The CI scenario suite SHALL grow to drive at least:
a restore fold, a live state read, a keyed dynamic route via the prepare
crossing, an effectively-once completion with offsets, and a workflow
run-sugar lifecycle — all through the C entry points, failing if any
required surface is missing.

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
- **WHEN** the scenario suite ships a persisted change log for an ABI-declared entity and the controller path delivers `RestoreInitState`
- **THEN** a `RestoreFold` activation SHALL be pollable through `flz_core_poll`, its completion SHALL unblock the entity, and the entity's next input activation SHALL observe the folded state's effects

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

### Requirement: The restore crossing carries each record's stamped schema version

The host restore activation SHALL carry, for the snapshot and for every change
body it crosses, the schema version stamped in that record's header. A host SDK
SHALL NOT be required to parse the 16-byte header itself to learn it.

The core SHALL resolve the version decision (decode / migrate / refuse) before
crossing, so a host SDK receives only items it is expected to decode at its own
version or to migrate from a named `from_version`. A refusal SHALL be produced
by the core and SHALL NOT cross.

The class declaration SHALL carry `schema_version` and
`min_readable_schema_version` per Procedure, not per class.

#### Scenario: a host receives the version it must migrate from

- **GIVEN** a host-run Procedure declaring `schema_version = 2` and `min_readable_schema_version = 1`
- **WHEN** a restore payload carries a snapshot stamped version 1
- **THEN** the crossing SHALL present the snapshot bytes tagged `from_version = 1`, and the host SHALL decode them with its version-1 migration rather than its current codec

#### Scenario: an unreadable record never reaches the host

- **GIVEN** a host-run Procedure declaring `schema_version = 2` and `min_readable_schema_version = 2`
- **WHEN** a restore payload carries a record stamped version 1
- **THEN** the core SHALL fail the restore with a schema error and SHALL NOT cross the record to the host

### Requirement: The C ABI exposes the topology check as a read-only call

The C ABI SHALL expose `flz_core_check_class`, which takes the same declaration record
`flz_core_declare_class` takes and registers nothing: on acceptance it SHALL hand out the
derived declaration as JSON, and on refusal it SHALL hand out the verdict as JSON —
`{"issues":[{"name","text"}, …]}` — so that no SDK parses prose to learn which refusals
fired. The refusal status SHALL still be the failing one and `flz_core_last_error` SHALL
carry the same verdict rendered for a reader, so a caller that ignores the buffer refuses
rather than registers.
`flz_core_declare_class` SHALL be that check followed by registration, and its refusal
text SHALL be the same rendering so a suite asserting a substring of one refusal still
holds. The record SHALL carry each channel's ordered chains and the raw checkpoint flags,
and the key set SHALL be pinned by `test/fixtures/host_class_decl_keys.txt` as today.

#### Scenario: A test asks for the verdict without registering

- **WHEN** an SDK's conformance run submits a fixture row's record to `flz_core_check_class`
- **THEN** it receives the derived declaration for a well-formed row, or a verdict naming
  every refusal for a malformed one, and no class is registered

#### Scenario: A stale builder is refused, not degraded

- **WHEN** a builder still sends `channel.consumers` or `class.ack_granularity`
- **THEN** the core refuses the declaration at `deny_unknown_fields` naming the key

