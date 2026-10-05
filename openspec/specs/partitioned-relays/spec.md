# partitioned-relays Specification

## Purpose
Run one logical flow Relay across N machines. `--partitions N` starts it
as N ordinary relay instances, each owning a contiguous range of the 256
key groups a routing key hashes into; the group is a view derived from the
members' own rows, never a stored record. Every mis-routing failure mode is
loud and named — a key a member does not own parks on its first frame, a
keyless class is refused, and coverage gaps surface as warnings rather than as
quietly missing data.
## Requirements
### Requirement: A partitioned start SHALL materialize N ordinary relay instances

`flz relay start <factory> --id <id> --partitions N` (N ≥ 2) SHALL start N ordinary relay
instances named `<id>#p0` … `<id>#p{N-1}`, each a full relay of the factory's kind: own
Overwatch, own ProcessHub child and registry row, own runner binding, own input log, own state
tree, own plugins, own DLQ identity, and, for a steps relay, own journal and own pass sheet. Each
member's durable cspec opts SHALL carry `group` (the base id), `partition`, `partitions`, and its
owned key-group range. There SHALL be no stored group record: every group-level view SHALL be
derived from member rows at read time. `partitions: 1` (the default) SHALL be byte-identical to
an unpartitioned start. The group SHALL be started, converged, read and rescaled through the one
group module for either kind; a steps run SHALL add no group mechanism of its own. A
partitioned start SHALL NOT carry a first message (`--text`): a message names one instance, and a
group's message reaches its member by key; such a start SHALL be refused before any member
starts, naming the keyed forward and the member id as the ways in.

A partitioned start with no `--id` SHALL generate the base id, trimming the factory prefix so that
the largest member id, `<base>#p{N-1}`, still fits the relay id limit, and SHALL report that
base as the started id. A generated base is never refused on the id budget.

#### Scenario: Start creates self-describing members

- **WHEN** `flz relay start <factory> --id ohlc --partitions 3` succeeds
- **THEN** registry rows `ohlc#p0`, `ohlc#p1`, `ohlc#p2` exist, each with cspec opts `group: "ohlc"`, its `partition`, `partitions: 3`, and a key-group range, and each member is bound and running as an ordinary relay

#### Scenario: A per-input steps starts as N instances

- **WHEN** `flz relay start <factory> --id orders --partitions 2` names a `per_input`
  steps factory
- **THEN** `orders#p0` and `orders#p1` exist as ordinary steps instances, each with its own
  journal at its own state key, its own durable input log and its own key-group range, bound and
  waiting for a message, and no record of the group exists beyond the two rows

#### Scenario: A partitioned start without an id generates a base that fits

- **WHEN** `flz relay start <factory> --partitions 12` succeeds with no `--id`, for a factory id long enough that its untrimmed prefix would not fit
- **THEN** a generated base is reported as the started id, members `<base>#p0` … `<base>#p11` exist, and `<base>#p11` is within the relay id limit

#### Scenario: Default is unchanged

- **WHEN** a relay is started without `--partitions` (or with `--partitions 1`)
- **THEN** exactly one instance starts with no partition opts and behavior is identical to an unpartitioned start

#### Scenario: A once or chained steps relay is refused partitions

- **WHEN** `--partitions 2` names a steps factory whose loop mode is `once` or `chained`
- **THEN** the start is refused with `topology.partitions_without_per_input_loop`, naming that its one
  message would reach one member while the others waited for nothing, at the controller when the
  class's loop mode is already recorded and at the first member's bind otherwise, and no member
  is left running

#### Scenario: A first message is refused with partitions

- **WHEN** `flz relay start <factory> --partitions 2 --text '{"symbol":"AAPL"}'` is run
- **THEN** the start is refused before any member starts, naming that a group's message reaches
  a member by key, and nothing is started

#### Scenario: Id budget is checked before any member starts

- **WHEN** a caller-given `base_id` makes `byte_size(base_id) + byte_size("#p" <> Integer.to_string(N-1))` exceed 32 bytes
- **THEN** the start is refused up front naming the 32-byte limit and the maximum base length for this N, and no member is started

#### Scenario: Quota is preflighted for N units

- **WHEN** the namespace's `max_relays` quota cannot admit N more relays
- **THEN** the group start is refused before the first member starts

#### Scenario: A mid-group member failure does not leave a silent half-group

- **WHEN** member k of N fails to start
- **THEN** already-started members are stopped (best effort) and the returned error names the failed member and its reason

#### Scenario: More partitions than runners warns loudly but proceeds

- **WHEN** `--partitions N` exceeds the namespace's active runner count
- **THEN** the start proceeds and the CLI emits a loud warning naming both counts and how many members will share machines

### Requirement: Key groups SHALL be assigned by a cross-language-pinned hash with explicit contiguous ranges
The partition function SHALL be `group(key) = fnv1a128(key_bytes) mod 256`, using the FNV-1a-128 constants already frozen by the SDKs' `StableId` (offset basis `0x6c62272e07bb014262b821756295c58d`, prime `2^88 + 0x13b`). The controller SHALL gain an Elixir implementation pinned against the same fixture vectors as the existing Rust and Java tests. Assignment SHALL be contiguous ranges computed at start (even split), and the owned range SHALL be stored and shipped explicitly — consumers SHALL never re-derive it from `(partition, partitions)`.

#### Scenario: Three implementations agree
- **WHEN** the Elixir, Rust, and Java FNV-1a-128 implementations hash the shared fixture inputs
- **THEN** all three produce the identical fixture digests

#### Scenario: Ranges cover the space exactly once
- **WHEN** a group of N members is started
- **THEN** the members' ranges partition `0..255` with no gap and no overlap

### Requirement: Partition identity SHALL reach the SDK via additive AddRelay fields
`AddRelay` SHALL carry `partition_index`, `partition_count`, `key_group_from`, `key_group_to` as additive proto fields whose zero values mean "unpartitioned". Partition identity SHALL NOT ride `init_values` (its stream-side namespace is owned by Tasklet names). The migrate-time AddRelay SHALL carry the same fields, closing the existing opts-drop on `Lifecycle.migrate_relay/1` for partition identity.

#### Scenario: A partition member's bind carries its range
- **WHEN** `ohlc#p1` of 3 binds to a runner
- **THEN** its AddRelay carries `partition_index: 1`, `partition_count: 3`, and its key-group range

#### Scenario: An unpartitioned bind is byte-identical
- **WHEN** a relay with no partition opts binds
- **THEN** the new fields are absent/zero and the encoded AddRelay is byte-identical to before this change

#### Scenario: Migration preserves the guard
- **WHEN** a partition member is migrated to another runner
- **THEN** the migrate-time AddRelay carries the same partition fields and the guard re-arms on the new runner

### Requirement: The SDK SHALL refuse a mis-routed key on its first frame
When `partition_count ≥ 2`, the SDK dispatch SHALL check that each computed cell key hashes into the partition's own key-group range. A miss SHALL take the park path with an error naming the key, its group, the owning partition, and the contract ("the routing item / flz.key must equal the key_by result") — before any state for that key is created or modified. When `partition_count < 2` the check SHALL cost no more than one comparison. The check SHALL be the one the router applies where a value is addressed to a Cell, for an instance of either kind: on a steps relay member it runs for the entry stage and for every inner keyed stage alike, and a message none of whose entry items it admits SHALL open no pass and SHALL NOT count as a message, exactly as a message the entrance cannot read does not.

#### Scenario: Mis-routed frame parks loudly
- **WHEN** a frame whose key hashes to a group outside the member's range is dispatched
- **THEN** the input is parked with the named contract error and no cell state is created or modified for it

#### Scenario: Correctly routed frames are unaffected
- **WHEN** every arriving key hashes into the member's range
- **THEN** dispatch behavior and output are identical to an unpartitioned relay

#### Scenario: A mis-routed message to a steps relay member opens no pass
- **WHEN** a member of a partitioned `per_input` steps relay receives a message whose `key_by` result hashes outside its range
- **THEN** the message is parked under the entry channel with the named contract error, no Cell is minted for its key, no pass opens, no iteration commits, and the member's next in-range message runs its pass as the next iteration

### Requirement: Partitioning SHALL be refused for a keyless topology
A class with no keyed tasklet (neither `key_by` nor declared cell keys on any entry-fed tasklet) SHALL NOT run partitioned: splitting a single unkeyed cell across N members would fold each member's input slice into a private copy of "the one" state — N divergent partial truths, silently. Because the topology is SDK-owned (key closures are compiled code, never on the wire), the authoritative refusal SHALL live in the SDK at the AddRelay boundary: when `partition_count ≥ 2` and the topology has no keyed tasklet, the SDK SHALL refuse the AddRelay with an error naming `key_by` (`topology.partitions_without_keyed_tasklet`). The controller SHALL treat that refusal as a member start failure (group rollback, error surfaced to the caller). Additionally, the class SHALL declare its keyedness via the existing `AddRelayResponse.metadata` → `ClassRegistry` declaration seam, so a later partitioned start of a known-keyless class is refused at the controller before any member starts. A statically keyed topology (declared cell keys, no `key_by`) SHALL be accepted; members' out-of-range static cells simply receive no input.

#### Scenario: First partitioned start of a keyless class fails at bind, loudly
- **WHEN** `--partitions 3` is used with a class whose tasklets declare no keys and no `key_by`
- **THEN** the first member's AddRelay is refused with `topology.partitions_without_keyed_tasklet`, any already-started members are stopped, and the CLI receives the SDK's error naming `key_by`

#### Scenario: Repeat start of a known-keyless class fails immediately
- **WHEN** the same class has bound before (its keyedness declaration is in the ClassRegistry) and a partitioned start is attempted
- **THEN** the controller refuses before starting any member, with the same named error

#### Scenario: Statically keyed classes partition normally
- **WHEN** a class declares static cell keys and no `key_by` and is started with `--partitions 2`
- **THEN** the start succeeds and each member serves only the declared keys whose groups fall in its range

### Requirement: Group plugin configs SHALL fan out per member with partition opts injected

A `--plugins` config given at group start SHALL be decoded once per member with that member's relay id, and `partition_index`/`partition_count`/`key_group_from`/`key_group_to` SHALL be merged into each plugin's opts as integers. Because that merge is unconditional and provider-independent — every plugin of every member receives all four, whether or not it reads one — the four keys SHALL be allowed by every provider's strict option check, declared once in the host-injected list rather than in per-provider copies. A provider that CONSUMES one SHALL additionally declare it as its own field; a provider that merely receives them SHALL NOT be required to declare anything, and SHALL NOT gain a struct field it never populates. Fanned-out configs SHALL leave `plugin_id` unset so the per-source default keeps worker registry keys unique across members.

#### Scenario: Each member's plugin knows its partition

- **WHEN** a group of 3 starts with a websocket plugin config
- **THEN** each member's plugin instance receives its own `partition_index` (0, 1, 2) and `partition_count: 3` in opts, persisted in that member's cspec

#### Scenario: A provider that reads none of them still enables

- **WHEN** a group starts with a plugin whose provider is strict about unknown options and consumes no partition opt
- **THEN** the member's plugin enables, and the injected four are neither refused nor materialised as struct fields

### Requirement: Runtime plugin enable and disable SHALL fan out over the group

`flz plugin enable <group> ...` against a partition group SHALL run the existing per-relay enable once per member, injecting that member's partition opts and suffixing the supplied plugin id with the member's `#pK` (preserving node-global plugin-id uniqueness). A member whose suffixed instance is already registered against that same member SHALL count as success, not failure, and the command SHALL report enabled and already-enabled member counts distinctly; a group whose members are all already enabled SHALL exit 0 as a no-op. `flz plugin disable <group> <plugin_id>` SHALL disable the corresponding suffixed instance on every member. Enable/disable against a single member id keeps today's behavior unchanged (including the single-plugin idempotent-enable verdict).

#### Scenario: Group enable reaches every member with its partition

- **WHEN** `flz plugin enable ohlc --provider relay_websocket --plugin-id ohlc_ws ...` targets a 3-member group
- **THEN** members `ohlc#p0..#p2` each get an enabled instance (`ohlc_ws#p0..#p2`) whose opts carry that member's partition fields, each persisted cspec-first via the existing enable path

#### Scenario: Group disable is symmetric

- **WHEN** `flz plugin disable ohlc ohlc_ws` is issued
- **THEN** all three suffixed instances are disabled and their workers stopped

#### Scenario: A partial group enable is loud

- **WHEN** enabling on member k of N genuinely fails (any reason other than already-enabled on the same member)
- **THEN** the command reports which members succeeded and which failed by name — never a silent partial enable

#### Scenario: Re-running a group enable after a partial recovery is a heal

- **WHEN** two of three members already carry the plugin and the same group enable is re-run
- **THEN** the missing member's instance is enabled, the two existing instances are untouched, and the command exits 0 reporting one enabled and two already enabled

### Requirement: The CLI SHALL present the group as one logical relay
`flz relay list` SHALL aggregate members (by the `group` opt) into one row showing partition count, health, and per-member placement. `flz relay info <group>` SHALL show per-member detail and SHALL validate coverage (every key group owned exactly once), rendering gaps or overlaps as loud warnings. `flz relay stop <group>` SHALL stop all members, each the way its kind is stopped: a steps relay member cooperatively, ending `cancelled` with its own completion record. `flz state get <group>/<tasklet>/<key>` SHALL resolve the owning member by key hash and answer through the existing state-get path; tasklet-level and selector-less reads SHALL fan out over members and merge. Everything a steps instance records for itself, its completion record, its journal, its input log and its rewind, SHALL be per member and addressed by the member id; a member that ended itself SHALL appear in `info` as a finished steps relay under its member id, and the group SHALL show the coverage gap it left until a converge restarts it.

#### Scenario: List shows one row
- **WHEN** `ohlc` runs with 3 partitions
- **THEN** `flz relay list` shows a single `ohlc` row with `partitions 3/3` and per-member runner placement, and `flz relay list` for unpartitioned relays is unchanged

#### Scenario: Coverage violation is loud
- **WHEN** a member row is missing or two members' ranges overlap
- **THEN** `flz relay info` reports the exact gap/overlap as a warning rather than silently showing partial data

#### Scenario: Keyed state read resolves the owner
- **WHEN** `flz state get ohlc/<tasklet>/BTC-USD` is issued
- **THEN** the controller hashes the key, routes to the member owning that group, and returns that member's live cell state

#### Scenario: A steps relay member that completed itself is a finished member
- **WHEN** `orders#p1` of a 2-member `per_input` steps relay group ends itself with `ctx.complete` while `orders#p0` keeps running
- **THEN** `flz relay info` lists `orders#p1` as a finished steps relay with its status and result under its own id, reports the group as `1/2` with the gap over `orders#p1`'s key groups, and `orders#p0` is untouched

#### Scenario: Stopping a steps relay group stops each member cooperatively
- **WHEN** `flz relay stop orders` is issued against a 2-member `per_input` steps relay group
- **THEN** each member receives its own cooperative stop, each ends `cancelled` with its own completion record, and no group-level record is written

### Requirement: One partition's entire state SHALL live on one controller node
A partition, being one ordinary relay instance, SHALL keep its whole durable state (input log, cell WALs, snapshots, DLQ entries) on the controller node that owns it. No mechanism introduced by this capability may split one member's cells across controller nodes.

#### Scenario: Partition placement moves state atomically per member
- **WHEN** ProcessHub migrates a member to another controller node
- **THEN** the member's entire state tree moves with it via the existing per-relay migration paths, exactly as for any relay today

### Requirement: A partitioned start over an existing group SHALL converge to N by starting only the missing members

When `flz relay start <factory> --id <id> --partitions N` targets an id whose group already has live members, the start SHALL converge instead of refusing: it SHALL start only the members whose indices are absent, iff every surviving member's cspec-declared identity matches the request: declared `partitions` equals N AND the stored range equals the recomputed `Partition.ranges(N)` slice for that member's index. On any mismatch the command SHALL refuse before starting anything, naming the mismatching member, its declared partition count, and `flz relay scale` as the remedy. Converge SHALL never change N, because changing N moves Cell state and start never does. A newly-started missing member SHALL receive the fan-out of the supplied `--plugins` config exactly as a fresh group start would; surviving members and their live plugin sets SHALL NOT be touched. A missing member racing into existence mid-run (concurrent converge, reconcile, or Convergence bind) SHALL be treated as already present, not as a failure. The command SHALL report started and already-running member ids distinctly; a fully-present group SHALL degenerate to the single-start no-op (exit 0, nothing started). Quota SHALL be preflighted on the missing count only. A fresh start (no live members) SHALL behave byte-identically to a start of a group that never existed, including the all-or-nothing rollback, which in every mode stops only members the current run started.

Converging needs the group's id. A partitioned start with no `--id` generates a base that names no existing group, so it SHALL always materialize a fresh group and SHALL never converge one.

#### Scenario: An under-membered group is healed by re-running the start

- **WHEN** a 3-member group has live members `demo#p0` and `demo#p1` (declared `partitions: 3`) and `flz relay start <factory> --id demo --partitions 3` is re-run
- **THEN** only `demo#p2` is started, it restores its Cell state from its on-disk WAL at bind, the two survivors observe no stop or rebind, and the command exits 0 reporting one started and two already-running members

#### Scenario: A fully-present group re-run is a quiet no-op

- **WHEN** all N members are live and the same start is re-run
- **THEN** the command exits 0, nothing is started or stopped, and the output states the group is already running at N

#### Scenario: A start without an id never converges an existing group

- **WHEN** a 3-member group `demo` is live and `flz relay start <factory> --partitions 3` is run with no `--id`
- **THEN** a new group under a generated base is started, and no member of `demo` is started, stopped or rebound

#### Scenario: An N mismatch refuses toward scale

- **WHEN** `--partitions 4` targets a group whose survivors declare `partitions: 3`
- **THEN** the command refuses before starting any member, naming the declared count and `flz relay scale`

#### Scenario: A real member failure during converge rolls back only this run's starts

- **WHEN** converge starts two missing members and the second fails for a non-duplicate reason
- **THEN** the first (started by this run) is stopped, the pre-existing survivors are untouched, and the error names the failed member and its reason

### Requirement: Group-base and singleton id collisions SHALL be refused in both directions

A plain (unpartitioned) start of an id that names an existing group's base SHALL be refused, naming the group's member count and `--partitions N` as the way to address it: the base id itself is unclaimed (only member ids are), so without the guard a rogue singleton starts beside the group. Symmetrically, a partitioned start of an id currently held by a live unpartitioned relay SHALL be refused before any member starts. The plain-start guard SHALL live in the single start-authority seam so every starter path (CLI, schedules, tasklet-requested spawns, routed starts) is covered. A generated id that meets either guard SHALL be drawn again rather than refused. A tasklet-requested spawn and a schedule carry no key and SHALL therefore address an unpartitioned instance only: a spawn naming a group's base id finds no instance under it, takes the start path and is refused by this guard, and the requester's result SHALL be `refused` with that sentence; a tasklet feeds a partitioned steps relay through a keyed output forwarded by `relay_forward`, which resolves the member from the group's ranges.

#### Scenario: A plain start of a group base is refused

- **WHEN** group `demo` has live members and `flz relay start <factory> --id demo` is issued without `--partitions`
- **THEN** the start is refused naming the existing group and `--partitions`, and no singleton is started

#### Scenario: A partitioned start over a live singleton is refused

- **WHEN** a live unpartitioned relay holds `demo` and `flz relay start <factory> --id demo --partitions 3` is issued
- **THEN** the start is refused before any member starts

#### Scenario: A spawn naming a group's base is refused

- **WHEN** a tasklet calls `ctx.spawn_steps` with instance id `demo`, `if_running: deliver` and a payload, while `demo` is a live partition group of a `per_input` steps relay
- **THEN** no instance starts, nothing is delivered to any member, and the requester's async result is `refused` with the sentence naming the group's member count

#### Scenario: A keyed forward reaches the member

- **WHEN** a flow relay's keyed output is forwarded by `relay_forward` to the group `demo` and emits a value whose `flz.key` hashes into `demo#p1`'s range
- **THEN** `demo#p1` receives the message and runs its pass over it, and no other member receives it

### Requirement: Partitioning SHALL be refused for a steps relay that reads one message

A steps class whose loop mode is `once` or `chained` SHALL NOT run partitioned: its one
message reaches one member, and the other members would wait for a message that never comes.
Only a `per_input` steps relay, one pass per message, is a group. Because the loop mode is a class
declaration the topology carries and the controller never sees at start, the authoritative
refusal SHALL live in the SDK at the `AddRelay` boundary: when `partition_count >= 2` names a
steps class whose loop mode is not `per_input`, the SDK SHALL refuse the `AddRelay` with an
error named `topology.partitions_without_per_input_loop` that names the class, its loop mode and `each
per_input` as the remedy, and the controller SHALL treat that refusal as a member start failure
(group rollback, error surfaced to the caller). Additionally, a steps class SHALL declare its
loop mode as `each` (`once` | `per_input` | `chained`) in the `AddRelayResponse.metadata`
declaration seam beside `keyed`, recorded against the factory as the keyless fact is, so a later
partitioned start of a known `once` or `chained` class is refused at the controller before any
member starts, with the same sentence. The keyless refusal applies to a steps relay unchanged: a
`per_input` steps relay whose entry-fed tasklet declares neither `key_by` nor cell keys SHALL be
refused with `topology.partitions_without_keyed_tasklet`.

#### Scenario: First partitioned start of a once steps relay fails at bind, loudly

- **WHEN** `--partitions 3` is used with a steps class whose loop mode is `once` and that has
  never bound
- **THEN** the first member's `AddRelay` is refused with `topology.partitions_without_per_input_loop`, any
  already-started members are stopped, and the CLI receives the SDK's error naming the class,
  `once` and `each per_input`

#### Scenario: Repeat partitioned start of a known chained steps relay fails immediately

- **WHEN** a `chained` steps class has bound before, so its `each` declaration is
  recorded against its factory, and a partitioned start of it is attempted
- **THEN** the controller refuses before starting any member, with the same named error

#### Scenario: A per-input steps relay with a keyed entry partitions normally

- **WHEN** a `per_input` steps relay whose entry edge declares `key_by` is started with
  `--partitions 2`
- **THEN** the start succeeds, both members bind, and each member's response metadata carries
  `each: "per_input"`

#### Scenario: A per-input steps relay with a single-cell entry is refused as keyless

- **WHEN** a `per_input` steps relay whose entry-fed tasklet declares no `key_by` and no cell keys is
  started with `--partitions 2`
- **THEN** the start is refused with `topology.partitions_without_keyed_tasklet`, as a flow relay's would be

