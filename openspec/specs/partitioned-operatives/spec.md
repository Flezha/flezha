# partitioned-operatives Specification

## Purpose
Run one logical stream Operative across N machines. `--partitions N` starts it
as N ordinary operative instances, each owning a contiguous range of the 256
key groups a routing key hashes into; the group is a view derived from the
members' own rows, never a stored record. Every mis-routing failure mode is
loud and named — a key a member does not own parks on its first frame, a
keyless class is refused, and coverage gaps surface as warnings rather than as
quietly missing data.

## Requirements

### Requirement: A partitioned start SHALL materialize N ordinary operative instances
`flz operative start <id> --partitions N` (N ≥ 2, stream kind only) SHALL start N ordinary operative instances named `<id>#p0` … `<id>#p{N-1}`, each a full operative — own Overwatch, own ProcessHub child and registry row, own runner binding, own input log, own state tree, own plugins, own DLQ identity. Each member's durable cspec opts SHALL carry `group` (the base id), `partition`, `partitions`, and its owned key-group range. There SHALL be no stored group record: every group-level view SHALL be derived from member rows at read time. `partitions: 1` (the default) SHALL be byte-identical to an unpartitioned start.

#### Scenario: Start creates self-describing members
- **WHEN** `flz operative start ohlc <factory> --partitions 3` succeeds
- **THEN** registry rows `ohlc#p0`, `ohlc#p1`, `ohlc#p2` exist, each with cspec opts `group: "ohlc"`, its `partition`, `partitions: 3`, and a key-group range, and each member is bound and running as an ordinary operative

#### Scenario: Default is unchanged
- **WHEN** an operative is started without `--partitions` (or with `--partitions 1`)
- **THEN** exactly one instance starts with no partition opts and behavior is identical to before this change

#### Scenario: Workflow kind is refused
- **WHEN** `--partitions 2` is given with `--kind workflow`
- **THEN** the start is refused with an error naming that partitions apply to stream operatives only, and nothing is started

#### Scenario: Id budget is checked before any member starts
- **WHEN** `byte_size(base_id) + byte_size("#p" <> Integer.to_string(N-1))` exceeds 32 bytes
- **THEN** the start is refused up front naming the 32-byte limit and the maximum base length for this N, and no member is started

#### Scenario: Quota is preflighted for N units
- **WHEN** the namespace's `max_operatives` quota cannot admit N more operatives
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

### Requirement: Partition identity SHALL reach the SDK via additive AddOperative fields
`AddOperative` SHALL carry `partition_index`, `partition_count`, `key_group_from`, `key_group_to` as additive proto fields whose zero values mean "unpartitioned". Partition identity SHALL NOT ride `init_values` (its stream-side namespace is owned by Procedure names). The migrate-time AddOperative SHALL carry the same fields, closing the existing opts-drop on `Lifecycle.migrate_operative/1` for partition identity.

#### Scenario: A partition member's bind carries its range
- **WHEN** `ohlc#p1` of 3 binds to a runner
- **THEN** its AddOperative carries `partition_index: 1`, `partition_count: 3`, and its key-group range

#### Scenario: An unpartitioned bind is byte-identical
- **WHEN** an operative with no partition opts binds
- **THEN** the new fields are absent/zero and the encoded AddOperative is byte-identical to before this change

#### Scenario: Migration preserves the guard
- **WHEN** a partition member is migrated to another runner
- **THEN** the migrate-time AddOperative carries the same partition fields and the guard re-arms on the new runner

### Requirement: The SDK SHALL refuse a mis-routed key on its first frame
When `partition_count ≥ 2`, the SDK dispatch SHALL check that each computed entity key hashes into the partition's own key-group range. A miss SHALL take the park path with an error naming the key, its group, the owning partition, and the contract ("the routing item / flz.key must equal the key_by result") — before any state for that key is created or modified. When `partition_count < 2` the check SHALL cost no more than one comparison.

#### Scenario: Mis-routed frame parks loudly
- **WHEN** a frame whose key hashes to a group outside the member's range is dispatched
- **THEN** the input is parked with the named contract error and no entity state is created or modified for it

#### Scenario: Correctly routed frames are unaffected
- **WHEN** every arriving key hashes into the member's range
- **THEN** dispatch behavior and output are identical to an unpartitioned operative

### Requirement: Partitioning SHALL be refused for a keyless topology
A class with no keyed procedure (neither `key_by` nor declared entity keys on any entry-fed procedure) SHALL NOT run partitioned: splitting a single unkeyed entity across N members would fold each member's input slice into a private copy of "the one" state — N divergent partial truths, silently. Because the topology is SDK-owned (key closures are compiled code, never on the wire), the authoritative refusal SHALL live in the SDK at the AddOperative boundary: when `partition_count ≥ 2` and the topology has no keyed procedure, the SDK SHALL refuse the AddOperative with an error naming `key_by` (`PartitionsWithoutKeyedProcedure`). The controller SHALL treat that refusal as a member start failure (group rollback, error surfaced to the caller). Additionally, the class SHALL declare its keyedness via the existing `AddOperativeResponse.metadata` → `ClassRegistry` declaration seam, so a later partitioned start of a known-keyless class is refused at the controller before any member starts. A statically keyed topology (declared entity keys, no `key_by`) SHALL be accepted; members' out-of-range static entities simply receive no input.

#### Scenario: First partitioned start of a keyless class fails at bind, loudly
- **WHEN** `--partitions 3` is used with a class whose procedures declare no keys and no `key_by`
- **THEN** the first member's AddOperative is refused with `PartitionsWithoutKeyedProcedure`, any already-started members are stopped, and the CLI receives the SDK's error naming `key_by`

#### Scenario: Repeat start of a known-keyless class fails immediately
- **WHEN** the same class has bound before (its keyedness declaration is in the ClassRegistry) and a partitioned start is attempted
- **THEN** the controller refuses before starting any member, with the same named error

#### Scenario: Statically keyed classes partition normally
- **WHEN** a class declares static entity keys and no `key_by` and is started with `--partitions 2`
- **THEN** the start succeeds and each member serves only the declared keys whose groups fall in its range

### Requirement: Group plugin configs SHALL fan out per member with partition opts injected
A `--plugins` config given at group start SHALL be decoded once per member with that member's operative id, and `partition_index`/`partition_count`/`key_group_from`/`key_group_to` SHALL be merged into each plugin's opts as integers. Providers consuming these opts SHALL declare them as known opts. Fanned-out configs SHALL leave `plugin_id` unset so the per-source default keeps worker registry keys unique across members.

#### Scenario: Each member's plugin knows its partition
- **WHEN** a group of 3 starts with a websocket plugin config
- **THEN** each member's plugin instance receives its own `partition_index` (0, 1, 2) and `partition_count: 3` in opts, persisted in that member's cspec

### Requirement: Runtime plugin enable and disable SHALL fan out over the group
`flz plugin enable <group> ...` against a partition group SHALL run the existing per-operative enable once per member, injecting that member's partition opts and suffixing the supplied plugin id with the member's `#pK` (preserving node-global plugin-id uniqueness). `flz plugin disable <group> <plugin_id>` SHALL disable the corresponding suffixed instance on every member. Enable/disable against a single member id keeps today's behavior unchanged.

#### Scenario: Group enable reaches every member with its partition
- **WHEN** `flz plugin enable ohlc --provider operative_websocket --plugin-id ohlc_ws ...` targets a 3-member group
- **THEN** members `ohlc#p0..#p2` each get an enabled instance (`ohlc_ws#p0..#p2`) whose opts carry that member's partition fields, each persisted cspec-first via the existing enable path

#### Scenario: Group disable is symmetric
- **WHEN** `flz plugin disable ohlc ohlc_ws` is issued
- **THEN** all three suffixed instances are disabled and their workers stopped

#### Scenario: A partial group enable is loud
- **WHEN** enabling on member k of N fails
- **THEN** the command reports which members succeeded and which failed by name — never a silent partial enable

### Requirement: The CLI SHALL present the group as one logical operative
`flz operative list` SHALL aggregate members (by the `group` opt) into one row showing partition count, health, and per-member placement. `flz operative info <group>` SHALL show per-member detail and SHALL validate coverage (every key group owned exactly once), rendering gaps or overlaps as loud warnings. `flz operative stop <group>` SHALL stop all members. `flz state get <group>/<procedure>/<key>` SHALL resolve the owning member by key hash and answer through the existing state-get path; procedure-level and selector-less reads SHALL fan out over members and merge.

#### Scenario: List shows one row
- **WHEN** `ohlc` runs with 3 partitions
- **THEN** `flz operative list` shows a single `ohlc` row with `partitions 3/3` and per-member runner placement, and `flz operative list` for unpartitioned operatives is unchanged

#### Scenario: Coverage violation is loud
- **WHEN** a member row is missing or two members' ranges overlap
- **THEN** `flz operative info` reports the exact gap/overlap as a warning rather than silently showing partial data

#### Scenario: Keyed state read resolves the owner
- **WHEN** `flz state get ohlc/<procedure>/BTC-USD` is issued
- **THEN** the controller hashes the key, routes to the member owning that group, and returns that member's live entity state

### Requirement: One partition's entire state SHALL live on one controller node
A partition, being one ordinary operative instance, SHALL keep its whole durable state (input log, entity WALs, snapshots, DLQ entries) on the controller node that owns it. No mechanism introduced by this capability may split one member's entities across controller nodes.

#### Scenario: Partition placement moves state atomically per member
- **WHEN** ProcessHub migrates a member to another controller node
- **THEN** the member's entire state tree moves with it via the existing per-operative migration paths, exactly as for any operative today
