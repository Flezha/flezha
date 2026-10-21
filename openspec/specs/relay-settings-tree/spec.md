# relay-settings-tree Specification

## Purpose
The relay-settings-tree capability is the one settings tree every relay class is configured by in every SDK: how it runs (`run`: `flow` or `steps`), where its work is kept (`durable`), the refusals the tree leaves, the layer each setting lives in, and how the wire carries it.
## Requirements
### Requirement: Every SDK SHALL offer one relay class configured by one settings tree

Every SDK SHALL offer exactly one relay authoring base, and SHALL offer no class,
trait, macro or factory function that names a kind. The base SHALL take one settings tree:
`factory_id` (required), `description`, `max_duration_ms`, `run` and `durable`. `run` SHALL
be one of `flow` (the default) and `steps`. `flow` SHALL carry `exactly`, one of
`at_least_once` (the default) and `effectively_once`, and `effectively_once` SHALL carry
`ack` (`per_input`, the default, or `checkpoint` with `every` and `max_delay_ms`) and
`correction_ms`. `steps` SHALL carry `each` (`once`, the default, `per_input` or `chained`)
and `on_failure` (`undo_all`, the default, or `keep_progress`). `durable` SHALL be one of
`false`, `local` (the default) and `remote`, and `local` and `remote` SHALL carry
`sleep_after`, a duration or `false`, which by default SHALL be `false` for a class that
runs as a flow and six hours for a class that runs in steps. The names are the ones the
change's design table settles; each SDK SHALL spell them in its own idiom and SHALL express
each "one of" as a sum type of its language and each nested setting as a member of its
parent only. The core SHALL be the one judge of a tree an SDK cannot type: such an SDK SHALL forward
the tree as the author wrote it and SHALL NOT parse it itself.

#### Scenario: A class with no settings is a flow relay kept on disk that never sleeps

- **WHEN** a class declares only its factory id and its graph, in any SDK
- **THEN** its registration SHALL announce `run: flow` with `exactly: at_least_once`, `durable: local` with no `sleep_after`, and no `max_duration_ms`

#### Scenario: A steps class that says nothing about sleeping sleeps after six hours

- **WHEN** a class declares `run: steps` and no `sleep_after`, in any SDK
- **THEN** its registration SHALL announce `durable: local` with `sleep_after` of six hours

#### Scenario: A steps class declares its loop shape and failure mode under `steps`

- **WHEN** a class declares `run: steps` with `each: chained` and `on_failure: keep_progress`
- **THEN** its registration SHALL announce a steps run with that loop shape and failure mode, and the same class with `each` or `on_failure` written outside `steps` SHALL fail to compile where the SDK's type system makes the setting unwritable, and otherwise SHALL be refused at registration by the core naming the misplaced setting

#### Scenario: A setting that belongs under another cannot be written elsewhere

- **WHEN** an author writes `sleep_after` under `durable: false`, `ack` or `correction_ms` under `run: steps`, or `each` under `run: flow`, in any SDK
- **THEN** an SDK whose type system makes the setting unwritable SHALL refuse it at compile time, and every other SDK, Elixir included, SHALL forward the tree as written and the core SHALL refuse it at registration with `topology.unknown_field` naming the setting and its parent; no SDK SHALL accept it and ignore it, and no SDK that forwards the tree SHALL keep a parser of it

### Requirement: Invalid combinations SHALL be unwritable, with one stated exception

No SDK and no controller check SHALL refuse a combination of two settings of the tree,
because the tree's shape SHALL leave none to refuse, except one: `exactly: effectively_once`
with `durable: false` SHALL be refused at registration with
`topology.effectively_once_without_persistence`, because the guarantee requires a disk. The
refusal codes `topology.steps_setting_on_flow`, `topology.ambiguous_relay_kind`,
`topology.unknown_relay_kind`, `topology.checkpoint_on_steps`,
`topology.correction_window_on_steps`, `topology.checkpoint_ack_without_tier`,
`topology.correction_without_tier`, `topology.sleep_without_persistence`,
`topology.sync_durability_on_flow` and `topology.sync_durability_without_persistence`
SHALL not exist. Value checks on one setting (a zero window, a zero checkpoint interval, a
checkpoint without a maximum delay, a correction window on a class with no event-time
clock) SHALL stay as they are.

#### Scenario: The retired codes are gone from the catalogue

- **WHEN** `crates/flz_transport/errors.json` and the regenerated `docs/public/errors.md` are read
- **THEN** none of the ten retired codes SHALL be present, and `topology.effectively_once_without_persistence` SHALL be

#### Scenario: The one exception is refused by name

- **WHEN** a class declares `run: {flow, exactly: effectively_once}` and `durable: false`
- **THEN** registration SHALL be refused with `topology.effectively_once_without_persistence`

### Requirement: Each setting SHALL live in exactly one layer

A behaviour of a relay SHALL be declared in its class and nowhere else. `flz relay
start` SHALL take only what makes an instance this instance (`--id`, `--partitions`,
`--init-values`, the first message, the plugins attached at start, the namespace and the
caller's credential), and SHALL take no option that changes how the class behaves. The
environment SHALL carry only where a relay runs and what it costs: <!-- docs-citation-record:begin -->`FLEZHA_PARK_IDLE_MS`
SHALL not exist<!-- docs-citation-record:end -->, and no snapshot interval SHALL be a start option. The one cross-layer check SHALL be a start of a class
whose `durable` is `remote` on a controller whose durability backend is `none`, refused
naming the backend setting.

#### Scenario: A start option that would change behaviour is refused

- **WHEN** `flz relay start echo --snapshot-interval 5000` runs
- **THEN** the CLI SHALL refuse the option as unknown, and `flz relay start --help` SHALL list no such option

#### Scenario: The sleep interval comes from the class

- **WHEN** a class declares `durable: {local, sleep_after: 10 minutes}` and the environment carries no sleep setting
- **THEN** an idle instance SHALL sleep after ten minutes, and `flz relay info` SHALL show the interval under the durability line

#### Scenario: A remote class on a controller without a backend does not start

- **WHEN** a class declares `durable: remote` and the controller runs with `FLEZHA_DURABILITY_BACKEND=none`
- **THEN** `flz relay start` SHALL be refused with a message naming `FLEZHA_DURABILITY_BACKEND`, and no instance SHALL exist

### Requirement: The wire SHALL carry the run and the durability, never a kind

The factory announcement (`UpdateFactories`) SHALL carry each factory's run (`flow`, or
`steps` with `each` and `on_failure`) and `AddRelay` SHALL carry the instance's run and
durability (`memory`, `local` or `remote`, the sleep interval, and for flow the `exactly`
setting with its members). Neither message SHALL carry a `kind` field, and the literals
`steps` and `flow` SHALL be refused by name wherever a run is parsed. The controller
SHALL derive every former kind decision from one predicate on the class record, whether the
instance keeps a task journal, and from the loop shape where the journal code reads it.

#### Scenario: A runner announcing a kind literal is refused

- **WHEN** a runner sends `UpdateFactories` with a factory whose run reads `steps`
- **THEN** the controller SHALL refuse the announcement naming the literal, and the factory SHALL not place

#### Scenario: `flz relay info` reports the run and the durability

- **WHEN** `flz relay info <id>` runs against a steps instance kept locally
- **THEN** its output SHALL show `Run: steps (each: per_input, on_failure: keep_progress)` and `Durable: local (sleep_after: 6h)`, and SHALL show no `Kind` line

### Requirement: Both runs SHALL perform as the two kinds did

`run: flow` SHALL build the dispatcher with no pass sheet and no journal, and the poll core,
routing and cell mailbox code SHALL carry no new branch for the run. `run: steps` SHALL
build the pass sheet and the journal as the steps run did. A flow instance MAY end on a
tasklet's `complete` (draining in-flight work first, as an external stop does) and on
`max_duration_ms`; a steps instance that has no way to end SHALL be ordinary and SHALL not
be refused.

#### Scenario: The benches are unchanged

- **WHEN** `mix bench` and `mix bench.steps` run once before the change and once after, on the same machine and message counts
- **THEN** each reported throughput and p99 SHALL be equal within the harness's stated noise, and the dev note SHALL record both runs

#### Scenario: A flow relay completes from inside

- **WHEN** a tasklet of a flow instance calls `complete` while other cells hold queued work
- **THEN** the instance SHALL run the queued work to its outputs, then end `completed`, and a message sent after that SHALL be refused as to a stopped relay

#### Scenario: A steps class that never ends is accepted

- **WHEN** a class declares `run: {steps, each: per_input}` and no `max_duration_ms`
- **THEN** registration SHALL succeed, and the instance SHALL run until stopped from outside

### Requirement: The words steps and flow SHALL name no relay

The words `steps` and `flow` SHALL appear in no identifier, wire literal, refusal
code, spec directory, spec requirement title or user-visible output that names a
relay, a class, a run or a kind. In prose the words MAY describe input data (a finite or
an endless input) or a mailbox's capacity.

#### Scenario: The sweep is empty

- **WHEN** the repository, its submodules and `openspec/specs/` are searched for `steps` and `flow` outside `openspec/changes/archive/`, `CHANGELOG.md` and the dev notes
- **THEN** every match SHALL be one of the input-data or mailbox senses recorded in the dev note, and no match SHALL name a relay, class, run, kind, refusal code, wire literal or spec directory

### Requirement: `max_duration_ms` SHALL count from the run's first bind

A class's `max_duration_ms` SHALL bound one run of an instance, counted from that run's first
successful bind, for both runs. The controller SHALL record, once per relay, the wall time the
window ends (the first bind's wall time plus the class's `max_duration_ms`), in state every controller of the cluster reads and a move keeps; the record SHALL end with
the run (a stop removes it), so a later start of the same id opens a new window. With every bind
of that run the controller SHALL send the window still left, computed on its own wall clock as it
sends the bind, in `AddRelay.max_duration_remaining_ms`. The runner SHALL arm the stop that far out
on its monotonic clock and SHALL NOT read its own wall clock for it; a remaining window of zero
SHALL end the run at once as an elapsed one does. A sleep and wake, a move to another runner or
controller, a runner restart or a controller restart SHALL NOT restart the window. An `AddRelay`
without the field SHALL arm the class's full window.

#### Scenario: A sleep does not restart the window

- **WHEN** a flow class with `max_duration_ms` 4 000 and `sleep_after` 1 s goes idle, sleeps, and is woken by an input 2 s after its first bind
- **THEN** it SHALL end `cancelled` about 4 s after its first bind, not 4 s after the wake

#### Scenario: A rebind past the window ends the run

- **WHEN** a steps run with `max_duration_ms` 1 000 is rebound 3 s after its first bind because its runner restarted
- **THEN** the rebound run SHALL be stopped at once and end `cancelled`

#### Scenario: A runner clock ahead of the controller's does not end the run

- **WHEN** a relay with `max_duration_ms` 60 000 binds on a runner whose wall clock is two minutes ahead of the controller's
- **THEN** the run SHALL be stopped about 60 s after its first bind, not at bind

#### Scenario: A move to another controller keeps the window

- **WHEN** a relay with `max_duration_ms` 4 000 is moved 2 s after its first bind and its new bind is sent by another controller of the cluster
- **THEN** it SHALL end `cancelled` about 4 s after its first bind

#### Scenario: A new start of the same id opens a new window

- **WHEN** a relay with `max_duration_ms` is stopped and later started again under the same id
- **THEN** the second run's window SHALL count from the second run's first bind

