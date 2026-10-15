# Changelog

## Unreleased

### Changed: `:relay_persistence` is gone; a class's `durable` alone decides its state (breaking)

The controller's `:relay_persistence` map (`%{class => bool}`) could switch a class's state off
behind its own `durable` setting, even for an `effectively_once` class the SDK refuses to run
without state: its Cells were then not restored and the state was lost on restart. The key is
removed. Whether a class keeps its work is the `durable` it declares and its runner announces;
a class that should keep no state declares `durable: false`. A leftover entry is ignored.

### Fixed: a Kafka producer no longer holds every output while its broker is down

Under the default `drop_nothing`, a record that found its partition's queue full waited in a
second, uncapped queue, so a producer whose broker was unreachable kept every output in memory.
A dispatch now waits while the producer's queue is full, at most `queue_block_timeout_ms`, and the
record then overflows; a delivery-guaranteed one is parked with the reason `queue_full`.

### Fixed: a remote flow Cell keeps its inputs while its first ship after a restart fails

A `durable: remote` flow Cell whose first ship after a controller restart kept failing reported no
consumed offset, so the class's input-log floor came from the other Cells alone and a sweep could
cut inputs this Cell still needed. It now reports the offset its remote copy holds: its records at
or below the flush cursor. A sleeping `durable: remote` class seeds its Cells' floors with the same
offset instead of counting records that were never shipped.

### Fixed: a controller a relay left no longer rolls back the copy shipped after it

A controller that once ran a relay kept its right to take the relay's remote copy over after
the relay moved on. Its periodic ship of a Cell it had never shipped then replaced the copy the
controller now running the relay had shipped with its own older one, and the relay's return
kept those older files, losing the turns taken elsewhere. That right now ends when the relay
leaves the controller and comes back when it binds there again.

### Fixed: one plugin name in two namespaces runs two workers

A built-in provider's worker was registered under the plugin's id alone, so a plugin of one name
enabled in two namespaces on one controller was handed the other namespace's worker (an
`http_poll` polled only the first namespace's endpoint), and a disable in either namespace
stopped it for both. Every built-in provider now keys its workers by the plugin's namespace and
id.

### Security: mint 1.10.2 and decimal 3.1.2

The controller now ships `mint` 1.10.2 and `decimal` 3.1.2, the patch releases that fix the
published advisories against the versions it pinned before (six for `mint`'s HTTP/1 and HTTP/2
response parsing, one for `Decimal.round/3`). `mix hex.audit` reports none.

### Fixed: validating a steps class refuses what declaring it refuses

A builder's `validate()` (the core's `flz_core_check_class`) accepted a steps class that
`flz_core_declare_class` then refused: a tasklet producing two channels
(`topology.multiple_output_channels`), a `chained` loop over a chain that branches
(`topology.branching_chained_loop`), and a durable `chained` loop whose last tasklet hands
nothing on (`topology.chained_loop_terminal_without_handoff`). The check now refuses all three
with the same codes, beside every other refusal the record earns.

### Fixed: a controller no longer binds a relay another controller hosts to its own runner

Each controller's convergence loop judged every relay in the cluster against its own runners, so
a relay bound to another controller's runner read as bound to a dead one and was rebound onto a
runner of this controller. Its Overwatch stayed where it was, could not reach that runner's
stream, and every message sent to it was dropped; two healthy controllers could also take each
other's relays. A controller's loop now observes, judges and places only the relays it hosts. A
relay whose controller lost its last runner for it moves to a controller that has one, as the
hub's redistribution already does. On `/healthz` and in `flz` listings a controller now reports
the relays another controller hosts as `unknown` instead of `failed`, and they no longer cut
short the `recovering` grace of its own relays.

### Fixed: a relay moved to a node that held nothing no longer keeps half of the copy it pulled

When the node a relay moved from stopped answering part-way through the pull, the node it moved
to kept the files that had arrived, so a steps instance could bind its Cells without its journal
and end at once as "resumed compensation after crash". A copy that does not arrive whole is now
removed again, leaving that node as it was before the move, the same as a node that kept an older
copy already gets its own copy back. The same holds for a copy fetched from the durability
backend.

### Fixed: a stopped Cell writer no longer lets a `durable: remote` input be confirmed unshipped

A Cell's writer that stopped while a ship was in flight let go of the records it held, so the
runner's next processed-prefix marker on the same stream could confirm an input whose record was
still on local disk alone, and a disk loss then lost an acknowledged input. Those records now
stay owed: the next writer of the Cell takes them over and its first ship confirms them. A relay
that leaves the node ships them once and confirms them when that ship lands; deleting the Cell
releases them, since its records leave the disk.

### Fixed: a runner busy in a handler or waiting out a retry is no longer restarted as wedged

The controller judged a runner's worker pool wedged when a frame was sent between two of its
heartbeats and the runner's count of completed work did not move. A handler still running when
the next heartbeat landed, or a failed payload waiting out its retry backoff, completes nothing
in that window, so a healthy runner could be restarted terminally. The heartbeat now also
carries `handouts`, the number of activations the runner has issued, and the pool reads as
wedged only while that count is flat too. A runner built before this change does not send the
key and is judged as before.

### Fixed: a park no longer costs a thread, and is never lost when one cannot start

A payload the runner could not deliver (a frame naming a channel the relay does not declare, an
unreadable entry, an edge that failed) used to wait for the controller's ack on a thread of its
own, so a source with one misspelled channel name at a high rate started one thread per frame,
and a frame whose thread could not start was lost with only a line on stderr. Every such payload
is now held by one park owner per runner, which retries and re-sends it as before and never
waits on a full consumer. It holds at most 4 096; past that, the thread handing over the next one
(the reader of a frame, or a Cell's worker whose output could not be delivered) waits for an ack,
with one log line per wait, instead of dropping it.

### Changed: every SDK's `cell_state` takes the controller's selector (breaking)

The argument of each SDK's `cell_state` (`cellState` in Java, Node and PHP) is now `cell`, the
`<tasklet>[/<cell_key>]` selector the controller reads, and it is optional: absent, the read
covers every Cell of the relay. Before, it was a required `cell_key`, and a bare Cell key was
refused as `state.tasklet_not_found`. Rust takes `Option<&str>`.

### Changed: an Elixir entry channel decodes by its codec (breaking)

`input/3` in the Elixir SDK now hands the key and the tasklet the payload decoded by the codec it
announces, as every other SDK does: JSON, the default, arrives as the decoded value, `codec: :text`
as a UTF-8 string, and `:bytes` as the bytes. Before, every codec handed over the raw bytes, so a
relay that decoded JSON itself (`map(&Jason.decode!/1)`) should drop that step, and one that
reads text should write `codec: :text`. `:msgpack` still hands over the bytes, since the SDK has
no MessagePack decoder. The short form takes `codec:` too, and `input_with/4` announces a
`description:`.

### Changed: an Elixir keyset needs its `cells_for/1` (breaking)

An Elixir tasklet declaring `cell_keys` and no `cells_for/1` is now refused as
`topology.cells_for_missing`, as in every other SDK, instead of routing each input to every
key. Write `def cells_for(_), do: cell_keys()` to keep that routing. A `cells_for/1` on a
tasklet with no keys is no longer refused, as in every other SDK.

### Changed: the Elixir SDK runs on the shared core (breaking)

The Elixir SDK now runs on `flz_core`, the runtime every other SDK uses, through a Rustler
library over `flz_core_ffi` (`flz_core_beam`, replacing `flz_sdk_nif`). Its own runtime is gone,
so a fix to the core now reaches Elixir too, and an Elixir runner answers activity totals and a
tasklet's Cells like the others. Tasklets run in a pool of BEAM processes fed by threads the
library owns (`FLEZHA_WORKER_POOL_SIZE`, `FLEZHA_WORKER_POOL_MAX`). Records an Elixir runner
wrote before restore unchanged, so no drain is needed.

Three breaking changes. `transport: :native` is removed: a runner connects over UDS (now the
default) or QUIC wherever it runs, the controller's own BEAM included, and `FLEZHA_TRANSPORT`
takes `uds` or `quic` only. A runner in the controller's BEAM no longer proves its namespace by
running there: it presents a runner credential like any other, through the new
`namespace_secret:` option or `FLEZHA_NAMESPACE_SECRET`. `named/2` names the channel only; the
tasklet keeps its own name.

A runner runs once per BEAM, as in every SDK: `close/1` ships each relay's state and ends the
session, and a later start in the node, the restart after a crash included, is refused as
`runtime.core_shut_down`. One runner or one local run is live at a time (`runtime.core_in_use`).

A host can now hand the core its connection settings at start (`flz_core_start_with`), so a
runner embedded in a process with someone else's environment reads its own. A start the
controller refuses, or does not confirm in time, leaves no runner behind and is answered at
the call. `flz_core_shutdown` now also ends the session once the state is shipped, and
`flz_core_pending_timers` reads the pending timers of the Cell an activation runs, which a
host keeping its own copy reads once after a restore. The Java SDK reads it on a session's
first timer call: its cancel of a timer registered in an earlier invocation used to record
nothing, so the timer still fired.

### Added: a tasklet's Cells, and a relay's outputs per channel

`Relay.activity/4` takes `cells_of` and `limit` (`flz relay activity <relay_id> --cells
<tasklet> [--limit N]`): beside the per-tasklet rows it answers that tasklet's Cells, the most
recently admitting first (50 by default, at most 500), each with its key, queue, totals and last
admission time, and the tasklet's Cell count. The runner picks them on the walk the activity read
already makes, from counters each Cell already keeps; `TaskletActivity` gains `cells_of` and
`limit`, and its response an optional `cells`. A runner that keeps no per-Cell counters, the
Elixir SDK among them, answers no Cells (`cells: nil`). An undeclared tasklet is
`state.tasklet_not_found`.

`Relay.listen/5` takes the direction `tally` (`flz relay listen <relay_id> --tally`). It binds
the listener's plugin first on `on_output` in a counting mode: each output adds one to a counter
for its channel on the relay's own node, and the plugin's heartbeat worker reports the totals
about once a second (`tally_ms`). The handler gets `{:tally, totals, %{read_at_ms:, epochs:}}`,
summed over a partition group's members; an epoch changes when a member's count restarts. The
channels are the relay's announced outputs unless `channels` names them; none is
`listener.no_channels`. A relay nobody tallies runs exactly the code it ran before.

`Fleet.providers/1` and `Fleet.provider/2` answer `feeds`, whether a provider's declaration says
it injects records into a relay. The Rust reference test app registers `showcase`, a relay whose
announced graph has every shape the console's relay graph draws.

### Added: what each tasklet of a relay is doing

`Relay.activity/3` (`flz relay activity <relay_id>`, also over HTTP and MCP), bound to
`relay_info`, asks the runner hosting a relay for one row per tasklet: its live Cells, the items
queued in total and in the deepest Cell, and the totals of items admitted, blocked waits, and
items dropped as newest, as oldest and at the Cell cap, with a dynamic tasklet's Cells created
and evicted. The reply carries the runner, its read time and the instance's sequence number, so
a caller can tell a new instance from a falling total. It rides a new control command,
`TaskletActivity` (variant 43). `flz_core` adds up counters its Cells already keep, walking them
off the control reader, and keeps an evicted Cell's totals in its tasklet's; nothing is added per
message. The Elixir SDK reports Cells and queues only, leaving every total absent. The
controller keeps nothing between calls. A runner that does not answer, or predates the read, is
`activity.no_answer`.

### Added: a relay's graph

A runner now announces each factory's graph with its API: every channel of the class with its
kind, the tasklet that produces it, the tasklets that consume it, whether it ends at a plugin
output or a call (and the call's target), and its overflow policy; and every tasklet with its
cell mode, cell keys and mailbox capacity. It rides `FactoryDecl.graph` in `UpdateFactories` and
a `graph` key in the connect frame, built when the factory is announced and never per message,
by `flz_core` for the Rust, Java, Python, Node, Ruby and PHP SDKs and by the Elixir SDK for its
own classes. The controller keeps it beside the API, out of the factory rows a call reads.
`Relay.graph/3` (`flz relay graph <relay_id>`, also over HTTP and MCP) answers one relay's,
bound to `relay_info`; a partition member and its group both read their factory's. A factory
whose runners announced none answers no graph with the reason `not_announced`, and its runners
are still placed. When a factory's runners announce different graphs, the read says so and
serves the latest.

### Added: a relay's traffic in bytes

Each relay now counts the payload bytes it takes in, filters and forwards beside the messages,
as the liveness counters `bytes_in`, `bytes_filtered` and `bytes_forwarded` and the relay
metrics of the same names (`relay_bytes_in` and so on). A forwarded message is no longer
counted on its own: `forwarded` and `bytes_forwarded` are what was received less what was
filtered, worked out when the counters are read, so a forwarded message still costs two counter
writes. A payload's size is read from the binary, not measured. Forwarded bytes are the size
before any `on_input` rewrite. The console shows a relay's rate as messages and bytes a second.

### Added: a relay's calls can be watched as they happen

`flz relay listen <relay_id> --calls`, and `Relay.listen/5` with the direction `calls`, show
every call a relay takes, from the CLI, the console, the HTTP API, MCP, a runner, a feeding
plugin or another relay: a `call` event with the request, who called (`flz.call_source`, and the
calling relay as `flz.call_caller`), the entry channel, when it was taken and its deadline, then
exactly one outcome, `answered` with the answer and its channel, `timed_out` or `refused` with
the error, each with the call's duration. The two are paired by `flz.call_id`, a digest of the
call's token that stays the same across a move and from which the token cannot be recovered.
The events are made in the callee's Overwatch and travel on a new listeners-only fanout hook,
`on_call`; a relay nobody listens to for calls builds nothing. Watching calls is judged by
`relay.listen`. `--calls` is refused beside `--input` with `usage.options_conflict`.

### Added: a provider's options are readable as typed rows

`Fleet.provider/2` (`flz plugin help <provider> --json`) answers `options`: one entry per option
a user can write, in declaration order, with its name, type, whether it is required and its
documentation, and its default, allowed values, bounds and whether it is live or transient when
it declares them. A transient option never answers its default. The keys it answered before are
unchanged, and the text `flz plugin help` prints is the same. The catalogue's argument
descriptions and the documented option tables are read from the same row description. Five
first-party options that were typed `any` now say what they take: Kafka `hosts` is `names`
(`host:port`, comma-separated or as a list; a `{host, port}` list is still accepted), Kafka
`auth` is `map`, Kafka and RabbitMQ `ssl_options` are `list`, and the listener plugin's
`namespace` is `string`.

### Added: an operator can drop a runner process

`Fleet.drop_runner/3` (`flz runner drop <runner_id>`) closes a runner's session, wherever in the
cluster it is connected, without telling the runner to stop: its relays are re-placed at once,
and every controller refuses the same process with `runner.dropped` until a different process of
the same runner connects. The drop mark names the runner's stable id and SDK incarnation, keeps
its process id for display, survives a controller restart and reaches every controller, the way a
revoked token does. `Fleet.dropped_runners/2` (`flz runner dropped`) lists the standing marks and
`Fleet.readmit_runner/3` (`flz runner readmit <stable_id>`) lifts one. Dropping and readmitting
need the new `runner.drop` action, granted to `ops` and `admin`, and are audited on
`fleet_service`. A runner with no stable identity or incarnation is refused with
`runner.untrackable`. `runner_dropped`, `runner_readmitted` and `runner_drop_refused` are
emitted on the `:flezha_runner` source.

### Fixed: disabled plugins no longer keep the convergence loop busy

A binding whose plugin was disabled (`flz plugin disable`) keeps its relay running and reports
`plugin.not_found`, as before, but the convergence loop no longer counts its retry as a repair.
With twenty or more such bindings the loop used to tick without pause and hold the controller's
CPU; it now ticks at `FLEZHA_CONVERGENCE_TICK_MS`. Enabling the plugin again still brings every
binding up at once. A tick's cost also no longer grows with the relay state left on disk: its
liveness read takes the live writers from memory, and the walk of the state directory resolves
each namespace and the state root once.

### Fixed: stopping a namespace no longer stalls on its runners' control channels

A runner's control channel that closed while its namespace was being stopped waited five
seconds for the fleet manager, which was busy stopping that namespace, and then crashed with an
error in the log. The channel now hands the disconnect over without waiting. Closing a state
writer that another closer already stopped counts as closed instead of failing.

### Fixed: a Cell's WAL no longer freezes behind a newer snapshot

A writer took its seq from the WAL file alone, and adopted a snapshot's seq only while it awaited
a cold baseline. A compaction that left the WAL empty, followed by a controller restart, or a
snapshot that landed while a seq gap was open, left the writer behind its own newest snapshot. A
restore then shipped the snapshot's seq, the runner resumed past it, and every Change was refused
as a gap: the Cell's state stopped persisting and its relay stayed `degraded` (`wal_frozen`). A
writer now boots from whichever of the WAL and the newest snapshot reaches further, and any
snapshot ahead of it is adopted as its baseline, closing an open gap.

### Fixed: a Cell is resynced and compacted against the runner its relay is bound to now

A seq gap or a compaction the Watchdog gave up on was resynced against the runner the fact named
when it was declared, and the Watchdog sent compactions to the runner that last wrote the Cell.
After that runner reconnected under a new session, and for an idle Cell whose writer still named
the old one, every compaction gave up and every resync went to a session that had ended
(`resync dispatch failed … no_control_channel`), so the Cell never recovered. Both now go to the
runner the relay is bound to (`Flezha.State.Recovery.bound_runners/0`), and to the named one only
while that binding is unknown.

### Fixed: a runner no longer drops into a reconnect every idle window

When the runner reconnected for a reason other than its control reader (a data-stream write
flood, a controller request), the replaced session's reader kept its socket open, and one idle
window later its timeout ended the session that replaced it, whose reader then did the same to
the next. The runner reconnected every 45 s for as long as it ran. A reader's end is now honoured
only for the live session, and the coordinator closes the control stream it replaces.

### Fixed: frames on one data stream can no longer interleave

The flusher's cell-state records, plugin outputs from every poll worker and the data heartbeat
share a runner's first secondary stream, and nothing serialized them: the kernel may queue a
large write in pieces, so another writer's frame could land inside it and the controller closed
the stream as `frame_too_large`. Every frame on a Unix stream is now written whole under a lock
shared by all handles cloned from it. `FlezhaStream::Unix` holds a `UnixWire`; build one with
`FlezhaStream::unix(stream)`.

### Fixed: a runner's reconnects no longer exhaust the controller's file descriptors

A stream channel published its stream handle to a persistent term and unpublished it only in
`terminate/2`, which a supervisor shutdown never ran. Every runner session left 16 handles behind,
each pinning its listener's wake descriptor, until the controller could open no socket at all
(`Too many open files`) and refused every runner.

### Fixed: a cluster of controllers shares one authorization state

Tokens, role bindings, namespaces and the bootstrap claim were kept per controller, so a token
issued on one controller was refused by the others and each asked for its own bootstrap claim.
Every mutation is now written on all connected controllers before the verb returns, and a
controller that reconnects merges with the others: a revoked token stays revoked, otherwise the
later write wins, and of two bootstrap claims made apart the earlier one wins. The stamps that
order writes live in a new `replica.dets` beside the other auth files.

### Fixed: an enforcing controller stops gracefully on SIGTERM

The image entrypoint's SIGTERM trap runs `flz controller stop`, an admin verb, without a token, so
a controller with authorization enforced refused its own graceful stop and was killed without the
drain. Set `FLEZHA_SHUTDOWN_TOKEN_FILE` to a file holding an `admin@_system` token and the trap
presents it.

### Added: `FLEZHA_INPUT_SILENCE_TIMEOUT_MS`, and the sweep judges the relay

The input-silence sweep (`:input_silence_timeout_ms`) can now be turned on from a container's
environment. It also judged each cell rather than the relay: one cell that produced once and then
had nothing to hear, such as a configuration cell beside a live feed, flagged the whole relay at
the first sweep and kept it latched, so a later real outage went unreported. A relay is now silent
only when none of its cells that produced output has heard a Change in the window.

### Added: a caller can read which operations it holds, and a class's history retention

`Namespace.whoami/1` now answers `operations`, the names of the operations the caller's role
grants (what `Admin.catalogue/1` answers for the same context), so a console can offer only
the controls a caller may use; `flz auth whoami` prints the same text. `Steps.cells/2` answers
`history_retention_hours`, so a retention set through `Steps.set_retention/3` can be read back.
`PluginManager.enabled/2` rows answer `provider_id`, the id a plugin's provider is registered
under, which `Fleet.provider/2` and an enable take; `provider` stays the provider's name.

### Fixed: a deleted Cell is no longer listed as one with persisted state

`Steps.delete/2` removed a Cell's files and stopped its writer but kept its counters, which
`Steps.cells/2` and `flz state list` read their rows from, so the deleted Cell stayed listed with
the counters it had before. Deleting a Cell now drops them too.

### Fixed: every relay keeps its liveness counters when another one stops

The liveness counter table belonged to whichever relay or channel registered first, so stopping
that one deleted every other relay's counters and `Controller.liveness_counters/1` answered only
relays started afterwards. The table is now created by the observability supervisor at boot.

### Added: a consumer on another node holds every stream with data alone

The context's `:emit` may now be a pid as well as a function. `Relay.listen/5`,
`Observability.tail/2`, `AuditLog.tail/2` and `Controller.stop/2` then send it each event as
`{:flezha_emit, event}` and end when it exits, so a console on another node no longer runs any
code of its own on the controller. With an `:emit`, the two tails run until the pid exits, the
context's `window_ms` closes or the audit writer does, release their subscription, and hand on
only what `Observability.query/2` or `AuditLog.read/2` would show the same caller. Runner
sessions are published as `runner_connected` and `runner_disconnected` on the `:flezha_runner`
observability source, and `Controller.status/1` answers `monotonic_us`, the clock signals are
stamped with ([`docs/api/README.md`](docs/api/README.md)). `Fleet.factories/2` now answers
`{:ok, list}` and `Fleet.runner_caller_context/2` answers `{:ok, map}`; `Relay.api/3`,
`Relay.call_channel/6` and `Relay.diagnose/3` are listed on the published surface.

### Changed: a shorter getting started, and the larger tutorials on a docs site of their own

Getting started is What is Flezha, then the echo built by hand on three pages: Up and
running, Connect a plugin, and Stop and clean up. The tradedesk and agentdesk tutorials
build as a docs site of their own under `tutorials/`, linked from the sidebar. The
`flz template` page is Template commands under the CLI, local mode is Testing in local
mode at the end of Core topics, and the Try it page is gone.

### Added: `flz template`, a new project from the release

`flz template create <language> <dir>` writes a small, complete project from a template the
installed release carries, and `flz template list` prints them; neither needs a controller
([`docs/topics/cli/template_commands.md`](docs/topics/cli/template_commands.md)). The
one template, `starter`, is each SDK's new `examples/starter`: a described entry channel `hit`
keyed by page into a tasklet that counts visits per page in state through a change, a test
with the SDK's harness (local mode in Elixir), and a README, tested by each SDK's suite.
`--tar` writes the files as a tar stream, so
`docker exec <container> flz template create python my-app --tar | tar x` hands them to the
host. The fifth knowledge tool, `flezha_new_project`, answers the same files over MCP, and the
assistant rules say to start a new project from it. New codes `template.unknown`,
`template.dir_not_empty` and `usage.tar_to_terminal`.

### Added: every relay has a public API

A relay's entry channels are its API, announced by its runner with its factories: each
channel's format, whether it answers and in which format, and a description and a JSON
Schema when the channel declares them (`describe`/`schema`, or the `description`/`schema`
options; Java records, Python types, PHP typed classes and, with the `schemars` feature,
Rust types are read into a schema) ([`docs/topics/core/sending_and_calling.md`](docs/topics/core/sending_and_calling.md)).
`Relay.api` lists it and `Relay.call_channel` calls a channel by value; `flz relay info <id>`
shows a relay's API, `flz relay call` takes `--arg name=value`, the HTTP API plugin serves
`POST /v1/relays/<id>/<channel>`, every SDK's runner gains `call` beside `send`, and each
described channel is an MCP tool `<factory>_<channel>`, so an AI agent calls a relay as a
tool. New codes `api.channel_unknown`, `api.input_invalid`, `api.not_advertised` and
`topology.schema_invalid`.

### Added: `flz relay diagnose`, what is wrong with a relay and what to do

`Relay.diagnose` reads what the controller already keeps about one relay (the convergence
loop's verdicts for the relay, its runner and its plugins, the runners that announce its
factory, its dead letters, its input log, its completion record) and answers a verdict,
`ok`, `warning` or `problem`, with findings, each a code, a message, details and a hint
([`docs/topics/core/running_and_testing.md`](docs/topics/core/running_and_testing.md#diagnose-a-relay)).
It is `flz relay diagnose <id>`, the MCP tool `relay_diagnose` and
`POST /v1/call/Relay.diagnose`, judged as `relay.list`. Eighteen `diagnose.*` codes join
the error catalogue, and the assistant rules say to call it first when a relay misbehaves.

### Added: `ctx.effect`, an outside call kept across re-runs

A tasklet calls a model, a tool or a service through `ctx.effect(name, fn)` in every SDK
(`effect/4` in Elixir), and a re-run of the invocation (a crash, a replay, a wake, a move)
reads the kept result instead of calling again
([`docs/topics/core/effectively_once.md`](docs/topics/core/effectively_once.md)).
The controller keeps results in `effects.log` beside the input log, as long as their
input, ships them with it to the durability backend, and sends them to the runner on every
bind. Where an input has no input-log offset, nothing is kept. New codes `effect.bad_name`,
`effect.nested`, `effect.outside_invocation`, `effect.too_large` and `effect.not_recorded`;
the cap is `FLEZHA_EFFECT_MAX_BYTES` (4 MiB). The test harnesses serve kept results on
their replay pass and name an effect whose function runs there.

### Added: `runner.send`, with a message id, a partition key or `local`

Application code on a runner sends to a relay with `runner.send` (`send_to` in Ruby,
`Flezha.SDK.send/5` in Elixir), through the controller as before
([`docs/topics/core/sending_and_calling.md`](docs/topics/core/sending_and_calling.md)).
A message id is stamped as `flz.msg_id`, so a relay with an input log absorbs a repeat;
`flz relay send --msg-id` does the same, and an id that is not text is refused
(`usage.not_text`). A partition key reaches the member that owns it.
The `local` option hands the message to a relay bound on the same runner without the
controller: it is on no disk and lost on a crash, and refused with the new
`runtime.relay_not_local` for a relay the runner does not hold. Every runner lists the
relays bound on it by instance id.

**Breaking:** `runner.admin()` is now `runner.control()` in every SDK, with no alias (Java
`com.flezha.sdk.control.Control`, Elixir `Flezha.SDK.Control`), and its `send` is gone. The
`admin.*` error codes are now `control.*`. Python's `relays()` answers instance ids rather
than factory ids, and Java's `getRelays()` and Rust's `relays()` answer a list of ids.

### Added: `flz mcp`, an MCP server for AI assistants

An AI coding assistant adds Flezha with one command, `claude mcp add flezha -- <release>/bin/flz
mcp` or its client's equivalent ([`docs/topics/core/mcp_server.md`](docs/topics/core/mcp_server.md)).
Before any controller runs it serves four tools from the installed release: the working rules
with a map of the guide pages, the current echo example of each SDK, an error code's meaning,
and one guide page. Once a controller answers, the operations an HTTP caller may reach are
tools too, called as the token's principal; stopping the controller, issuing tokens and
resetting never are. The MCP code is now one core module that the HTTP API's `POST /mcp`
shares, so it serves the same four tools. A value `flz` sends to the controller is no longer
interpolated: a message holding `#{...}` reaches the relay as that text.

### Changed: a relay's start and stop reach every sink, and fewer repeated log lines

A deliberate start and a stop or completion are now ordinary observability events,
`relay_started` and `relay_stopped` of source `flezha_relay` beside
`relay_exited`, so the ring buffer, `flz observe tail`, PubSub and the exporters see them;
controller plugins still hear each once, from the same event. A fact the controller already
records once is no longer logged again: an append refused by an input log that refused its
file, a durability claim another node took over, a plugin subscriber that ended normally or
began a restart. A failure that repeats on every record is logged once: a plugin hook that
fails on every record, once until its chain changes; a stream frame that does not decode or
whose Cell's writer will not start, once per runner session; a credit overrun or shed, as
one anomaly event per episode with every frame still counted. The websocket source no
longer builds a debug line every thousand frames.

### Added: Postgres change capture, as a plugin package

The new package `flezha_postgres_cdc`, built from `plugins/flezha_postgres_cdc_plugin`, brings
the provider `postgres_cdc` ([`docs/plugins/postgres_cdc.md`](docs/plugins/postgres_cdc.md)).
It reads a publication's row changes from Postgres's write-ahead log through a logical
replication slot and injects one JSON event per insert, update, delete and truncate, in commit
order, after a first copy of the published tables taken in the slot's own snapshot, so the
copy and the stream meet with nothing missed or doubled. Each event carries an `flz.msg_id`
that a re-stream repeats, and the read position is confirmed to Postgres after every accepted
transaction when the relay's input is durable, every `commit_interval_ms` when it is not.
The plugin checks the server before it creates anything, reports the log its slot holds back,
and keeps the slot when it is disabled. The dev compose file gains a `postgres-cdc` service,
and six error codes join the catalogue.

### Added: RabbitMQ in both directions, as a plugin package

The new package `flezha_rabbitmq`, built from `plugins/flezha_rabbitmq_plugin`, brings two
providers ([`docs/plugins/rabbitmq.md`](docs/plugins/rabbitmq.md)). `rabbitmq_consumer`
consumes one queue with a prefetch window and hands its messages to the relay in batches;
a message is acknowledged only after the relay took it (after the input log holds it when
the relay's input is durable), a refused one goes back to the queue, and its `message_id`
becomes an `flz.msg_id` prefixed with the plugin id. Several relays on one queue are
competing consumers; a partitioned relay is refused. `rabbitmq_producer` publishes each
output to an exchange, routed by a static key or the output's `flz.key`, with its identity
as headers; a delivery-guaranteed output carries a stable `message_id` and is held until the
broker confirms it, published again after a refusal or a lost connection, and parked as
`rabbitmq.exhausted` or, with `mandatory`, `rabbitmq.unroutable`. Every relay bound to one
producer on a controller shares one connection. Both reconnect after a broker restart and
report unhealthy meanwhile. The dev compose file gains a `rabbitmq` service.

### Changed: one overflow policy for every sink queue

What a full queue does under `queue_overflow` is now decided in one place,
`Flezha.Process.Backlog.admit/5`, for `relay_http_push`, the Kafka producer and the
RabbitMQ producer. Neither existing sink behaves differently.

### Added: a Kafka consumer in the Kafka package

The package `flezha_kafka` gains the provider `kafka_consumer`, which joins the consumer
group `group_id` for its `topics` and injects every record of the partitions Kafka assigns
it as one input, its value as the payload and its topic, partition, offset, timestamp, key
and headers as `kafka.*` metadata, with an `flz.msg_id` of the plugin, the topic, the
partition and the offset ([`docs/plugins/kafka_consumer.md`](docs/plugins/kafka_consumer.md)).
Each fetched batch waits for the relay's verdicts; its offsets are committed at once when
the relay's input is durable, so a record fetched again is absorbed, and every
`commit_interval_ms` when it is not. A refused batch commits nothing and is fetched again.
Several relays bound to consumers of one group split the partitions; a partitioned
relay is refused. The lag per partition, each rebalance, a broker or group that is away
and a run of refused batches are reported. The producer and the consumer now share one set
of connection options, and the package raises the client library's request timeout
(`sync_timeout`) to 5 s, which a group's join needs; while a broker is away the producer's
stats can answer that much later.

### Changed: the Kafka producer is a plugin package (breaking)

The Kafka producer has left the controller for the plugin package `flezha_kafka`, built from
`plugins/flezha_kafka_plugin`, and the release no longer carries `kafka_ex`. Install the
package and enable `kafka_producer` where `relay_kafka_producer` was; a plugin enabled
under the old id is not converted ([`docs/plugins/kafka_producer.md`](docs/plugins/kafka_producer.md)).
Without a `partition`, a record now goes to the partition Kafka's Java client chooses for its
key (`partitioner: murmur2`), an unkeyed effectively-once record to the partition its
`flezha.dedup_key` hashes to, and any other unkeyed record to a sticky partition that moves
after each batch; every relay bound to one plugin casts to one worker per topic, which keeps a
queue, window and producer session per partition. `compression: gzip` compresses batches
(`snappy`, `lz4` and `zstd` are refused), the record of a guaranteed or effectively-once output
carries its identity as `flz.key`, `flz.cell`, `flz.seq` and `flz.epoch` headers beside
`flezha.dedup_key` (a plain output's carries none), a
`headers` option adds static ones, and `idempotent` now defaults to `true`. `stats/1` reports
each partition under `partitions` and `stats/3` is gone. The core gains
`Flezha.State.Outbox.dedup_key/1`, which the HTTP sender's `idempotency-key` now uses; the
unused `FLEZHA_TESTNET_KAFKA_PORT` is removed. The dev broker also listens on
`localhost:19092` for processes on the host.

### Added: a Postgres table read by polling, and a literal Postgres password that works

The new provider `relay_postgres_poll` reads the new rows of one table, and with a
`timestamp_column` its changed rows too, every `interval_ms`, oldest first, and injects each
row as one JSON object with an `flz.msg_id` of the plugin, the table and the row's cursor
([`docs/plugins/postgres_poll.md`](docs/plugins/postgres_poll.md)). It needs no replication
rights: the plugin builds the query from validated, quoted names and runs every read in a
read-only transaction. The cursor is committed to the binding after every batch when the
relay's input is durable, so a row read again after a restart is absorbed, and otherwise
once the reader catches up and at most every `commit_interval_ms`. It is not shareable and
refuses a partitioned relay. The Postgres sink connects with a literal `password` again:
it used to send the redacted placeholder, so only `password_env` could log in. Both providers
now share their connection options, which lists the sink's `ssl` beside the other connection
rows.

### Changed: a shared source hands each record straight to its relays

A shareable source plugin (`relay_websocket`, `relay_http_poll`) still runs once per
controller, but its worker now injects each record directly into every relay sharing it,
with no process in between and without waiting on any of them: a slow relay falls behind
in its own input queue and loses nothing. A relay whose input is durable (effectively-once,
sleeping or durable steps) runs the source on its own again, as before plugins could be
shared, so every frame is confirmed to it and a refused poll is offered again, and a slow one
holds back only its own connection. A shared poller offers each relay the content it does
not hold yet, so a newly bound relay gets the current content at the next poll. A relay
no longer reports `plugin_lagging` for a shared source, and
`FLEZHA_PLUGIN_BACKLOG_BYTES` and `FLEZHA_PLUGIN_BACKLOG_FRAMES` now bound only a controller
plugin's hook events. A plain output to a binding that is not started is counted as the
relay metric `relay_not_started_drops` instead of a
`[:flezha, :plugin, :not_started, :dropped]` event per output. A package provider declaring
both `feeds` and `shareable` injects into `Flezha.Plugin.Host.targets/1` and takes
`{:targets, targets}` (see [`custom_plugins.md`](docs/guides/advanced/custom_plugins.md)); one
that injects into its copy has the record dropped, warned of once and reported as
`plugin_lagging`.

### Added: a relay's outputs sent over HTTP (breaking for `hook_webhook` telemetry)

The new provider `relay_http_push` sends each output (or, on `on_input`, each ingested
record) as one `POST` or `PUT` to its `url`, over connections it keeps open, in order by
default ([`docs/plugins/http_push.md`](docs/plugins/http_push.md)). Every request carries the
output's `flz-key`, `flz-cell`, `flz-seq` and `flz-epoch`, and a guaranteed output an
`idempotency-key`. A plain output is retried and then dropped and counted; a delivery-
guaranteed one is held until a 2xx and otherwise parked as a dead letter with
`http_push.rejected` or `http_push.exhausted`, after a retry budget bounded by `retries` and
`delivery_timeout_ms`. The queue is bounded by payloads and bytes with the Kafka producer's
`queue_overflow` policies, and under `drop_nothing` the dispatch waits only while it is full.
Relays bound to one plugin share it. `plugin.headers_invalid` now names the header in its
`header` detail. The webhook controller plugin sends through the same worker, and its events
take the shared names: `shipped`, `retried` and `failed` replace `sent`, `failed` and
`dropped`. Neither shows the URL's path or query in a process status any more.

### Added: what a connector package needs from the core (breaking for `http.partitioned_host`)

Every plugin's options now carry `durable_input`, true when its relay journals its input
(a durable steps relay, or a flow relay with `effectively_once` or that sleeps), set by
the controller over anything stored. `Flezha.Plugin.Host.inject_many/4` hands a batch to the
relay and answers one verdict per record, in order, with up to
`FLEZHA_PLUGIN_INGEST_WINDOW` (64) records waiting at once; `send_io_confirming_many/5` runs
on it and now answers `{:error, {:unreachable, :timeout}}` per record its deadline cut
instead of exiting. `mix flezha.plugin.package` writes the project's configuration of each
library it bundles into that library's `.app`, so the library reads it on the controller,
and refuses a setting an `.app` cannot hold. A package's provider events are exported under
the provider's name, its shared verbs and the ones it declares with `telemetry:`.
`ProviderBehaviour.refuse_partitioned/2` gives every source that cannot route by key one
refusal on a partition group member, `plugin.partitioned_host`, which replaces the HTTP
server plugin's `http.partitioned_host`.

### Added: controller plugins, told what happens in a namespace (breaking for `type: :io_binding`)

A provider is now a relay plugin (`type: :relay`, the default and every existing
provider) or a controller plugin (`type: :controller`); `type: :io_binding` no longer
compiles. A controller plugin is bound to no relay: `flz plugin enable <name> <provider>
--hook <hook> ...` stores its hooks, `flz plugin update` may change them, and `flz plugin
bind` of it is refused with `plugin.not_bindable`. It runs on every controller that holds
its namespace, each copy hearing its own controller, or once with `--single`, until it is
disabled. Five hooks tell it what happened: `on_relay_started`, `on_relay_stopped`,
`on_relay_failed`, `on_dead_letter` and `on_plugin_unhealthy`, each carrying the
namespace, the relay, the controller and a one-line `text`, and never a record's data.
Delivery is best effort, from a queue bounded by the plugin backlog settings; a copy that
falls behind drops its oldest events and `flz plugin list` shows where it lags. The first
one, `hook_webhook`, POSTs each event as JSON to a URL
([`docs/plugins/hook_webhook.md`](docs/plugins/hook_webhook.md)). `plugin.unknown_hook` now
names the hooks a plugin may take. A new DLQ entry also executes
`[:flezha, :dlq, :dead_lettered]`.

### Fixed: a stop forwarded to another controller keeps its options

A relay stop that a controller forwarded to one holding the namespace's hub dropped its
options, so a partition rescale's stop there deleted the member's state instead of keeping
it for the member that takes it over. The forward now carries them.

### Changed: a plugin has a name and relays are bound to it

A plugin is enabled once, by name, in its namespace, with no relay, and relays are
connected to it: `flz plugin enable <name> <provider> [--single] [options]`, then
`flz plugin bind <name> --relay_id <op> [--hook=...] [--channel ...]`; `flz plugin unbind`
takes the connection away and the plugin stays enabled, and only `flz plugin disable <name>`
turns it off. `flz plugin enable ... --relay_id <op>` still enables and binds in one
command. `flz plugin update <name> [options]` changes a running plugin: in place when the
provider declares the changed options live (the websocket provider's `subscription_items`),
by a restart otherwise, naming the relays bound to it. `flz plugin list` shows the plugins
enabled in a namespace, with their provider, bound relays and the controllers they run on
or `idle`; the provider registry moves to `flz plugin list --providers`.

A provider that can be shared (`websocket`, `http_poll`, `stdout`, `kafka_producer`) runs once
on each controller where a relay bound to it runs, and every relay gets every record
through a backlog of its own (`FLEZHA_PLUGIN_BACKLOG_BYTES`, `FLEZHA_PLUGIN_BACKLOG_FRAMES`): a
slow one loses its oldest records, its health says `plugin_lagging`, and the others are not
slowed. With `--single` one copy runs in the whole cluster. A copy whose last relay leaves
stops, at once on an unbind or a stop and after `FLEZHA_PLUGIN_IDLE_STOP_MS` otherwise, so the
connection stays open across a relay's move. The members of a partitioned relay do not
share: each runs its own copy, asking only for its own share of the keys, beside the copy the
unpartitioned relays share. A provider that cannot be shared refuses a second relay
(`plugin.not_shareable`). A relay placed on a controller that lacks a
plugin's package now runs without it, its health naming `plugin.provider_not_loaded`, instead
of parking. Every SDK's admin object gains `bindPlugin` and `unbindPlugin` in its own spelling.

Nothing converts the plugins a relay carried before: a relay starts without them and
logs a warning naming them, and each must be enabled and bound again. `--plugins` specs name a
plugin with `name` instead of `id`. The HTTP server package is the submodule
`plugins/flezha_http_plugin`.

### Changed: a plugin package outlives an Erlang or Elixir upgrade

A package built on OTP `N` now loads on OTP `N`, `N+1` and `N+2`, and one built on any older
or equal Elixir `1.x` loads too, with one warning naming both versions, logged at boot and
printed by `flz plugin install`, when the Elixir minors differ. From Flezha 1.0 a package
loads on any later version of the same major; before 1.0 the rule is unchanged. A refusal is
still `plugin.runtime_mismatch`, its `rule` detail and the first words of its message naming
the rule that failed.

### Changed: the guides teach one relay

The guides, the glossary and the architecture, resilience and SDK pages describe one relay
whose class says how it runs (`run: flow`, the default, or `steps`) and where its work is kept
(`durable: false | local | remote`), instead of two kinds. `what_is_flezha.md` opens with "One
relay, two ways to run it", Part 1 says when an idle relay sleeps, Part 2's workflow page
explains why the order executor runs in steps and every tab declares its loop shape and failure
mode, and `durability.md` shows the durability ladder as one table. Part 3's code pages carry
Java and Elixir; their other language tabs say the page is not yet written for that language. The words bounded and unbounded are kept for input data, as the new
glossary entry says.

### Fixed: a sleeping flow relay confirms its input even when its tasklets keep no state

A flow relay that sleeps, and whose Cells write no record (Rust tasklets without state, say),
never reported how far it had processed its input, so its input log's floor stayed put and every
bind delivered the whole log again. The runner now reports that point for every instance whose
input is journaled.

### Fixed: a relay moving back to a controller no longer loses the turns taken elsewhere

A controller that kept a copy of a relay's state when the relay moved away bound from that
copy when the relay came back, because any local file counted as the newest. As the relay
arrives, the controller now compares its copy with the one on the node it moves from, or else the
cut another controller shipped to the durability backend, by the WAL-owner epoch and then the
highest seq. When the other copy is newer, the local one is set aside and the newer one is pulled
or hydrated as on a controller that holds nothing; what only the local copy held is kept, and a
newer copy that does not land whole leaves the local one in place, with a warning. A relay
leaving a controller closes its writers there, and the retention sweep no longer reopens an input
log. A bind now awaits its restore, so the state a hand-over brings is never reset to the WAL's
afterwards.

### Changed: `durable: remote` keeps a flow relay's work remote

A flow class may now declare `durable: remote`, and `topology.remote_on_flow` is gone: no level of
`durable` is refused on any run. Such a class acknowledges an input (under `ack: per_input` or
`at_least_once`) or a checkpoint (under `ack: checkpoint`) only once the durability backend holds
the records it produced and the input log they point into, so a controller that loses its disk
loses nothing it acknowledged and delivers none of those inputs again. A remote write per synced
batch is the price; `ack: checkpoint` pays it once per interval. A backend outage stalls only the
`remote` classes on a controller, loudly through the RPO alarm, and a class at `local` ships as
before, off every acknowledgement. `flz controller status` prints each running class's level as
`durability_levels`, and each remote commit's wait is reported as
`[:flezha, :state, :remote_commit]`.

### Changed: one relay, configured by one settings tree (breaking)

A relay class says how it runs and where its work is kept in one settings tree: `run` is `flow`
(the default: the commit point is an input, or a checkpoint) or `steps` (the commit point is a
stage; `each` is `once`, `per_input` or `chained`, `on_failure` is `undo_all` or `keep_progress`);
under `flow`, `exactly` is `at_least_once` or `effectively_once` with `ack` (`per_input`, or
`checkpoint` with `every`, `max_delay_ms` and `outputs`), `correction_ms` and `beyond_window`;
`durable` is `false`, `local` (the default) or `remote`, and `local` and `remote` carry
`sleep_after`, a duration or `false`: by default a flow class never sleeps and a steps class
sleeps after six hours. A sleeping flow instance has its input journaled before it is forwarded,
which cost the default flow path about 38 % of its throughput, so the flow default stays awake (a
decision taken while the maintainer was away; it may be overruled). Each SDK spells the tree in its own types, so a
setting written under the wrong parent does not compile, or is refused at registration with
`topology.unknown_field` naming it and its parent. A flow relay may now end itself with
`complete` (it drains what its Cells hold first) and may declare `max_duration_ms`; a steps relay
that has no way to end is ordinary.

Each setting lives in one place. The channel builder's `effectively_once`, `checkpoint_ack`,
`checkpoint_every`, `outputs_at_checkpoint`, `correction_window` and `beyond_window_policy` calls
are gone; `FLEZHA_PARK_IDLE_MS` is gone (the interval is the class's `sleep_after`);
`flz relay start --snapshot-interval`, which nothing read, is gone; the per-class
`:sync_durability` override is gone (`durable: remote` is the class's). On a controller without a
durability backend a class at `durable: remote` is refused its start with
`relay.remote_without_backend`, naming `FLEZHA_DURABILITY_BACKEND`.

The wire carries the run: `UpdateFactories` announces `run` (with `each` and `on_failure` for
steps), `AddRelay` carries it, and the AddRelay response announces the run and the durability.
`flz relay info` prints `Run:` and `Durable:`. The Rust pipeline macro is `flezha::pipeline!`. Upgrade: `flz controller stop && flz reset --yes`.

A chained steps loop declared in Node, Java, Python, Ruby or PHP now runs every pass: its last
tasklet's value reaches the core as the next pass's input, where before the loop ended after
its first pass.

### Added: a published record of what is not yet settled

`docs/publishing/maturity.md` names every part of Flezha that is not settled, and anything it
does not name is stable. It holds one entry today: the QUIC transport is
[experimental: quic](docs/publishing/maturity.md#quic), so its settings and behaviour may
change drastically or it may be removed without a compatibility break. The Unix domain socket
transport stays the default and is stable. A controller that starts its QUIC listener, a
runner that selects QUIC and an Elixir SDK runner started over QUIC each log one warning
saying so, once per process; nothing else changes and no setting is needed to keep using it.

The record also gives each SDK a support tier. Rust, Java and Elixir are tier 1, run by the
maintainer in a live deployment. Node, Python, Ruby and PHP are tier 2: they pass the same
full conformance run in CI with every capability present and are permanently supported, but
the maintainer does not yet run them in a live deployment. Each SDK's README and page under
`docs/sdk/` states its tier. `scripts/check-feature-maturity.sh`, run with the naming gates,
keeps the record and the tree agreeing.

### Added: a relay behind HTTP, as a plugin package

The HTTP server plugin, the package `flezha_http` (provider `http_server`), is built in its own
repository, `Flezha/flezha_http_plugin`, on Bandit and Plug, and installed with
`flz plugin install`. Enabled on a relay of either run, it listens on `port` (loopback
unless `bind` says otherwise) and makes every request one outside call of that relay,
carrying a JSON request envelope; the relay's `response()` answers with a status, headers
and a body, or with a stream answer naming a relay of its namespace, whose parts, outputs
and dead-letter notices the plugin then writes as server-sent events. Connections, waiting
requests (counted before the body is read), bodies (size and one deadline; a body without
`content-length` is refused 411) and open streams are bounded, only the headers `pass_headers` names reach the relay, and every failure is an HTTP
status with the error model's document, stripped of a tasklet's error text and entries. A member of a partition group
is refused. The reference is `docs/plugins/http_server.md`.

The catalogue gains the plugin's eight codes (`http.body_too_large`, `http.body_timeout`,
`http.length_required`,
`http.busy`, `http.streams_full`, `http.answer_invalid`, `http.partitioned_host`,
`http.listen_failed`). A package provider's worker that stops at start with a detailed reason
(one tag and a map of details the catalogue maps, `Flezha.Error.detailed?/1`) keeps that code,
and so does such an activation failure, instead of `plugin.provider_failed` or
`plugin.activation_failed`. `flz plugin enable` on a partition
group that every member refuses answers the members' own error instead of
`plugin.group_enable_partial`. A listener, `flz relay listen` included, on a relay
neither running nor declared is refused with `relay.not_found` at once, where it waited out
the hub's settling first.

### Added: a relay calls another from its graph

`call(...)` ends a chain on a call to another relay, in every SDK and from a relay of
either run: to an instance or a partition group by name (`call("pricing", deadline_ms: 500)`),
or to the instance of a factory whose id is the record's key
(`key_by(...) |> call(factory: "agent", deadline_ms: ..., create: false)`), started with the
request when absent unless `create: false`. Nothing waits: the outcome arrives later as a
`Response` input on the Response channel `<channel>.response`, carrying the request, the answer
or the error that ended the call (`call.timeout`, `call.failed`, `call.busy`,
`relay.not_found`, `relay.factory_mismatch`, `quota.exceeded`, an `authz.*` code, ...),
the instance that answered and whether the call started it. A `:per_input` steps relay
calls too, reading the outcome as a later turn.

When caller and callee both keep a durable input log, a call survives a crash of either side,
runs its turn at most once and ends with exactly one `Response` (at most one when the caller's
side was down past the deadline plus `call_retention_margin_ms`, counted
`[:flezha, :call, :recovered_unanswered]`); off that tier it is best effort. This reverses the
earlier rule that a call frame is never journaled: the request is journaled in the callee's log
and the `Response` in the caller's, and each is kept until its call is over. Requests from one
caller to one target keep their order. `call_pending_max` now bounds the answers a callee owes
relay callers too; a caller's own calls never count against it. A call is judged as the
namespace's runner identity (`spawner` or `app`), as `ctx.spawn_steps` is.

Registration refuses `topology.call_outside_per_input_loop`, `topology.call_without_key`,
`topology.call_deadline_zero` and `topology.call_response_unread`, and a call beside another
plugin end on one channel (`topology.channel_ends_twice`). The host class declaration gains the
`call` link, so the five hosts on the core are rebuilt together; new settings are
`FLEZHA_CALL_RETENTION_MARGIN_MS` and `call_worker_idle_ms`. See `docs/sdk/calls.md`.

### Fixed: a PHP, Python, Ruby or Node entry channel reaches its keyed consumer behind an unkeyed one

An entry channel feeding an unkeyed tasklet declared first and a keyed one after it answered
the routing keys from the first consumer's chain, so a value that chain filtered out never
reached the keyed tasklet. The PHP, Python, Ruby and Node SDKs now answer them from the keyed
consumer's chain.

### Fixed: the Elixir SDK judges a keyless graph as the core does

A graph is keyless for a partitioned start when no tasklet an entry channel feeds is keyed, as in
the other SDKs. The Elixir SDK called a graph keyless only when every tasklet was single, so a
graph whose only keyed tasklet sits behind an inter-task edge started partitioned there and is
now refused (`PartitionsWithoutKeyedTasklet`).

### Added: a tasklet can send a part while it runs

`ctx.part(value)` (`part(ctx, value)` in Elixir, `$ctx->part($value)` in PHP) sends a preview
of the output an invocation is producing at once, from anywhere in `run`, in every SDK and for
a flow relay and a steps one alike. A part previews the tasklet's own output channel,
which must end in a sink (`part.no_sink` otherwise, a `response()` end included), carries a
number from `0` within its attempt, the attempt and the Cell, and goes to the relay's
listeners and nothing else: it is never durable, never reaches a sink or a waiting caller, and
is dropped rather than waited on when the wire cannot take it. The runner counts what it sent
and dropped in its heartbeat (`parts_sent`, `parts_dropped_*`). The wire gains the data frame
`output_part` and the C ABI the entry `flz_core_part`, so the five hosts on the core are
rebuilt together; a controller without this change drops a part.

`flz relay listen` prints a part with a third field beginning `part=<n> attempt=<a>`, and a
dead-lettered input as a notice `error=input.dead_lettered entry=<entry>`; its JSON lines carry
`kind`. It ends with exit 0 when the relay ends, and a listener that falls behind on outputs
is ended `listener.overrun` (`FLEZHA_LISTENER_PENDING_BYTES`, `FLEZHA_LISTENER_PENDING_FRAMES`).

A plugin reads the same stream through `Flezha.Service.Listener.listen/4`, with an idle event
for a server-sent events heartbeat. A listener now survives its relay sleeping, being
rebound, its Overwatch restarting, moving to another controller and a rescale, and leaves no
plugin row behind on any of them. `flz plugin disable` finds a plugin by the relay whose
row holds it rather than by the node that enabled it, so a plugin on a moved relay, or on a
member a rescale added, is disabled from any controller. A bind brings up every declared
plugin its table does not hold, so a relay found asleep at boot that gained a runtime
plugin before it woke keeps its sinks.

### Changed (BREAKING): every failure carries a code

A failure now reaches a caller in one shape on every surface: a stable `code` spelled
`family.snake` (`namespace.not_found`), a `class`, a plain `message`, `details` naming what the
message is about, and a `hint` naming the next command when there is one. The codes are defined
once, in `flz_transport/errors.json`, and listed in the generated `docs/errors.md`.

- `flz` writes a failure on stderr as `error: <message> (<code>)` and a `hint:` line, and under
  `--json` or `--output json` as one `{"error": {...}}` line. `--json` is accepted on every verb.
  The trailing `Error: Command execution failed.` line is gone.
- `flz` exit codes follow the class: 1 internal, 2 invalid, 3 unreachable, 4 denied, 5 not_found,
  6 conflict, 7 unavailable. A script testing `$? -eq 1` for every failure must test `!= 0`, or
  the class it cares about. `flz controller stop` refused for want of a token now exits 4 rather
  than reporting the controller as unreachable, a partial drain exits 7, and `flz reset` exits 2
  when it only printed its plan and 6 while the controller is up.
- Every published `Flezha.Service.*` function answers a failure as `{:error, %Flezha.Error{}}`
  rather than a raw reason term, and `Flezha.AuthZ.TokenStore.verify/1` answers `authz.token_*`
  codes. The audit record of a failed act carries `error_code`, and an `authorization_denied`
  record's `reason` is the `authz.*` code.
- The wire carries one `Error` message in `AdminResponse`, `SpawnStepsResponse`,
  `TaskCommitResponse`, `IterCommitResponse`, `RelayMessageResponse` and `CommandResponse`,
  and a refused runner connect is answered with `ConnectRefused` before the stream closes. A
  refused spawn carries the controller's own code, and `SpawnStepsResponse.outcome` keeps only
  `spawned` and `delivered`.
- The C ABI's `flz_core_last_error` returns the JSON error document for every failure; the
  separate failure bodies of `flz_core_check_class` and `flz_core_spawn_result` are gone. The new
  `flz_core_error_document(code, details_json)` renders a host's own refusal from the catalogue.
- Each SDK exposes one error type carrying the five fields, raised for every refusal, and a
  refusal's name is its code: `SleepWithoutPersistence` is `topology.sleep_without_persistence`.

### Added: the Elixir builder has `branch`

`branch/2` fans a stream out to a list of arms, each a function called with that same stream,
and answers the stream: `placed |> branch([&into(&1, ConfirmFill), &into(&1, RecordOrder)])`
builds the topology two `into/2` calls off the held handle build. It is imported with the rest
of the builder, and a branch of fewer than two arms is refused as `BranchTooFewArms`, as in
every other SDK.

### Changed (BREAKING): an Elixir tasklet writes each change with `defchange`

In the Elixir SDK the idle TTL is `idle_ttl_seconds`, a positive whole number of seconds, in place
of `cell_idle_ttl_ms`. The clock follows from what the tasklet writes: `event_time/1` (renamed
from `event_time_of/1`) makes it event time, `logical_clock: true` makes it logical, and both at
once do not compile; the `clock:` option and `clock/0` are gone. A `state` block declares a
state's fields and defaults and generates no change: the `set_<field>` and `push_<field>`
recorders and their `{:set, ...}` and `{:push, ...}` clauses are gone, and `defchange` is the one
way to change state, with or without a block. A module that `use`s `Flezha.SDK.Tasklet` or a
relay macro and writes `apply_change/2` by hand does not compile; the hand-written
`@behaviour` form, which does, is the reference form. `complete/2` is imported like `emit/2`. A
retired option, or a `clock/0`, `event_time_of/1` or `cell_idle_ttl_ms/0` written in a `use`
module, does not compile, naming what replaced it, and one in a hand-written module is refused
at registration as `RetiredCallback`, beside the new `TwoClocks`. Saved state is unaffected as
long as a change keeps its term, with two exceptions. A change a `set_<field>` or
`push_<field>` recorder wrote (`{:set, field, value}`, `{:push, field, item}`) folds only through
a `defchange` that records the same tuple, such as `defchange set(state, field, value)`. A change
a hand-written `apply_change/2` took as a bare term (a string, an atom, a map) is not a term
`defchange` records, so a log holding one no longer folds and that relay's saved state must be
cleared once.

### Fixed: an Elixir tasklet's `stable_id` differs between cells

`stable_id/1` in the Elixir SDK hashed an empty identity at seq 0 in every cell, so two bounded
instances, or two cells, answered the same ids: every tradedesk order was booked and reserved
under one id. It now derives from the cell's identity and the change seq the invocation starts
at, as the other SDKs do, so two cells never share an id and an invocation that runs again gets
the ids it had. The seq moves on only when an invocation records a change, a timer op or an
effect, so a cell that records nothing answers the same ids on every invocation. Existing Elixir
cells now answer different ids than before: an input first run before the upgrade and run again
after it gets a new id, so an effect keyed by that id is not idempotent across the upgrade.

### Changed (BREAKING): a Node tasklet emits only through its context

A value leaves a Node tasklet only through `ctx.emit` or `ctx.emitTo`: what `run` returns is no
longer emitted, for a tasklet and a short form alike. `run`, `onTimer` and `compensate` are typed
to return `void | Promise<void>`, so TypeScript refuses one that returns a value, and a plain
JavaScript `run` that still returns its output now emits nothing. The output type is the
definition's type arguments or the annotation on `ctx`, never the body's return type. The
initial-state hook is `initState`; the idle TTL is `idleTtlSeconds`, a positive whole number of
seconds, beside a top-level `maxCells`; the clock follows from what the definition writes
(`eventTime` makes it event time, `logicalClock: true` logical, and both are refused); and a
tasklet fed through `keyBy` gets a cell per key from the wiring alone. `dynamicCells`, `clock`,
`initialState` and the `ClockKind` and `DynamicCells` types are gone, and `defineTasklet` refuses
any field it does not read, naming it and what replaced it. A stream handle gains `sealed()`,
which ends a chain for the type checker and changes nothing at runtime. Saved state is unaffected,
since every tasklet keeps its name, with two exceptions. A tasklet fed through `keyBy` that did not
carry `dynamicCells` ran as one cell and now gets a cell per key, so its old single-cell state is
never read again and each key's cell starts from `initState`. The reverse holds too: a
`dynamicCells` tasklet fed by no `keyBy` edge is now single, so its per-key cells are never read
again and every key folds into one new cell.

### Fixed: two relays of one factory on a Node runner no longer share a cell

A Node runner kept a cell's state by tasklet and cell key alone, so two relays of one factory
served by the same runner, such as two instances of a steps relay running at once, read and
wrote each other's state. Each relay now has cells of its own.

### Changed (BREAKING): a Ruby tasklet names itself and emits only through its context

A value leaves a Ruby tasklet only through `ctx.emit` or `ctx.emit_to`: what `run`, `on_timer`
and `compensate` answer is ignored, for a tasklet and a short form alike, so a `run` ending in
`ctx.change(...)` no longer emits the folded value. Ruby answers its last expression, so a
non-nil answer is not refused. `Ctx#emit_returned` and the harness's `Outcome#returned` are gone.
Every tasklet class writes `tasklet_name`, and one wired without it is refused when its relay
is declared, so renaming a class never moves its saved state. The initial-state hook is
`init_state`, and optional under `state_type`: a new cell then starts from `state_type.new`. The
idle TTL is `idle_ttl_seconds` in whole seconds and `max_cells` is a setting of its own; the
clock follows from what the class writes (`event_time` makes it event time, `logical_clock`
logical, both are refused); `migrate_state` and `migrate_change` are instance methods receiving
the stored bytes; and a tasklet fed through `key_by` gets a cell per key from the wiring alone.
`dynamic_cells`, `idle_ttl_ms`, `clock` and the migration blocks are gone and raise
`NoMethodError` naming what replaced them, and a class that still defines `initial_state` is
refused naming `init_state`. Saved state is unaffected as long as each tasklet keeps the name it
had (an unnamed tasklet was named after its class in snake case, so write that name), with two
exceptions. A tasklet fed through `key_by` that did not carry `dynamic_cells` ran as one cell and
now gets a cell per key, so its old single-cell state is never read again and each key's cell
starts from `init_state`. The reverse holds too: a `dynamic_cells` tasklet fed by no `key_by`
edge is now single, so its per-key cells are never read again and every key folds into one new
cell.

### Fixed: two relays of one factory on a Ruby runner no longer share a cell

A Ruby runner kept a cell's state by tasklet and cell key alone, so two relays of one factory
served by the same runner, such as two instances of a steps relay running at once, read and
wrote each other's state, from two worker threads at once. Each relay now has cells of its
own.

### Changed (BREAKING): a Python tasklet is a named class that emits only through its context

`@flezha.tasklet(name=...)` decorates a class whose `run(self, value, ctx)` returns `None`: a
value leaves only through `ctx.emit`, and a function handed to `flezha.tasklet` is refused,
pointing at the short form. `name` is required, so renaming a class never moves its saved
state. `run`, `on_timer` and `compensate` take the context last; one annotated to return a type
is refused at declaration, and one that returns a value anyway fails that input without a retry.
The context is `Ctx[State, Output]`, so pyright checks what `ctx.emit` is handed and checks a
named tasklet at the edge. The initial-state hook is `init_state`, and optional: a new cell
starts from the state class the context names, called with no arguments. The idle TTL is
`idle_ttl_seconds` in whole seconds, the clock follows from what the class writes (`event_time`
makes it event time, `logical_clock=True` logical, both are refused), and a tasklet fed through
`key_by` gets a cell per key from the wiring alone. `dynamic_cells`, `idle_ttl_ms` and `clock`
are gone and refused as unknown keywords. A function short form still returns its output, and
takes no context; a class short form follows the class form. `Edge.sealed()` ends a channel for
the type checker, and a `TypedDict` state or change is stored as the plain dict it is. The
harness's `Outcome.returned` is gone. Saved state is unaffected as long as each tasklet keeps the
name it had (an unnamed tasklet was named after its function or class in snake case, so write
that name), with one exception: a tasklet fed through `key_by` that did not carry
`dynamic_cells` ran as one cell and now gets a cell per key, so its old single-cell state is
never read again and each key's cell starts from `init_state` or the state class. The reverse
holds too: a `dynamic_cells=True` tasklet fed by no `key_by` edge, which the core routed by the
keys its producer or entry frame carried, is now single, so its per-key cells are never read
again and every key folds into one new cell. A class that still writes `initial_state` is
refused, naming `init_state`.

### Fixed: two relays of one factory on a Python runner no longer share a cell

A Python runner kept a cell's state by tasklet and cell key alone, so two relays of one
factory served by the same runner, such as two instances of a steps relay running at once
or two partitions of a stream, read and wrote each other's state. Each relay now has cells of
its own.

### Changed (BREAKING): a PHP tasklet names itself and emits only through its context

A PHP tasklet's `run` is declared `: void`, for a tasklet and a short form alike: a value leaves
only through `$ctx->emit()` or `$ctx->emitKeyed()`, and PHP refuses a `run` that returns one.
`#[Task]` requires a `name`, and a class without one is refused at registration, so renaming a
class never moves its saved state. The initial-state hook is `initState(?string $cellKey)`, the
idle TTL is `idleTtlSeconds` in whole seconds, and the clock follows from what the class writes:
an `eventTime` override makes it event time, `logicalClock: true` makes it logical, and both
together are refused. A tasklet fed through `keyBy` gets a cell per key from the wiring alone.
`dynamicCells`, `idleTtlMs`, the `clock` argument and the `Flezha\Clock` enum are gone, and a
class still writing one is refused naming it. A change stays plain data folded by `applyChange`.
Saved state is unaffected as long as each tasklet keeps the name it had (an unnamed tasklet was
named after its class in snake case, so write that name), with one exception: a tasklet fed
through `keyBy` that did not carry `dynamicCells` ran as one cell and now gets a cell per key, so
its old single-cell state is never read again and each key's cell starts from `initState`.

### Fixed: two relays of one factory on a PHP runner no longer share a cell

A PHP runner kept a cell's state by tasklet and cell key alone, so two relays of one factory
served by the same runner, such as two instances of a steps relay running at once, read and
wrote each other's state. A step of one order could then act on another order's values. Each
relay now has cells of its own.

### Changed (BREAKING): a Java tasklet is one class, and nothing is generated

The Java SDK has one way to write a tasklet: your own class implements
`Tasklet<Input, State, Output>`, carries `@Tasklet.Spec` with its name and settings, and writes
`run(input, ctx)`. `ctx.state()` reads the cell's state, `ctx.change(...)` records a change, and
a change is a record implementing `Change<State>` whose `applyTo` writes the state. A new cell's
state comes from the state class's no-arg constructor, or from a written `initState(cellKey)`;
hooks such as `onTimer`, `eventTime` and `compensate` are declared by writing them, and writing
`eventTime` makes the clock event time. The relay names the class itself, `.into(TrackPrices.class)`,
and a short-form class annotated `@Relay` or `@Relay` is read when it is
registered. The `flezha-sdk-processor` module is gone, with `@Tasklet`, `@State`, `@Journal`,
`@InitState`, `@CellsFor`, `@CellKey`, `StateHandle`, `TaskletCtx`, `Stateless` and the clock and
config type parameters. The types a tasklet saves are found by the SDK and take ids derived from
their names, so adding a change record moves no other id, and a type nested in the tasklet is named
through the tasklet's `@Tasklet.Spec` name, so renaming or moving the tasklet class keeps its
state; a written `kryoTypes()` owns ids by list order from the first save. State saved by an earlier Java SDK is not readable: clear it once when upgrading.

### Fixed: an instance's input floor is what it durably processed

A plain sleeping stream folded inputs twice after a controller restart while it was awake, a
dead-lettered input could be owed again after a restart, a durable loop moved off a dead runner
could run input again, and a crash-resumed steps relay could run a message twice. Every
persistent Cell's records now carry the input offset it consumed through, every Cell drops an
input at or below the floor it took, and the runner reports a processed prefix that the
controller confirms once the records behind it are on disk; that prefix is the input log's
floor. An inter-tasklet value rides its producer's record, so a value lost from a consumer's
mailbox is delivered again, and a tasklet's spawn repeated by a crash re-run resolves to the
worker it already started. Every SDK carries the change; the park record no longer holds an
offset, and the task journal gains an `input_released` entry that `flz state journal` shows.

### Added: `flz runner list` shows each runner's process id

A runner connected over the Unix socket is listed with the process id of the process on the
other end, as the operating system reports it, so a runner can be found among the machine's
processes, and crashed on purpose with `kill -9` when trying out recovery. The JSON form keeps
its `runners` list and adds a `pids` object from runner id to process id. A runner connected
over QUIC, which may be on another machine, is listed without one.

### Fixed: a sinkless short form that returns a value no longer parks its input

In the Node, Python, Ruby and PHP SDKs, a tasklet with no outgoing channel whose `run` returned a
value tried to emit it, failed, retried and parked the input in the dead-letter queue. The value
is now left alone, as the Rust SDK always did; an explicit `emit` with nowhere to go is still
refused. The Python and Ruby test harnesses no longer emit what a `compensate` hook returns,
which the runner never did, and a Python `runner.admin()` taken before `start` is refused by name
instead of raising `AttributeError` on every call.

### Fixed: the local-only durability warning points at a page that exists

A production controller started with `FLEZHA_DURABILITY_BACKEND` unset warns that its disk holds
the only copy of its durable state and runs local-only. The warning, and the refusal of a backend
missing an option, sent the reader to a page that is not published; they now name
`docs/configuration.md`. The durability page no longer says such a controller refuses to start.

### Fixed: `flz relay list` reads `recovering` while relays move off a dead runner

After a runner died, a partitioned relay whose members were being moved to another runner
read `failed` in `flz relay list` for a few seconds, though the takeover succeeded. A
`failed` or `unbound` relay now reads `recovering` there, in the table and in the JSON,
for as long as `/healthz` calls the same moment `recovering`
(`FLEZHA_CONVERGENCE_UNMET_INTENT_GRACE_MS`), and its own word after that. The HEALTH column
is two characters wider to fit it. `flz relay info` (each relay and each partition
member), the health counts of `flz cluster status` and the admin `relay_rows` verb follow
the same rule, so an application reading relay health can now see `recovering`.

### Changed: a steps stage commits through its Cells' own records

A durable steps relay Cell writes the same effect record a flow Cell on the effectively-once tier
writes: one record per invocation with its state changes and its plugin outputs, shipped as the
item settles. A stage's `TaskCommit` names the records each Cell wrote (`CommittedCell`, a
`first_seq..seq` range and the channels written on) instead of carrying an outbox, and the
controller appends the stage's journal entry only once every named record is durable, then acks.
A committed stage can no longer be durable ahead of the state it commits.

There is one outbox, the Cell writer's, for both runs. A steps relay Cell's outputs leave once the
entry naming their record is durable; a failed, interrupted or rewound attempt's outputs never
do. A steps output carries the identity a flow relay's carries: `flz.cell`, `flz.seq = <record
seq>.<ordinal>`, the Cell's `flz.epoch`, `channel`, `flz.key` and the author's metadata, so
`emit_keyed` and metadata work in a steps tasklet in every SDK, and an undo output is a newer
record of the compensating Cell rather than a `.1` suffix. With persistence off a steps relay's
outputs go to the wire with their full metadata.

Removed with the second outbox: `TaskOutboxItem` and `TaskCommit.outbox` (field 6 reserved), the
journal's `outbox_flushed` entry, the iteration-boundary snapshot (the Watchdog's triggers compact
steps relay Cells as they do flow Cells), the `task_outbox_flush` telemetry event and the
`task_outbox_retry` backoff row. `flz state journal` prints each Cell's records and channels;
`flz state outbox` prints the entries held for a stage marker.

### Changed: a per-input steps runs as a partition group

`flz relay start <factory> --partitions N` now starts a `per_input` steps relay as N ordinary
steps instances, each with its own key-group range, journal, input log and pass sheet, and
`flz relay scale` rescales it between passes. Each member ends, compensates and commits on
its own; there is no group end, result or rollback. A `once` or `chained` steps relay is refused
with `PartitionsWithoutPerInputLoop`, and a steps class declares `each` in its
`AddRelay` response. A partitioned start carrying `--text` is refused.

A rescale seals every surviving steps relay member at its latest committed iteration: a rewind or an
all-or-nothing rollback no longer reaches a pass committed before it, and the scale command says
so. On the shared path, a stop that keeps state for a move now closes the relay's writers and
its input log on the node it ran on, a same-id move from another controller is a real pull, a
cross-controller pull carries a steps relay's journal and input log with its Cells, and a member
placed on another controller pulls its files before its bind reads its resume state.

### Changed: timers fire inside a steps relay; `wake_after` is gone

A steps tasklet registers Cell timers exactly as a flow tasklet does, in all seven SDKs, and
a due timer opens a pass at the tasklet that set it: its `on_timer` output travels down that
tasklet's chain and commits stage by stage. A firing whose tasklet's stage is running joins it;
any other waits beside messages and runs at the next committed boundary. Its timer leaves the
pending set when that stage commits, so a crash before the commit fires it again and a crash
after it does not. A sleeping steps relay wakes at its earliest pending timer. Only a `compensate`
hook still answers `TimersUnavailable`.

The class setting `wake_after` (`wakeAfter`, `wake_after_ms`, `wakeAfterMs`) is removed from
every SDK, and a woken instance no longer receives an empty `flz.wake` message; a tasklet that
wants a periodic wake registers `timer_after` in its `run`. A host steps relay's entry channel now
runs the event-time crossing a flow relay's does.

### Changed: a relay of either run sleeps when idle

`sleep_when_idle` is a class setting of both runs in all seven SDKs. A flow class that declares
it sleeps once every cell has been idle for `FLEZHA_PARK_IDLE_MS` (600 000 ms), is evicted from
the runner, and wakes on its next input with its state intact; a steps class sleeps by default
when it persists, as before. Sleeping needs persistence: a class that declares it with
persistence off is refused at registration (`SleepWithoutPersistence`). The knob
`FLEZHA_STEPS_PARK_IDLE_MS` is now `FLEZHA_PARK_IDLE_MS`, and the park record moved out of the
task journal into the class directory.

The persistence switch is one class setting for both runs too: a Java flow class that turns
persistence off is now declared so (it used to read its tasklets), and a live state read of any
class with persistence off answers `persistence_off`. Time travel, retention, the watchdog and
evicted-cell deletion now treat a steps relay's cells as a flow relay's. Stopping a sleeping steps relay
wakes it and stops it cleanly instead of removing it hard.

### Changed: a steps tasklet may be keyed

A `key_by` edge into a steps tasklet is accepted in all seven SDKs; the `DynamicCells` refusal
is gone. The tasklet runs one cell per key, minted when a stage's item is addressed to it and
capped by `max_cells` as a flow relay's is. A stage commits the cells it addressed, a crash-resume
restores the keyed cells the instance persisted, and a rollback compensates each keyed cell that
ran, once. An idle keyed cell is evicted by `cell_idle_ttl` only between passes, and never while a
rollback of the declared `on_failure` could still reach it: under all-or-nothing a cell that ran
a compensable tasklet is kept until the instance ends. One pass of one tasklet runs at most
65 535 cells, the width of a stage commit's cell count; a class that keys more per pass declares a
smaller `max_cells`.

### Fixed: an Elixir steps relay rewinds, and delivers its output when it journals nothing

`flz relay rewind` on an Elixir steps instance now compensates the unwound iterations,
latest first, before the loop goes on from the rewound-to iteration; it used to resume without
undoing anything. An Elixir steps class with persistence off now delivers its plugin-bound
output during the pass, as a flow relay does; it used to drop it.

### Changed: a steps instance holds no thread

A steps instance no longer owns a driver thread: its run is a small pass sheet advanced by
events on the thread that brings each one, and its stages reach their cells through the stream's
routing, mailboxes and poll core, under the in-flight cap (`FLEZHA_MAX_INFLIGHT_ACTIVATIONS`).
Ten thousand waiting instances now cost no CPU and about 61 KiB each. A stage commits when its own
last item settles, so siblings commit independently and a sibling that fails later compensates
the one already committed. A cell's retry is the stream's per-item ladder: the failed item and
those queued behind it re-run, not the whole batch, and a stop disarms a pending backoff at once.
`FLEZHA_STEPS_DRIVER_STACK_KIB` is gone. A Java, Node, Python, Ruby or PHP steps tasklet that
declares static `cell_keys` now runs the cells its `cells_for` names, and `flz relay state` /
`flz state get` answer for a durable steps relay's cells. The journal, the wire frames and the
authoring surface are unchanged.

### Changed: a steps relay is declared like a flow relay

A steps class writes its graph in `channels`, as a flow class does, in all seven SDKs; the
steps-only `tasklets` body is gone. The runner
announces each factory's kind with its id, so `flz relay start` and `flz schedule add` take no
`--kind`, the admin start has no `kind` field, a schedule of a flow factory is refused, and a
spawn naming a flow factory answers `factory_not_steps`. The class-level `retry_attempts`,
`retry_backoff_ms` and `barrier_retry_attempts` are deleted: a steps stage's retry ladder is its
consuming edge's `retry_max` / `retry_backoff_ms`, as on a flow relay, and a commit barrier is re-sent a
fixed three times. A steps class is refused by name when it sets the effectively-once tier
(`EffectivelyOnceOnSteps`), a checkpoint setting (`CheckpointOnSteps`) or a correction window
(`CorrectionWindowOnSteps`), and a timer call inside a steps tasklet fails naming
`TimersUnavailable`. The short form of a steps relay may write its loop mode.

### Changed: the single-tasklet short form is a relay declaration; every tasklet body is `run`

The short form moves from the tasklet declaration to each SDK's relay entry point in all
seven SDKs: `#[flezha::relay]` in Rust, `@Relay` in Java, `@flezha.relay` in Python,
`defineRelay` in Node, a `Flezha::Relay` subclass in Ruby, `#[Relay]` on a class extending
`Relay` in PHP, and `use Flezha.SDK.Relay` in Elixir. A declaration is a short form when it
carries its tasklet's `run` body instead of a topology body; the identity
is the relay's factory id, and the generated tasklet is named after it, so growing into the
full form keeps every cell's state. `sink` is optional: with none, the entry channel feeds the
tasklet and no plugin channel is declared. The refusals are `ShortFormWithTopology`,
`SinkWithoutOutput` and `DuplicateFactoryId`; the three that policed the old `sink` switch are gone.
Node's `handle` and the `process` method of Ruby's and Python's class tasklets are renamed `run`,
the name every SDK now gives a tasklet's body. The conformance suite gains `sugar_state_only`, a
short form with no sink proven by its cell state.

### Removed: the one-function steps body; a tasklet ends its instance

A steps relay always runs tasklets. The one-function body `run(ctx, args)` and its context
(`emit`, `recv`, `loop_until`, `complete`, `is_cancelled`) are deleted in all seven SDKs with no
alias, together with the C ABI's `flz_core_steps_emit`, `_recv` and `_cancelled`, the
`StepsRun` activation and the `StepsDone` outcome; a steps class that declares no
tasklet is refused at registration. A chat agent is a `PerInput` steps relay whose tasklet keeps
the conversation in cell state, so a crash loses no turn.

Every tasklet context gains `complete(result)`: once the iteration commits, the instance ends
with status `ok` and `result` instead of waiting for its next input. The rest of the iteration
still runs; several completions resolve to the tasklet latest in the chain and, within it, the
cell whose key sorts last; a failed iteration discards its completion; a flow tasklet
calling it fails its input, non-retryable, naming `CompleteOutsideSteps`. The result is
encoded the way a sink naming no codec encodes a value: bytes as they are, a string as UTF-8,
anything else as JSON. The completion rides `TaskCommit` and `IterCommit`, is journaled in
`task_committed` and `iter_committed` (an entry written before this change reads as not
completing), comes back on a rebind, and `flz state journal` shows a completing entry.

### Changed: a relay starts without a name, and every start answers its id

The id is optional at every start. `flz relay start <factory>` takes the id as
`--id` / `-i`, the consumer surface's `Relay.start` takes a spec with no `id`, a tasklet's
spawn takes none, and a partitioned start generates its group base. The controller then
generates `<factory>-<16 characters>`, trimmed to the 32-byte limit, checks it with the same
atomic claim a named start uses, counts a leftover state folder as taken, and draws again at
most three times. A start without an id that is retried after a lost answer starts a second
relay, and a spawn without an id always starts a new instance; pass an id when that
matters.

Every start and spawn answers `{id, outcome}`. A start answers `started` or `already_running`
and no longer carries `relay_id`, `runner_id`, `node` or `pid`; a spawn answers `spawned`,
`delivered` or `skipped_on_replay`. `SpawnStepsResponse` gains `instance_id`. The audit
record of a start names the id it ran under.

The SDK calls take the factory first and the id as an optional named argument:
`RelaySpec::new("echo").id(..)` and `ctx.spawn_steps_with(.., SpawnOptions::id(..))` in
Rust, `RelaySpec.of("echo").id(..)` and `SpawnOptions.id(..)` in Java, keyword options in
Elixir, keyword arguments in Python and Ruby, named arguments in PHP and an options object in
Node. A spawn now returns a handle at once in all seven SDKs: Python, Ruby and PHP no longer
wait for the controller or raise its refusal into the tasklet, Node's `spawnSteps` is no
longer `async`, and a refusal is read from the handle (`wait` / `await` and `try_result` /
`tryResult`). The C ABI replaces `flz_core_spawn_steps` with `flz_core_spawn_request` and
`flz_core_spawn_result`, and the spawn handle lives once in `flz_core`.

The CLI spellings change: `flz relay start <relay_id> <factory_id>` becomes
`flz relay start <factory_id> [--id <id>]`, which prints `Started <id>` (or
`{"id":..,"outcome":..}` with `--json`) instead of `Starting relay...`; `--init_values` and
`-iv` become `--init-values` on `relay start` and `schedule add`; `-si` and the `-i` of
`relay rewind --to-iteration` go. Both verbs refuse an option they do not read.

### Added: `merge`, the latest-document router and `flat_map` in every SDK

Node, Python, Ruby, PHP and Elixir can now merge several channels into one tasklet, as
Rust and Java could: each source brings the mapping that turns its values into the one value
the tasklet reads, the key is declared once on the merge, and a merge of one source, of the
same source twice, or of a source already wired into the tasklet is refused by the same
names (`MergeTooFewSources`, `DuplicateMergeSource`, `EdgeAlreadyWired`). The spellings are
`c.merge().from(s, f).keyBy(k).into(P)` in Node, `c.merge().from_(s, f).key_by(k).into(P)`
in Python, `c.merge.from(s) { }.key_by { }.into(P)` in Ruby,
`$c->merge()->from($s, fn)->keyBy(fn)->into(P::class)` in PHP and
`Channels.merge/1` with `Flezha.SDK.Merge` in Elixir. The same five gain `route_latest`
(`routeLatest`) and its named form, and Node, Python and Ruby gain `flat_map` (`flatMap`).
Every SDK's test app carries two new fixtures, `merged_input` and `transform_chain`, driven
end to end by `test/system/stream_graph_e2e_test.exs` on whichever runtime is selected.

An input fanned by `flat_map` across several keys now reaches every one of them in the
Java, Node, Python, Ruby and PHP runners; it used to reach the first key only. Each Cell
runs only the values keyed to it, and a static consumer whose `cells_for` answers no key
runs nowhere, as in Rust.

The Elixir SDK's `Channels.sealed/1` is removed: it sealed nothing. Drop the call; the
topology is unchanged.

### Changed: the Elixir SDK is the `flezha_sdk` package, in its own repository

The Elixir SDK is a mix package of its own, `flezha_sdk`, developed in
[`flezha_elixir_sdk`](https://github.com/flezha/flezha_elixir_sdk) and present here as a
git submodule at `sdk/elixir`, with `test/apps/elixir_runner` a symlink to its test app and
the echo example moved with it. An Elixir runner now depends on `{:flezha_sdk, "~> 0.1.0"}`
alone rather than on the whole controller, and needs no `controller_enabled: false`.

The controller depends on the package and imports the modules both use rather than keeping
copies: `Flezha.SafeCall` is `Flezha.SDK.SafeCall`, `Flezha.Process.Counters` is
`Flezha.SDK.CounterTable`, `Flezha.State.Wire` is `Flezha.SDK.Wire` (its output stamps moved
to `Flezha.State.Outbox`), the key-group hash is `Flezha.SDK.KeyGroup.group_of/1`, the record
frame's pure half is `Flezha.SDK.RecordFrame`, and `Flezha.Transport.Local.Stream` is
`Flezha.SDK.LocalStream`.

The socket modes dial through the package's own NIF, which a release publishes precompiled
for the five supported triples with a checksum file; other hosts build it from source. The
controller builds none of it outside its test environment. The in-node `:native` transport
reaches the controller through the `Flezha.SDK.Connect` behaviour it registers at boot.
`scripts/check-sdk-boundary.sh` is gone: the compiler enforces the direction now.

### Changed: every SDK but Elixir lives in its own repository

The PHP, Java, Python, Ruby and Rust SDKs follow Node out of this repository:
[`flezha_php_sdk`](https://github.com/flezha/flezha_php_sdk),
[`flezha_java_sdk`](https://github.com/flezha/flezha_java_sdk),
[`flezha_python_sdk`](https://github.com/flezha/flezha_python_sdk),
[`flezha_ruby_sdk`](https://github.com/flezha/flezha_ruby_sdk) and
[`flezha_rust_sdk`](https://github.com/flezha/flezha_rust_sdk). Each is a git
submodule at `sdk/<lang>`, pinned to a commit, with `test/apps/<lang>_runner` a
symlink to its test app, and the Python and Ruby examples moved with their SDK.
Clone with `--recurse-submodules`, or run `git submodule update --init` in an
existing checkout: the cargo workspace lists the Rust SDK's crates, so no cargo
command works here without it. The Rust SDK depends on `flz_core` and
`flz_transport` at a git tag, and this workspace patches that source back to
its own crates. Every end-to-end scenario still runs here.

### Added: free-threaded wheels and precompiled Ruby gems

A release now also publishes one free-threaded Python wheel per platform for
CPython 3.14t, and one precompiled Ruby gem per platform carrying a library for
each supported Ruby minor, so installing the Ruby SDK needs neither cargo nor
git. The source gem remains the fallback and says so when either is missing.

### Fixed: a keyed local run no longer finishes before its keys are answered

`run_until_idle` could return while an input was still with the host for its
key, and closing the run then dropped it. It now waits for every key request,
including one being retried.

### Changed: the Node SDK lives in its own repository

`@flezha/sdk` is developed in
[`flezha/flezha_node_sdk`](https://github.com/flezha/flezha_node_sdk) now and is
present here as a git submodule at `sdk/node`, pinned to a commit, with
`test/apps/node_runner` a symlink to the test app inside it. Clone with
`--recurse-submodules`, or run `git submodule update --init sdk/node` in an
existing checkout. Every end-to-end scenario still runs here, against the commit
the pin names.

### Added: an SDK can be proven against the core without a checkout of it

A release now publishes what an SDK needs to test itself: the N-API addons as
the five `@flezha/sdk-<platform>` packages, the `abi3` wheels,
`flz-fake-controller` for every supported triple, and the cross-language
fixtures as `flezha-fixtures-<version>.tar.gz`. `flz-fake-controller` is the
control-socket peer the crate's own ABI scenario drives, as a command: it binds
a socket directory, walks the handshake, starts a class, feeds inputs, counts
outputs and shuts the runner down, exiting non-zero naming the first frame that
is wrong or late.

A package that builds its native binary from source now does so through a
wrapper crate it carries, whose one dependency is the core at a git tag, so a
source build needs cargo and git rather than a checkout of this repository.

### Fixed: a local run hands over every value it released

Closing a local run stopped it and discarded whatever the graph had released in
the last poll interval, so an author could miss output the run did produce. It
is worst for a keyed or dynamically keyed class. Fixed in the core and in all
five SDKs.

### Fixed: a local run fires the timers a class declares

`run_until_idle` called a graph settled while a processing-time timer was still
armed, so `on_timer` never ran. It now waits for the deadline, and an event-time
timer fires off the high-water a later input advances, exactly as under a
controller.

### Changed: the testnet lives in its own repository and streams market data

The fake exchange at `test/apps/testnet` is now the git submodule
[`flezha/flezha_testnet`](https://github.com/flezha/flezha_testnet). Clone with
`--recurse-submodules`, or run `git submodule update --init test/apps/testnet` in an
existing checkout; `mix test.setup` says so when it is missing. The testnet now ships
the trade and order book generators its subscription channels use, so a subscription streams
data instead of only being acknowledged, including in the `:latest-dev` image.

### Added: a channel names its payload format in the Ruby, Python, Node and PHP SDKs

`input` and `sink` take a named codec: `bytes`, `text` and `json` in all four,
and `msgpack` in Ruby, Python and Node. It is spelled the way each SDK spells a
policy: `codec: :text` in Ruby, `codec="text"` in Python, `{ codec: 'text' }` in
Node and `codec: ChannelCodec::Text` in PHP. A text channel no longer needs a
hand-written decoder and encoder. An unknown name, or a codec given beside a
decoder or encoder, is refused when the relay is declared, and `text` refuses
invalid UTF-8 rather than replacing it.

### Changed: an undeclared entry reads JSON, and an undeclared sink sends by the value

**Breaking for the Ruby, Python, Node and PHP SDKs.** In Ruby, Python and Node an
entry with no codec and no decoder now reads JSON instead of MessagePack, as PHP
already did. In all four a sink with no codec and no encoder sends bytes
unchanged, a string as its UTF-8 bytes and any other value as JSON, the rule Java,
Rust and Elixir already follow, so a PHP sink no longer quotes a string and the
other three no longer send MessagePack. `msgpack` restores the previous format in
one word. Payloads between tasklets of one relay, and state and change
bodies, keep their formats.

### Added: a Ruby runner can do work between starting and waiting

`Flezha::Runner#await_termination` waits for shutdown and then drains, and
`run` is now `boot` followed by it, the way `start` and `awaitTermination`
split in Java. An application can boot a runner, make its admin calls, and
then wait, which the Ruby SDK had no public way to do.

### Fixed: a relay reads healthy as soon as a restarted runner takes it

After a runner stopped and came back, its relays were bound again and answering
within seconds but reported `degraded` for about a minute, while the controller kept
retrying a repair they no longer needed. The stall a closed stream reports now clears
as soon as the relay is bound to another runner.

### Fixed: a runner stopped with Ctrl-C or SIGTERM exits cleanly

A Node runner could end with `Aborted (core dumped)` instead of exiting 0, because the
SDK core ended the process while Node was still shutting itself down. The core now
ends the process at once, once every relay's final state has shipped, without the
exit-time cleanup that raced Node's own. The change is in the core that every SDK but
Elixir is built on.

### Fixed: an application's plugin options reach the plugin

An application enabling a plugin through its runner's admin handle had every
option it passed refused as unknown, so it could attach only a plugin that
takes none. Options now reach the provider by the names it declares, and an
option it does not declare is still refused by the name it was given.

### Fixed: an admin call made right after a runner starts is answered

A runner's start returns once its connect is sent, before the controller has
taken the connection over and admitted the session. An admin call made in that
window was lost and reported unanswered after 30 seconds, or, once delivered,
judged before the runner was recorded and refused with
`no runners for factory`. The controller now keeps what a runner sends after
its connect queued until the runner's own channel takes the stream, and judges
an admin call or a steps spawn once the session is admitted. No SDK changed.

### Fixed: an Elixir runner's admin call over a socket is answered

`Flezha.SDK.Admin` from a standalone Elixir runner (`transport: :uds` or
`:quic`) crashed the runner's server on its first call, because the admin
request had no guarantee class. It is now a control frame: sent once, never
held, and a refused write answers the caller with the refusal.

### Fixed: a runner acts only on the relays bound to it

The controller now records, for every relay, the stable identity of the
runner it is bound to, and refuses a frame about that relay from any other
runner: an output, a state record, a completion, a park, a shutdown snapshot or
a commit barrier. A runner judged dead while its process was still alive kept
its copy of a relay the controller had moved elsewhere, and after it
reconnected that copy could stop the relay, suspend it, overwrite its
snapshot or rewind its WAL. The copy is now refused, counted on
`[:flezha, :runner, :foreign_frame_refused]`, and told to stop. A stable
identity presented under another runner credential while its session is held
is refused as a connect rejection.

### Added: a file plugin root can be an S3 bucket

`FLEZHA_FILE_PLUGIN_ROOT` also takes `s3://<bucket>/<prefix>`, so a file source
and a file sink keep working when their relay moves to another controller,
with no shared mount. Four settings give the connection,
`FLEZHA_FILE_PLUGIN_S3_ENDPOINT`, `FLEZHA_FILE_PLUGIN_S3_REGION`,
`FLEZHA_FILE_PLUGIN_S3_ACCESS_KEY_ID` and `FLEZHA_FILE_PLUGIN_S3_SECRET_ENV`,
and none falls back to the durability tier's; a root overlapping the tier's
bucket prefix is refused. Under an S3 root the sink takes only
`commit: rename` and holds the open part in memory, at most 5 GiB. See
`docs/plugins/file.md`.

### Fixed: an HTTP request no longer takes its caller's other messages

While the OTLP exporter or the S3 durability backend waited for a response, it
read every message its process received and dropped those that were not the
connection's. A second export sent during a slow one waited out its 60-second
call instead of being served.

### Changed: a plugin's hooks have one name, and an enable refuses an option it does not read

The point at which a relay calls a plugin is a hook everywhere a user
types or reads it. This breaks every command, spec and manifest that used the
old words:

- `flz plugin enable --extension=<name>` is `--hook=<name>`. Sub-options stay
  `--<name>.<key>=<value>`.
- `--trigger` is removed. Delete `--trigger none`; `--trigger on_input` and
  `on_output` become `--hook=on_input` and `--hook=on_output`; `--trigger
  interval` and `proactive` become `--hook=on_interval
  --on_interval.interval_ms=5000`. There is no default interval.
- `flz plugin enable` refuses a plain option it does not read, naming it,
  instead of dropping it. It reads `--namespace`, `--relay_id` and `--hook`;
  provider settings stay `--X<key>=<value>`.
- `"subscriptions"` is `"hooks"` in the `--plugins` spec of `flz relay
  start` and `flz schedule add` and in the fields of an admin `enable_plugin`
  call in every SDK. `subscriptions:` is `hooks:` in a package `manifest.exs`,
  in `Flezha.Plugin.new/1` and in `%Flezha.Plugin{}`.
- Refusals name hooks: `:unknown_hook`, `:invalid_hook`, `:invalid_hook_opts`,
  `:unknown_hook_opt` and `:invalid_hooks`. `flz relay info` prints `hook:`
  lines and its JSON carries `hooks` as `{"name", "opts"}` objects, and `flz
  plugin help <provider_id>` lists `SUPPORTED HOOKS:`.
- Controller state written by an earlier build holds plugin records this build
  does not read. Run `flz reset` before starting this build over it. A package
  installed by an earlier build is refused when enabled; install it again, or
  clear the catalog with `flz reset --include-plugins`.

### Changed: plugin help names the real provider id, and `help/0` becomes `help/1`

`flz plugin help <provider_id>` names the id the enable takes and lists only the
options a user can write. A plugin provider's `help/0` callback becomes
`help/1`, taking that id.

### Fixed: Java `@Tasklet` strings may hold quotes and backslashes

The `id`, `sink`, `input` and `keys` of a Java `@Tasklet` may hold quotes,
backslashes and control characters; the generated source now carries them
exactly instead of failing to compile.

### Fixed: `flz reset --dry-run` previews while the controller is up

A dry run changes nothing, so it no longer refuses with exit 4 against a live
controller.

### Fixed: a runner refused because its hub could not start names the start error

A runner connect refused because its namespace hub could not start now names
the start's own error in its log lines and in the
`[:flezha, :transport, :connect_rejected]` event.

### Fixed: a Java constructor that throws is reported as its own error

A Java relay, steps relay or tasklet whose constructor throws reports that
exception instead of claiming it has no public no-arg constructor.

### Fixed: two controllers started at once on a fresh state directory agree on their cookie

Two node starts racing a first boot no longer leave the cookie file holding a
value the running node did not use.

### Fixed: the Node SDK's CommonJS build loads a handler module by path

`defineRelay('./handlers.cjs')`, `declare` and worker loops no longer fail with
`could not import handler module` when the runner is required from CommonJS.

### Fixed: `flz reset` no longer empties a folder it reports as kept

`flz reset` refuses to clear a root that resolves onto, into or around a root it
keeps, such as a `dlq` symlinked to `audit`.

### Fixed: Node and Ruby runners ship their final state at shutdown

Node and Ruby runners stopped by SIGTERM or SIGINT now ship their final relay
state instead of an empty or missing snapshot.

### Fixed: a trailing channel option takes effect in Node, Python, Ruby and PHP

Node, Python, Ruby and PHP apply `named`, `keyed` and the overflow and retry
options when declared, so a trailing one is no longer dropped and a 256-byte
name is refused.

### Fixed: `flz controller stop` ends the controller

`flz controller stop` drained the controller and printed `outcome=clean`, but left
its Erlang VM running: the node kept its name, and a new start failed until the
process was killed. It now ends the VM once the drain has finished and the outcome
is printed, as SIGTERM already did. `--no-wait` ends it too.

### Fixed: Ctrl-C stops an attached controller instead of freezing it

Pressing Ctrl-C on a controller started with `flz controller start` in attached
mode gave the prompt back but left the Erlang VM paused behind its break menu. The
node kept its name and its socket, so `flz runner check` still found it, every
other `flz` command failed with `:noconnection`, and a new start could not boot
until the process was killed. The controller now runs with the break handler off,
so Ctrl-C ends it at once. It is a hard stop with no drain, recovered on the next
boot; `flz controller stop` and SIGTERM still drain.

### Changed: `flz runner check` prints one fact per line

The check printed its verdict, the socket directory, every path it checked and
the fix on one long line, and said "nothing to fix" when the controller answered.
It now prints the verdict first, then one labelled line each for the directory,
the paths checked and the fix, and leaves the fix out when a controller answered.
The one-line diagnosis a runner prints before it retries is unchanged.

### Fixed: a move to another controller no longer brings back a relay's older state

When a runner stops gracefully it hands its relays' final state to the
controller, which kept that copy for good. If the relay then ran on another
runner, took more input, and lost that runner to a crash before moving to another
controller, the move shipped the old copy and skipped reading the relay's
saved state, so every change since the graceful stop was gone. A kept copy is now
dropped as soon as another runner takes the relay over, and a move that
follows a crash reads the saved state instead.

A relay class that opted out of persistence has no saved state to read. It
no longer gets that older graceful-stop state back after another runner took it
over and crashed; it starts from its initial values, as a hard kill already does.

`RelayManager.create_snapshot/2` still answers the relay's current state,
but no longer keeps it for a later move or rebind, for the same reason.

### Fixed: a healthy runner sent input just after its heartbeat is no longer restarted

The controller judged a runner's work pool wedged when frames had been sent to it
and the dispatch count in its heartbeats had not moved. A heartbeat can only count
work that reached the runner before it, so a quiet runner handed input just after a
heartbeat looked frozen, and one such verdict ended the runner with a terminal
restart. The controller now notes when each heartbeat lands and judges a frozen
count only across a heartbeat that landed after a send. A runner that really stops
dispatching while it is being fed is still found and restarted.

### Added: a file as a relay's input and output

Two first-party plugins read and write local files. `relay_file_source` reads
a file onto an entry channel, one line per frame, follows it as it grows, and
resumes after the last committed line on a plugin restart, a move to another
controller and a controller restart. Delivery is at-least-once.
`relay_file_sink` appends one line per record and syncs each batch before it
confirms a delivery-guaranteed record; a batch synced just before a crash can be
appended a second time.

Both touch one folder and nothing outside it: `app_data/files` by default,
created by the first enable, or the folder `FLEZHA_FILE_PLUGIN_ROOT` names, which
must already exist. Every path must resolve inside it with symbolic links
followed. A file the sink creates is private (`0600`). See `docs/plugins/file.md`.

```
flz plugin enable reader relay_file_source --relay_id=op1 \
  --Xpath=events.ndjson --Xchannel_in=events
flz plugin enable writer relay_file_sink --relay_id=op1 \
  --extension=on_output --Xpath=out/alerts.ndjson --Xchannel_out=alerts
```

### Changed: `flz relay listen` replaces `io_listen`, and it ends

`flz relay io_listen input|output <relay_id>` is now
`flz relay listen <relay_id> [--input]`, with no alias. It listens to
what the relay emits; `--input` listens to what it ingests instead.

Every frame prints the metadata it carries: the channel, a tab and the payload,
and on a keyed sink a third field with its cell, seq and key. `--json` prints
one object per line carrying the whole metadata map. The listener runs for
`--duration-ms` (30 seconds unless given) and Ctrl-C ends it; the plugin it
attaches is detached however it ends, on every member of a partition group.
Before, it ran until Ctrl-C, Ctrl-C opened the Erlang break menu instead, and
every listener that ended left its plugin attached on the controller.

Listening is authorized as its own action, `relay.listen`, granted from
`app` upward, rather than as `plugin.enable`.

`flz relay send --await <ms>` attaches the listener before it delivers and
prints what came back, so one command checks a deployment:

```
$ flz relay send my_echo --text 'hello world' --await 2000
Delivered to 'my_echo'.
echo	hello world
```

### Added: a plugin's channel filter names a set of channels

A plugin that acts on the records passing through a relay could restrict
itself to one channel or to none. One destination carrying two of a
relay's four output channels had neither answer: naming one channel dropped
the other, and naming none pulled in the two the user did not want. The
workaround was to enable the same plugin twice, which opens two upstream
connections for what is one destination.

The filter now takes several names, comma-separated:

```
flz plugin enable alerts relay_kafka_producer \
  --extension=on_output \
  --Xhosts=broker-1:9092 \
  --Xtopic=alerts \
  --Xchannel=alerts,audit
```

A websocket plugin writes the same set in `channel_out`, because its `channel`
also names the entry channel its inbound frames are injected on. One name
behaves exactly as it did, no filter still matches every record, and the set
selects rather than merges: each record keeps its own channel metadata, so a
consumer downstream can still tell the two apart.

Order does not matter and a repeated name changes nothing. An empty segment is
a typo rather than a shorthand, so `alerts,,audit` and `alerts,` are refused
when the plugin is enabled, naming the option.

**Breaking for a channel whose name contains a comma, or surrounding
whitespace.** A comma in these three options now separates names, so
`--Xchannel=a,b` selects `a` and `b` where it used to name the single channel
`a,b`. Each name is also trimmed, so `--Xchannel=" alerts "` selects `alerts`
where it used to match only records whose channel carried the spaces — a plugin
relying on that stops matching after the upgrade, with no refusal to announce
it. On the entry side — `channel_in`, and
`channel` on a plugin whose provider feeds the relay, such as `http_poll`
and the websocket — several names are refused rather than quietly read as one,
since a payload is injected on exactly one channel. A caller that builds the
options itself can still pass a list, which is how a name containing a comma is
written.

### Changed: a host SDK reuses its encode buffer instead of allocating one per value

Every value crossing between two tasklets is encoded to bytes, because the
core owns the queue between them and cannot hold a JVM or a JavaScript object.
The encode is unavoidable; the work three SDKs piled on top of it was not.

The Java SDK's `KryoCodec` kept a fresh `ByteArrayOutputStream`, a fresh
`Output` and a fresh `Input` for every value. It now keeps one growable buffer
and one reader per thread. Measured in isolation, a 40-byte `String` round trip
went from 424 ns to 136 ns and a `Double` from 364 ns to 49 ns. The Node and
Ruby SDKs stopped copying bytes their own encoder had already finished. The
Python and PHP SDKs never did, and needed no change.

The same values encode to the same bytes, so nothing on disk or on the wire
moves.

**Breaking for a custom Kryo serializer that calls back into `KryoCodec`.** One
buffer per thread is shared for the duration of one call, so a serializer that
re-enters the codec would write into the buffer it is nested inside. Both
directions now refuse, naming the type. An encode nested inside an encode
produced silently wrong bytes before and is no loss. A **decode nested inside a
decode worked** before this change and no longer does: it would re-point the one
`Input` the outer decode is mid-read of. Give the nested value a `KryoCodec` of
its own. A decode nested inside an *encode* is unaffected — those are different
buffers — and a test pins that it stays working.

### Changed: one plugin `channel` option, one meaning in both directions

**Breaking for websocket plugins that filter their output with `channel`.** The
option meant two opposite things depending on which provider read it: the
`http_poll` source injected on it, while the Kafka producer and the websocket
filtered outputs by it — and because the websocket's `channel` went to the
filter, naming the entry channel its inbound frames arrive on was possible only
by hiding a `"channel"` key inside `payload_metadata`, a general label bag that
appears in no help screen.

`channel` now names the channel a plugin is attached to, in whichever
directions it works: a plugin that feeds the relay injects on it, a plugin
subscribed to `on_input` or `on_output` filters on it, and a plugin that does
both does both with that one name. `channel_in` and `channel_out` name one
direction each, for a plugin whose two names differ.

What breaks: a websocket plugin subscribed to `on_output` with
`channel: "prices"` used to send only outputs naming `prices` while injecting
its inbound frames on the relay's FIRST entry channel. It now also injects
on `prices` — which, since an entry channel and an output channel never share a
name inside one relay, means those frames are dead-lettered under that name
instead of silently landing somewhere else. **Write `channel_out: "prices"` to
keep exactly the old behaviour**, or write the pair `channel_in`/`channel_out`
to name both ends. The shipped example in
`docs/plugins/websocket_subscription.md` moved with it.

Four shapes are now refused when the plugin is enabled rather than discovered
in flight: `channel` together with `channel_in` or `channel_out`; a spelling for
a direction the plugin does not work in (`channel_out` on a plugin subscribed to
no chain extension); `channel` on a plugin that neither feeds the relay nor
is subscribed to one, because nothing would read it; and a `"channel"` key
inside a websocket's `payload_metadata` alongside any channel option. The bag on
its own keeps working unchanged. `http_poll` still requires an entry channel and
either spelling satisfies it.

### Added: the single-tasklet shorthand names its entry channel

The shorthand's entry channel used to be the literal string `input`, and that
string is not private: it is the address a user types into a plugin option
to inject on, and the name a forwarded frame has to match to reach the
relay. An author who needed to choose it had to abandon the shorthand and
write the whole topology by hand.

All three spellings now take an optional name. Java is an annotation element,
`@Tasklet(id = "…", sink = "…", input = "prices")`. Rust is an attribute key,
`#[flezha::tasklet(id = "…", sink = "…", input = "prices")]`, and an optional
name on the `flezha::relay!` pipeline head,
`input::<String>("prices") -> … -> plugin::<Vec<u8>>("k")`, written in the same
position the `plugin` terminator already takes its own name.

**Nothing changes for a declaration that writes no name.** The generated
topology is byte-identical to today's, entry channel included, so every
existing relay, the cross-SDK conformance table and every fixture are
untouched. Only Rust and Java have this shorthand; the other five SDKs have
none to extend.

A name written on a spelling that can omit its `sink` is refused at compile
time, naming both — the Java processor generates no relay without a `sink`,
and the Rust attribute already refused a `sink` without an `id` the same way.
The `flezha::relay!` pipeline cannot omit its terminator, so it owes no such
refusal; it does refuse an empty name, which would declare a channel nothing can
address.

### Changed: `sink("x")` names the channel `x`, in every SDK

**Breaking for every SDK.** A sink used to name a plugin attachment point and
leave the channel named after the producing tasklet, so `sink("items_output")`
shipped frames stamped `channel = "accumulator_tasklet"` while its author read
`items_output` in the source. The string an author writes on a sink is now the
channel's name, everywhere a channel name appears: wire metadata, journals,
diagnostics, the address a plugin filter matches, and the name a local-mode
callback registers under.

`sink()` with no name is available in every SDK and changes nothing: the channel
keeps the name it would have had without a sink at all — a `named(...)` override
if the declaration carries one, else the producing tasklet's identity. Java
takes an overload; Node tells a string from a function in the first position;
Python, Ruby, PHP and Elixir default the argument; Rust, which has neither
overloading nor default arguments, spells the four combinations `sink()`,
`sink_named(name)`, `sink_with(serializer)` and `sink_named_with(name,
serializer)` — so the two existing Rust methods both change shape.

**Two declarations stop registering.** A channel named by both `named("a")` and
`sink("b")` is refused as `ChannelNamedTwice`, naming both, and is refused when
the two strings agree as well: an exception for the consistent case would leave
a redundant pair with a rule nobody remembers. And two channels naming one sink
is now a duplicate channel name, refused as `DuplicateSinkName`. Nothing is lost
in behaviour, only in expression — the shared string was read by no runtime
path, so "two channels sharing a sink" has only ever meant two channels a plugin
did not filter apart, and that shape is still written the same way by giving
each channel its own name and the plugin no channel filter.

Both code generators stop emitting the `.named(x).sink(x)` pair they needed to
get the naming their users expected, so a sugared relay and a hand-written
one now produce the same channel name for the same declaration. That
disagreement is what this change exists to close.

**Migrating.** A declaration reading `.named("x").sink("y")` keeps the channel
name it has today by becoming `.sink("x")` — the sink's own string was read by
nothing. One reading `.sink("y")` alone renames its channel from the producing
tasklet to `y`; write `.sink()` to keep the old name. The compiler cannot help
with the second case in any SDK, and a plugin filtering on the old derived name
stops matching, so sweep every `sink(...)` in a codebase before deploying.

**Durable records written before the rename carry the old channel name as
data**, and a replay re-emits them under it. Three consequences, all of them
only for a channel whose name actually moves:

- A dead-letter entry outside the effectively-once tier is keyed on
  `channel:sha256(payload)`, where the channel is the one that fed the parking
  tasklet. Rename that channel and a payload parked before the upgrade,
  re-sent or re-parked after it, computes a different entry id and lands as a
  second row instead of deduplicating against the first. Drain the dead-letter
  queue for the affected tasklets before upgrading. Under the tier the key is
  `cell_key@origin_offset` and carries no channel name, so it is unaffected.
- A steps instance that straddles the upgrade has both names in the staged
  output set its rewind guard reads back from the journal. The guard's verdict
  is unaffected — it decides by tasklet index, and the names are only what
  the refusal reports — but a refusal spanning the upgrade names channels under
  both spellings.
- A plugin filtering by channel sees nothing for replayed records under the old
  name. Keep the old name with `sink()` if that matters more than the new one.

A relay whose channel names do not change — every declaration that keeps
its name through the migration above — needs none of this.

### Removed: the steps relay handoff codec declaration, from the Rust and Java builders

**Breaking for the Rust and Java SDKs.** `codec()` and `codec_with(...)` are
gone from the Rust stream handle, `codec()` and `codecWith(...)` from the Java
one. A chain that calls any of them no longer compiles: there is no shim and no
deprecation window. Delete the calls; nothing replaces them.

The runtime derives the journal codec for every inter-tasklet channel now,
which is what every host over the erased poll core already did. In Java the
methods never did anything at all — the handoff bytes have always come from the
instance's own Kryo codec, and the declared encoder was written down and never
read back. In Rust the codec is derived from the producing tasklet's declared
`Output` type, at the moment the channel is created.

That makes the Rust `Tasklet::Output` and `SimpleTasklet::Output` types
**writable to bytes and readable back**, unconditionally, the way `State`
already was. A tasklet whose output has no serde derives stops compiling, with
a message naming the type and the journal rather than serde. The bound cannot be
narrowed to steps relays: whether a class journals is a user's setting at
deploy time, not a fact the compiler has. Three types in this repository failed
it and gained the derives — `TickInput`, `Ticker` and `Candle`, all in test
fixtures — and nothing else moved. (Two more, in a compile-fail fixture, gained
them so that its snapshot stays about the input-type mismatch it is there to
pin.)

Two registration refusals change with it. `MissingInterTaskCodec` is deleted —
no channel can be missing a codec now — and `LoopTerminalWithoutCodec` becomes
`LoopTerminalWithoutHandoff`, which asks the question that is left: a durable
chained loop seeds each iteration from its terminal tasklet's journaled
handoff, and only an **inter-tasklet** channel carries one.

**One shape stops registering.** A durable `Chained` loop whose terminal
tasklet's only output is a plugin sink is now refused in every SDK. The
runtime attaches a journal codec to inter-tasklet channels and to nothing
else, so that loop shipped an empty handoff at its iteration commit and a
crash-resumed run read the empty seed as convergence and stopped — truncating
the run with no error. In Java, Node, Python, Ruby and PHP that was already the
behaviour, because a declared codec never reached the core; in Rust the shape
worked if the author had written `.codec()` on that channel.

Two ways out. Take the sink off the terminal's own channel — a channel with a
producer and no consumer is legal, and is exactly what a retained loop output
already is — or declare `PerInput`. Simply wiring another tasklet behind the
sink does not help by itself: that tasklet becomes the new terminal and the
same rule applies to it.

Nothing else about the journal moves: the codec still rides an inter-tasklet
channel and no other kind, and the same value produces the same postcard bytes,
so a journal written before this change re-seeds after it. There is no wire
change. The Node, Python, Ruby, PHP and Elixir SDKs never offered the method and
change no code.

### Removed: the entry-input idle signal, from every SDK

**Breaking for every SDK.** The per-entry-channel idle signal is gone: the
accessor `last_entry_delivery` / `lastEntryDelivery`, the registration
`on_entry_idle` / `onEntryIdle`, the `IdleObserver` type each SDK carried, and
the Elixir `start_link/1` option `idle_observers:`. Calls to any of them no
longer compile, import or resolve, which is the intended trigger: there is no
shim and no deprecation window.

The C ABI loses `flz_core_entry_last_delivery_nanos`,
`flz_core_idle_watch_new`, `flz_core_idle_watch_poll` and
`flz_core_idle_watch_free`, and with them the `FLZ_IDLE_*` status constants. A
host binding that declared them by hand must drop those declarations.
`flz_core_monotonic_nanos` stays: it is the clock every host's `ctx` reads.

`entry_idle` also leaves the conformance vocabulary and the required capability
set, so a `conformance.json` that lists it under `fixtures` or `deferred` now
fails the manifest gate.

Nothing in the tree consumed the signal. The nearest supported replacement is
the controller's own input-silence watchdog: set `input_silence_timeout_ms`
(off by default, `docs/configuration.md`) and it emits
`[:flezha, :state, :input_silence]` and one `[watchdog]` warning for a
relay class that has gone quiet.

**It is a coarser instrument, and the difference matters if you were using the
removed signal.** The watchdog counts durable Change frames, not arriving
input, and it latches per relay class rather than per entry channel. So:
a relay with two entry channels whose first feed dies while the second
keeps folding state is never flagged, because the class keeps writing changes;
a relay that has never persisted a change is never watched at all, since
the sweep only considers a class whose `last_persisted_seq` has moved; and only
flow relays are swept, because a steps relay is quiet between stages by
design and a checkpoint-acknowledging class is quiet between checkpoints. If you need
per-channel silence, you now have to observe it in your own tasklet, from the
frames it receives.

Plugin providers keep their own silence handling: the websocket's
`idle_timeout_ms` and `resubscribe_after_ms` are untouched.

### Changed: a frame naming a channel that is not an entry channel is parked

An input frame whose `channel` metadata names something that is not one of the
relay's entry channels — a name it declares nowhere, or one that is really a
sink or an inter-tasklet channel — used to be thrown away with one line on the
runner's stderr, and, on the Elixir SDK, delivered to whichever entry channel
happened to be first. It is now parked in the dead-letter store with its payload
bytes, under the name it asked for, and an error naming the entry channels the
relay does declare:

```sh
flz dlq list --token "$FLEZHA_TOKEN"
# … channel: not_prices   error: no entry channel `not_prices`; the relay declares: config, prices
```

The park is immediate: the name will not resolve on a re-attempt, so the entry
records one attempt and no retry delay is waited. `flz dlq replay` re-injects
the payload on the recorded name, so once the relay declares a channel of
that name the parked frames reach it.

Two behaviour changes ride with it. The Elixir SDK's default entry channel — the
one a frame carrying no `channel` key takes — is now the **first declared**
entry channel, as it already was on every other SDK; it used to be whichever
declared name sorted first, because the channels live in a map. Declare `volumes`
before `prices` and an unaddressed frame now reaches `volumes`. And a parked
entry that resolved no channel is stored under a fixed `_unresolved` identity
rather than under the name the frame asked for, so `flz dlq list` groups them
together; the name the frame asked for is still the entry's `channel`, which is
what `flz dlq replay` re-injects on.

The dead-letter store also stops taking `.`, `..` or an empty identity component
as a directory move: each becomes underscores. No parked entry written by a
released version is affected, because until now every one of those components was
an author-declared channel or tasklet name.

The park is only as good as the transport under it, and that has not changed:
with no controller transport installed the core logs the payload and drops it,
and each park holds a thread until the controller acks, so a source misconfigured
at a high rate parks a thread per frame. Those limits are the dead-letter path's,
not this change's, and they are unchanged by it.

**Breaking for the Elixir SDK.** Anything relying on an unresolved name landing
on the first declared entry channel now sees a park instead. A frame carrying no
`channel` key at all is unchanged: it still takes the first declared entry
channel, which is what a source plugin sends by default.

### Added: an identity holds several roles, and a `stopper` role to combine

`flz role bind <principal> <role> <namespace>` now ADDS a role instead of
replacing the one the principal held, and an action is allowed when any role it
holds grants it. `flz role unbind <principal> <role>` removes one role, and
`flz role unbind <principal>` still removes every one. `flz role list` prints
every role an identity holds on one line, which for an identity holding one role
is the line it printed before.

The new `stopper` role grants `runner.connect` and `relay.stop` in its
namespace and nothing else, so an application bound to both `spawner` and
`stopper` starts, sends and stops its own work and still enables no plugin:

```sh
flz role bind runner@team-orders spawner team-orders
flz role bind runner@team-orders stopper team-orders
```

As with `app` and `spawner`, `flz token issue` issues no credential for
`stopper`: a runner proves its namespace with a `runner` credential.

Two behaviour changes come with it. A bind naming a namespace other than the one
the principal already holds roles in is now REFUSED, naming that namespace;
before, it moved the identity and silently dropped the role it held, which also
let an admin of one namespace take a binding out of another. Moving an identity
is now an unbind followed by a bind. And a runner principal bound only to a
user's role (`viewer`, `ops` or `admin`) now connects as `runner`;
before, its connect was refused while a live session kept that role's rights
until it reconnected. A connection holds only bound roles that grant
`runner.connect`, which is what keeps the administration surface's namespace and
quota mutations out of reach of runner code.

Every act record and every authorization denial now carries
`metadata.caller_roles`: the roles the caller was judged with, or no value when
the refusal came before any role was resolved. No existing key changes, and a
record written before this reads as one whose roles were not recorded.

Tokens are unchanged: a caller presenting one is judged by the token's own role
alone, whatever the store holds for its principal. Existing role bindings load
as a set of one with no migration step.

### Fixed: the Java SDK's POM no longer pulls in the modules its jar already holds

`com.flezha:flezha-sdk` is one jar that already contains `flezha-sdk-api`,
`flezha-sdk-runtime`, `flezha-sdk-native` and the libraries they use, but the POM
installed beside it still declared the three modules as dependencies. An
application that depended on it resolved those jars as well, so every SDK class,
and Jackson and Kryo with them, reached its classpath twice, and a module jar
newer than the fat jar could put an old and a new copy of one class side by side.
The installed POM now declares no dependencies, so the fat jar is the only copy on
an application's compile and runtime classpath. Nothing an application imports
moves: those classes are still inside the jar. An application that shades the SDK
into its own jar also stops getting an overlapping-classes warning for each of
those jars.

### Changed: a websocket plugin's connection warning names the retry and its wait

The `Websocket error:` warning a websocket plugin logs when its connection fails
now ends with the retry it is about to make and how long it waits first, such as
`; reconnect attempt 1 in 250 ms`, so a retry that has reached its 30 second
ceiling no longer reads as a fixed interval.

### Fixed: one slow reader no longer loses a QUIC runner its connection

A stream reader that fell behind on a runner's QUIC client blocked the thread
that serves the whole connection, so the keepalives stopped too and the
controller declared the runner dead while the runner reported nothing wrong. A
reader 64 chunks behind now pauses only its own stream, and QUIC's flow control
asks the controller to slow down on it. Every other stream, the control channel
included, keeps moving, and nothing is dropped or reordered.

### Fixed: a runner the controller refuses backs off instead of reconnecting at once

A runner the controller refused at connect reconnected with no pause at all,
about 2 600 times a second, enough for one runner to keep the controller
refusing it busy on two and a half cores. Every SDK now waits longer after each
refusal, on the same ladder it already used when the controller could not be
reached, never more than 30 seconds apart; a session the controller admits puts
it back on the shortest wait.

### Fixed: `flz namespace delete` refuses a namespace that does not exist

Deleting a namespace the catalog did not hold reported success, so a typo in a
teardown script read as a delete. It now prints
`error: namespace delete: :not_found` on standard error and exits with status 1,
the way `flz namespace create` already refuses a name that exists.

### Fixed: a namespace that loses its runner no longer stops its hub starting relays

A durable relay whose namespace had no runner made the hub's periodic
reconcile fail inside the worker that starts that namespace's relays. The
worker died with it, every start queued behind it was stranded, and the same
failure came back every 30 seconds without healing. The reconcile now starts
such a relay without a runner, where it waits for one to connect, and the
namespace's other starts go through.

### Fixed: an in-node Elixir runner answers a refused data-stream bind instead of crashing

An Elixir SDK runner started with `transport: :native` crashed with a
`MatchError` when the controller had no free channel for one of its data
streams, and the controller saw a `ConnectSocket` reply that never came. The
runner now answers `ConnectSocket` with the error and closes every stream it had
already bound in that round. A UDS or QUIC Elixir runner refused part-way
through the same round now closes the streams it had opened too, instead of
leaving them open.

### Fixed: the Elixir SDK's schema refusal names the floor the Tasklet declared

An Elixir Tasklet whose `min_readable_schema_version/0` is above its own
`schema_version/0` reads only that version, and a record below it was refused
naming the version instead of the declared floor. It now names the declared
floor, as Rust and Java already did: a Tasklet at version 2 declaring floor 5
refuses a record stamped 1 with `record stamped schema_version 1 is below the
readable floor 5 the running tasklet declares`. What is refused is unchanged.

### Removed: the merge-gated bench no longer fails on a throughput number

The `bench-regression` job failed a pull request whose regression bench read
below 432 000 messages a second on the receive window, 0.9 of a floor recorded
on one desktop. That number was arbitrary, and noisier than the band around it.
The job still runs the bench and now writes the end-to-end and receive-window
rates to its summary; it fails only when the bench itself fails or leaves no
report with a rate in it. The job and workflow keep their names, so a
branch-protection rule that requires the check still finds it.

`mix bench.floor`, which printed the recorded floor for that job, is gone, and
`docs/operations/benchmarking.md` no longer records a regression floor.

### Changed: `flz reset` can clear every store under the data root, and names everything it keeps

`flz reset` left `app_data/dlq/`, `app_data/schedules/`, `app_data/auth/`,
`app_data/audit/` and `app_data/plugins/` behind and did not mention them, so
a relay re-created after a reset could meet its own old dead-letter
records, and old schedules fired against a fresh state tree. Five new flags
clear them: `--include-dlq`, `--include-schedules`, `--include-auth`,
`--include-audit` and `--include-plugins`. A plain reset still keeps all five.

**`flz reset --yes --all` now clears all five**, as well as the certificates
and sockets it cleared before. That means it:

- **removes every credential.** Every token, runner credentials included,
  every role binding and every namespace but `_system` is gone. With
  authorization enabled (the prod default), the next start writes a fresh
  bootstrap token to `<auth_dir>/bootstrap.token`. Until it is claimed with
  `flz auth bootstrap <token>`, administration verbs are refused and every runner is
  admitted with no credential into any namespace but `_system` it names, as on
  a fresh install, so claim it at once; then bind roles and issue runner
  credentials again.
- **erases the audit trail.**
- **removes every installed plugin.**
- clears parked DLQ records and every schedule.

A script that runs `flz reset --yes --all` and meant to keep any of these
should name the flags it wants instead. With a remote durability backend
configured, the next boot restores the auth store and the schedules from it,
unless the backend is set to `none` for that boot or its prefix is purged
first.

The report now names every store a reset leaves behind, on every run and in
`--json`: each of the five with the flag that clears it, such as
`use --include-auth to clear`, and `logs/` with `not reset's to clear`. No flag
clears `logs/`: it is named relative to the directory the controller ran in,
and reset cannot tell which directory that was. Like every other root, the
five are `<app_data root>/<name>`; a store moved with its own variable, such as
`FLEZHA_DLQ_DIR` or `FLEZHA_AUTHZ_DIR`, is not found there.

A script that reads `--json` sees two differences. The `kept` list is never
empty any more: under `--all` it still names `logs`. And both lists follow the
layout's order (defaults, then the flag rows with the socket directory last
among them, then `logs`), so the socket entry is no longer the fourth root.

### Fixed: a `flz` verb run during a controller's first start no longer locks the CLI out

With no `RELEASE_COOKIE`, the release script minted a node-local cookie for every
release command, the `flzvm rpc` behind each `flz` verb included. A verb run in the
same instant as a controller's first start, such as `flz boot await` right after
`flz controller start &`, could write a second value over the one the controller
kept, and every later verb was refused until the controller restarted. Only a
command that starts a node (`start`, `start_iex`, `daemon`, `daemon_iex`) now
creates the cookie file; every other command only reads it. A verb run before the
first start reports the controller unreachable, which `flz boot await` waits out.

### Changed: a controller refuses to boot on a socket directory over 60 bytes

A controller whose UDS socket directory was 61 to 79 bytes booted, because its control
socket fitted, and then refused every runner with a long id at connect, which is every
runner in a namespace of 11 bytes or more: a runner's data socket name is up to 46 bytes,
and the whole path must fit the 107 bytes a Unix domain socket address holds. Such a
controller now refuses to boot, before it removes anything in the directory, with a
reason that names the directory, its length, the address limit and the variable:

    the socket directory /srv/flezha/.../sockets is 61 bytes, which leaves no room for the
    data socket of a runner whose id is 24 bytes, the longest a socket name carries: the
    socket path ... is 108 bytes; a Unix domain socket address holds 107. Set
    FLEZHA_SOCKET_FILES_PATH to a shorter directory.

A deployment that booted from such a directory and served only runners with short ids
now stops at boot: set `FLEZHA_SOCKET_FILES_PATH` to a directory of at most 60 bytes.
The socket ladder's own candidates fit on an ordinary host, and so does the repository
fallback `app_data/sockets` while the controller's working directory is at most 43
bytes.

### Added: a `spawner` role, so a namespace can spawn steps relays without the rest of `app`

With authorization on, a tasklet's `ctx.spawn_steps` needs the runner's
`runner@<namespace>` principal to hold the start and the delivery a spawn performs.
Until now the only role that held them was `app`, which also lets the namespace's
application code stop relays and enable or disable plugins. A `spawner` binding lets
the runner's own connection start and send to relays in its namespace, through a
tasklet's spawn and through `Relay.start` and `Relay.send` on the SDK's
administration surface, and nothing else: no stop, no plugins, no schedules, no other
namespace and no token:

    flz role bind runner@<namespace> spawner <namespace>

`app` keeps everything it had, spawns included. `flz token issue` refuses `spawner` as it refuses `app`: both are
roles for a runner's principal, and a runner proves its namespace with a `runner`
credential.

### Fixed: `flz boot await` waits for a controller that is still starting

For a few seconds after `flz controller start --mode daemon` the controller cannot be
reached, and `flz boot await` failed at once with `--rpc-eval : RPC failed with reason
:noconnection`, so a script that started a controller and then awaited it failed unless
it retried the await itself. `flz boot await` now keeps asking inside the same
`--timeout` until the controller answers, printing nothing meanwhile, and the
controller's verdict is the answer. A controller it did reach is still asked once, and
`--timeout 0` still asks once. When the timeout runs out with the controller still
unreachable it exits 1, as a timeout always has, with a line saying what that can mean:
the controller is not up yet or stopped after it was reached, or the shell's cookie or
node name is not the controller's. The failed connect reads the same for all of them, so
a wrong cookie or node name, or a controller that exited during its boot, is now reported
when the timeout runs out rather than at once, and a controller that refuses the cookie
logs two `Invalid challenge reply` errors per attempt meanwhile, about four a second,
where the single attempt used to log two.

The first-deployment guide now starts the controller with `--mode daemon`. Without it
`flz controller start` stays in the foreground, and the guide's next line, `flz boot
await`, never ran.

### Fixed: a topology refusal reaches a Node, Ruby or PHP author under its own name

A Node relay builds its graph while its module loads, so a refusal its builder
raised there reached the author as `could not import handler module ...`, with the
refusal's name only on the chained cause. A declaration refusal thrown while the module
loads now reaches the author as itself. Every other failure to load, a syntax error or
a missing file, is still reported as `could not import handler module ...` naming the
module, with the original error chained.

The one refusal the Node, Python, Ruby and PHP builders raise under a name from the
core's vocabulary, `NodeIdentityCollision` for two different tasklets claiming one
node name, carried its name only in the error's `refusals` list. Its message now reads the way the core's
refusals do, for example `dup_node: plain: topology rejected [NodeIdentityCollision]:`
followed by the sentence on its own line, so a Ruby or PHP runner that dies on it
prints the name.

### Fixed: the controller's log names why a runner's tree could not start

When the supervision tree of a connecting runner failed to start, the controller still
asked that tree's supervisor for the runner's control channel, found no supervisor, and
refused the connect as `{:runner_supervisor_down, {:noproc, ...}}`. The refusal named a
missing process rather than the failure, such as a socket directory too long to hold the
runner's data socket. The refusal now carries the start's own error, in its
three log lines and in the `[:flezha, :transport, :connect_rejected]` event, for example
`the socket path ... is 113 bytes; a Unix domain socket address holds 107. Set
FLEZHA_SOCKET_FILES_PATH to a shorter directory.` The refusal keeps its
`{:runner_supervisor_down, reason}` shape. The runner itself is told nothing new: its
stream is closed, as for every refused connect.

### Fixed: an idle controller no longer spends a third of a core on its fsync tick

A controller with no runner and nothing to write used about 37 % of a CPU core. The
fsync batcher's 5 ms tick re-armed itself whether or not any write was waiting, so an
idle node woke about 167 times a second, and the scheduler busy-waited after each wake.
The tick now runs only while a write is waiting for its fsync, and an idle release
measures about 2.6 % of a core. For a write that arrives after a quiet spell the tick
falls due at the next point of the rhythm the last tick set, never later than before,
and ticks that keep finding work keep the rhythm they had. On an otherwise idle machine
the timer fires about half a millisecond late on average, because the core it runs on is
now allowed to sleep. `agent_fsync_interval_ms` keeps its meaning.

### Fixed: a burst to a QUIC runner arrives at once again

Earlier in this release a runner's QUIC client stopped letting one slow stream reader
stall its whole connection: a reader 64 chunks behind now pauses only its own stream,
and wakes the client when it has room again. That wake could be lost. When more than
64 chunks arrived at once for a reader that was idle, the reader never woke the
client, and the rest of the burst waited for the client's next timer, up to the 5 s
keep-alive. The reader now wakes the client whenever it takes a chunk from a full
queue, including a read that was already waiting when the burst arrived, so the
burst arrives at once.

### Fixed: a partition group keeps its members across a registry replay

After a reconcile replay, whether inside a running hub or on a warm restart, a running
partition group answered no members: the replay wrote each member's registry row without
the partition identity the start had put there. Input forwarded to the group went to its
base id, and a plain start could take a running group's id. A replay now keeps every key
of the row it replaces. The fix is in process_hub 0.7.1, which this release needs.

### Changed: a spawn a tasklet requests is judged like any other start

`ctx.spawn_steps` started a steps instance, or fed a running one, without asking
whether the runner's namespace was allowed to, and left no audit record of who asked.
It is now judged as the identity the runner's connection proved. A start is judged the
way `flz relay start` is and writes the same `start` record, naming
`runner@<namespace>`. A delivery to a running instance is judged the way
`flz relay send` is but recorded only when refused, because it happens once per
event.

With authorization on, the default in a release, a namespace whose tasklets spawn
steps relays now needs `flz role bind runner@<namespace> spawner <namespace>` (or `app`,
which adds stop and plugin enable and disable). Until it has one, every start or delivery
a tasklet asks for resolves `Refused`, with the refusal as its reason. With
authorization disabled nothing is refused, and a start is still recorded, as every
other start is.

### Fixed: `flz role bind` binds the `app` role

`flz role bind` refused `app` as an unknown role, so the binding an application needs
to administer its own namespace, and now to spawn steps relays, could not be made at all.
The documentation also wrote the command with the role and the namespace swapped. It
is `flz role bind runner@<namespace> app <namespace>`, and it binds. `flz token issue`
still refuses `app`: a runner proves its namespace with a `runner` credential, and no
credential is issued for `app`.

### Fixed: a websocket sink's re-sends show on the backoff telemetry

Under `sink_ack_mode: "required"` the websocket plugin re-sends a frame the
upstream has not acknowledged, on a ladder that starts at 1 s and gives up after
six sends. That ladder never reached the backoff telemetry. Each re-send now
announces the wait it served on `[:flezha, :backoff, :wait]` under ladder
`:ws_sink_ack`, and the acknowledgement of a re-sent frame announces
`[:flezha, :backoff, :reset]`. A frame acknowledged on its first send announces
nothing.

### Fixed: a runner session is admitted only once its handshake completes

A runner whose data sockets could not be connected was recorded as connected
anyway, and stayed that way with no data plane: the controller answered the
connect before the runner had. The controller now waits for the runner's own
answers, without holding up any other connect, and ends a session the runner
refuses, does not answer within `control_unit_reply_timeout_ms`, or leaves before
answering. On the default UDS transport such a connect used to crash the runner's
control thread instead of refusing; it now refuses on both transports.

The reconnect backoff now resets only on a completed handshake, so a controller
that admits a runner and then fails it is retried with backoff rather than at
once. A lost data stream no longer makes a runner reconnect: the controller
rebinds the data plane inside the live session, up to three times, and ends the
session after that. A disconnect now tears the runner's processes down even when
its namespace has already left the controller's runner list.

### Changed: a namespace name may contain an underscore

`flz namespace create java_test` was refused with `:invalid_namespace_name`, and
so were a role binding and a runner token for that name. With authorization on,
a runner had to use `java-test` instead, although the SDKs accept either and
nothing in the controller needs the ban. Names now match `[a-z][a-z0-9_-]{0,62}`:
an underscore is allowed after the leading letter, and a name still cannot start
with one, which keeps that prefix for system namespaces such as `_system`.

### Changed: the controller reports ready in seconds instead of after 45

`flz boot await`, `GET /readyz` and the image health check held every controller
at `recovering` for about 45 seconds after start, a single node with nothing to
wait for included. A namespace hub stayed in recovery until its first reconcile
round, and only the `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS` timer opened it.

The round now opens once the hub has heard from every peer it is connected to,
after a short settle window, so a controller with no peers is ready about two
seconds after start. The new `FLEZHA_PROCESS_HUB_CLUSTER_SETTLE_MS` (default
`2000`) sets that window; `0` suits a deployment that forms its cluster before
the controller starts. The grace keeps its `45000` default and now only caps the
wait on a connected peer that never answers. Nothing in that window waits for a
runner: a relay whose runner has not reconnected is started unbound and
bound when the runner arrives.

This needs process_hub 0.7.1. Against 0.7.0 the new setting is ignored and the
wait is unchanged.

### Fixed — an enabled plugin survives a controller restart

A plugin enabled with `flz plugin enable` was silently lost when the controller
restarted. The relay came back up, reported healthy, and carried no plugins;
the data path stopped with nothing logged anywhere.

The enable recorded the plugin in the hub's process registry row and nowhere
else. That row is on disk and is copied to every node, but a controller with
auto-restore on never reads its registry back after a restart: it rebuilds each
relay from the hub's declared list instead, and the declared list is written
when a relay is created and when it is deleted, so it never learned about
the plugin. The self-healing loop that should have noticed reads the same row the
restart had just overwritten, so it saw nothing to fix.

A plugin mutation now reaches the declared list as well, through the same
authority that starts and stops relays, and a restart rebuilds the relay
with the plugins it was last known to have. Disabling a plugin removes it from
the same record, so a restart no longer switches one back on. An enable that
fails part way now removes what it wrote instead of leaving a plugin that no
command can see.

**Upgrade note.** A plugin enabled before this release is recorded only in the
row that is never read back, so the first restart after upgrading still loses it.
Run `flz plugin enable` once more for each plugin already enabled; from then on
it survives on its own.

### Fixed — a frame is refused rather than written with a truncated length

The frame encoder wrote each metadata key's and value's length into one byte and
the table's into two, all as unguarded casts. A value over 255 bytes wrote a
wrong length, the reader took that many bytes and resumed at the wrong offset,
and every remaining pair in the frame was misread. Nothing detected it: the
frame was small and well formed by every check the decode side applies, so it
parsed cleanly and carried pairs the sender never wrote.

The encoder now refuses a key, a value or a table longer than the field that has
to carry it, naming the offending key and the length. One encoder serves both
directions, so frames the runner builds and frames the controller builds are
covered by the same check. A heartbeat that cannot be framed skips its tick; a
data emit that cannot be framed fails as non-retryable rather than shipping
something corrupt.

A channel name rides every frame its channel produces, so an over-long one is
now also refused at registration, where the author can still see the
declaration. `ChannelNameTooLong` joins the reference refusal vocabulary and its
fixture row, so every SDK renders it the same way.

### Changed — the dead-data-flow watchdog is now an input-silence sweep, off by default

The Watchdog reported a relay as a catastrophic data-flow stop when it had
produced output at some point and then received nothing for 60 seconds. That is
idleness, not a fault: a stream fed hourly, nightly, or by user activity is
silent for far longer in normal operation, and the detector cannot tell that
from a dead upstream. It logged at `critical`, the level that should mean the
controller itself is in trouble.

The sweep is renamed for what it measures, reports at `warning`, and its message
now states the observation instead of a conclusion. It is off by default, and a
user who wants it back sets a positive window. **Breaking** for anyone
reading the knob, the event or the metric derived from it:

| Was | Is |
| --- | --- |
| `catastrophic_dataflow_timeout_ms`, default `60_000` | `input_silence_timeout_ms`, default `0` (off) |
| `[:flezha, :state, :catastrophic_dataflow_stop]` | `[:flezha, :state, :input_silence]` |

The old names are not accepted.

### Fixed — a `flz` failure reaches the user instead of the Controller's log

Every refusal and every error a `flz` verb rendered was written to the
Controller's log. The user saw an exit trace and an exit code, and nothing
that said what was wrong or what to do about it.

A verb runs on the Controller, over rpc. Its standard output already travelled
back to the caller, because a remotely spawned process inherits the caller's
group leader. Standard error did not: it is a process registered per node, and
the one a verb reached was the Controller's. The failure is now written to the
caller's, and a refused verb exits in a way that no longer prints a
language-level trace over the sentence it just rendered. No exit code changes.

Three surfaces were losing text this way: every refusal and rendered error,
`flz boot await`'s verdict line, and `flz controller stop`'s drain progress.

### Fixed — the bootstrap refusal names the token file

A gated verb refused while the bootstrap is unclaimed said the token path was
logged at Controller boot. It is logged only on the boot that writes the file,
so a user on any later boot was sent to a line that was not there. The
refusal now carries the path.

### Added — a publication-readiness record and its gate

`docs/publishing/readiness.md` records every decision the first public release makes
irreversible: 32 entries, 23 of them blocking. An entry is blocking when settling it
differently afterwards would need an action outside the project's control, an act that
cannot be undone, or a compatibility break for someone already depending on the answer.
Everything else is owed. The record states rules with checks, never the instances that
currently trip them.

`scripts/check-publish-readiness.sh` gates it in two arms. The shape arm runs on every
pull request and judges that each entry parses and carries a class, a door, a rule, a
check and an owner, that ids are unique, and that a settled entry names its evidence. The
release arm runs on a version tag, before any release artefact is built, and refuses the
release while a blocking entry is open. An owed entry never stops a release.

### Added — local mode in the Node, Python, Ruby and PHP SDKs

Every SDK can now run a declared relay graph in its own process with no
controller. `LocalRelay` takes the relay class you deploy, runs the graph
it already declares, hands sink output to a callback and blocks until the graph
is idle. State is held in memory and nothing is durable; every run says so.

Each SDK gains a worked example and a test that reads the same recorded scenario
the Rust, Java and Elixir suites read, so the cores are compared against one
expectation rather than by inspection. The PHP host has one thread, so its
`runUntilIdle` drives the poll loop rather than parking in the core.

Ending a local run no longer waits out the executor's drain deadline: the host
stops fetching on its own flag instead of on the core's word, which local mode
never gives. Node, Python and Ruby each gained the `stopFetching` seam the Java
SDK already had.

### Changed — a test fixture, a CI toolchain step, a gate and a test tier are each declared

A test declares the fixtures it needs (`use FlezhaTest.Case, fixtures: [...]`)
rather than building a temporary directory, an environment key or an auth store in
its own `setup`, and a module that builds one anyway fails a gate. CI installs
every toolchain through the composite actions under `.github/actions/`, which read
one version set in `.github/toolchains.json`; a steps relay that sets a toolchain up
itself or writes a version literal fails a gate. Node and Python each had two live
values across the steps relays and now have one.

Every repository gate is a row in `scripts/naming_gate.py`, which owns running it
and one exit contract: `0` clean, `1` a finding, `2` a tool the gate needs is
absent, `3` a tool ran and reached no verdict. CI calls the runner once instead of
listing gates by hand, so a gate cannot be added and left uncalled. `scripts/gate_lib.sh`
and `scripts/format-check.sh` are removed; the local gate is now
`python3 scripts/naming_gate.py --run @format`.

Every test tier is one row in `FlezhaTest.Tiers` saying where it runs, what it
needs, which transports it covers and where it is documented, and
`docs/sdk/conformance.md` publishes the table. A tier that no steps runs and
gives no reason is a failure, as is a tag spelled like a tier selector that
nothing selects. Ten such tag names were removed: each of their modules already
declares the same axis as a `capability:`.

The Rust workspace pins every shared dependency once in `[workspace.dependencies]`,
and every crate takes its version from the workspace.

### Changed — the controller says "ready" once, and not through the health endpoint

Readiness is one verdict folded from a declared table of preconditions, and three
surfaces read it rather than each deciding for itself: `flz boot status` prints the
verdict and every precondition behind it, `flz boot await` blocks on the verdict
instead of iterating hubs, and `GET /readyz` serves it. `flz boot await` no longer
reports success when no namespace hub exists yet, and a `--timeout 0` evaluates once.

The container layer follows. The image `HEALTHCHECK` runs `flz boot await --timeout 0`
instead of testing for the control socket, and the entrypoint waits for the socket as
the transport being bound before waiting on the readiness verdict. `/healthz` is
unchanged in every verdict, status code and body field.

The boot sequence no longer blocks on hub recovery, and readiness is no longer latched
at boot: a controller whose bounded replay wait elapsed becomes ready when its hubs
reach `:normal`, with no restart. `FLEZHA_BOOT_REPLAY_TIMEOUT_MS` now bounds only the
cluster-formation wait for a connecting runner.

`Flezha.Convergence.RecoveryGate` is the one module that decides whether a hub is still
recovering, as its specification already required.

### Changed — a counted fact is declared once, and every surface derives

Every data-plane liveness counter is now one row that says what it is, how it
reduces, which heartbeat key it mirrors and which surface reads it. The metrics
this exports, the controller's heartbeat mirror, the Elixir SDK's heartbeat and
the two documents describing the key set are all derived from that one list
instead of repeating it, so adding a liveness metric is one row rather than
seven edits. Five counter stores that numbered their own slots by hand now share
one table.

Provider telemetry gained one vocabulary: an event is the family root, the
provider's own id and a verb, with `shipped`, `retried`, `failed` and
`overflowed` meaning the same thing everywhere. The websocket provider's events
move from `[:flezha, :ws, …]` under the plugin family with the rest, and the
kafka producer's events reach a sink for the first time. Exported metric names
and sources are unchanged.

### Changed — one layout, one store, one identifier rule

Where this product puts its files is now one declaration. Ten subtrees are
listed once, each with the setting that moves it and what `flz reset` does with
it, and both the controller and the `flz` command read that list — so the
directory the controller writes to and the one a reset reports are the same
path, and `flz controller mem-top` reads the configured ProcessHub root instead
of always looking in `./app_data`.

The seven durable key-value stores now share one contract. Opening, rotating a
corrupt file, syncing every mutation and closing on shutdown are done in one
place instead of once per owner, and the plugin catalog — which had its own copy
of all of that — joins them. Each store's recovery instructions are unchanged.

Identifiers are one table too, a row per kind with its length limits and its
pattern. That closes a real defect: the controller could mint a runner id longer
than a socket name can hold, and the transport would quietly rename it, so the
socket on disk carried an id no operations surface ever reported. A minted id now
fits, and the control socket's filename is composed by the same code every SDK
already reaches for the data sockets.

### Changed — one durable record frame, one crc, one way a durable file lands

Four pieces of code decided what a durable record was, and they gave three
different answers to a damaged one. There is one now: every read and write of
the on-disk record framing, its 16-byte header and its checksum goes through a
single module, on the controller side and in the Elixir SDK alike, and the
kernel's own reader is held to the same bytes by a fixture both runtimes' tests
read. A change to the framing on either side now fails the other side's tests.

Whole durable files also land one way. Each one declares how it is written, what
makes its bytes durable and the name it wears while it is being written, and one
install performs that ceremony; the scanners that must ignore a file being
written derive what to skip from those declarations instead of listing suffixes,
so a new durable file cannot show up as a phantom Cell because a scanner was
not edited.

An Elixir-hosted runner now reports how much queued input a restore threw away,
under the same heartbeat key every other runtime uses. It was the only runtime
that reported neither the count nor the key, which left that operations surface
correct for six SDKs and blank for the seventh.

### Fixed — a QUIC peer ending a stream is now reported, and reported the same way

The controller's transport server ran two separate drivers, one for Unix
sockets and one for QUIC, and they disagreed about what had happened to a
stream. Over Unix sockets a runner that ended a stream cleanly was reported as
`connection_closed`; over QUIC it was reported as nothing at all, because the
driver could not tell "the peer finished" from "no more data right now". A
controller's log therefore said different things about the same event depending
on which transport it was configured with, and one of the two said nothing.

The two drivers are now one loop with one implementation per transport, so a
close reason is one word for both and a capability added for one reaches the
other by construction. Nothing a user configures changes.

The Java SDK's native surface is also declared and asserted now, the last of
the five language bindings to have that: an entry point added on one side and
not the other fails the build instead of surfacing as a missing method at the
first call.

### Changed — every frame declares what the wire owes it

An SDK that cannot write a frame no longer decides what to do about it at the
write site. Every frame now carries a guarantee class, and the class answers
once for every writer: a control frame is refused to its caller and never held,
a durable record or a plugin output is held in the bounded ring and replayed on
the next bound stream, and the data heartbeat is held by nothing at all because
it reports a moment that has passed. A payload id no class table names is
refused rather than given a default.

What changes for a user:

- **A shutdown notification the wire refuses is held and counted, not printed
  and lost.** The relay's final state is what makes reconnect-resume
  loss-free, so it is now treated as the durable frame it is.
- **A runner ends when its wire goes, whatever its host language.** Standard
  input closing and SIGINT or SIGTERM are detected once, in the shared core, on
  a thread no host loop can hold up — so a runner asleep inside a long
  activation still exits. Python, Ruby, PHP and Node stop installing detectors
  of their own; the Java SDK keeps its JVM shutdown hook.
- **An undelivered heartbeat asks for a reconnect on every runtime.** The Elixir
  SDK now runs the same flood ladder the shared core does, and a runner with one
  live and one dead secondary does not reconnect for ever.

A latent defect this closed: on the shared exit path a line to standard error
could take the runner down with it once the harness had closed its pipes,
leaving the process alive instead of ending it.

### Changed — one bounded call, one lookup, one timer, one child spec

Five jobs every process in the controller does — call another process, find the
one that serves a relay, learn that one died, arm a timer, declare a
supervised child — were done at four hundred-odd sites in as many spellings as
there were authors. Each is now a seam a site declares into.

What changes for a user:

- **A wedged process no longer stalls the caller for five seconds.** Every call
  into a process reached by pid carries an explicit bound and answers a value
  its caller declared, rather than waiting out a default and taking the caller
  down with it. A call into a registered node-wide service is deliberately left
  as it was: a store that is gone is a fault worth crashing on.
- **Tearing down many relays at once costs one timeout, not one each.**
  Where the wait happens inside a single manager, that manager now takes the
  whole list in one request and overlaps its own calls. Six unreachable
  relays took twelve seconds and now take two, and every one that could not
  be reached is still named in the result.
- **One vocabulary for "that relay is not there".** The three different
  error strings for the same fact are down to two, written at one place, and
  the internal answer distinguishes not-found, dead and hub-unavailable.

Nothing a user configures changes, and no SDK is affected.

### Changed — a `flz` verb declares what it takes, and one reader reads it

Every `flz` verb now declares its positional arguments and its options — the
long spelling, the short alias, the kind of value, whether it is required, the
knob that answers it when no flag is written — in the row it already lived in,
and one walk reads them. The reader, the refusal for a value it cannot carry,
the arity check and the usage line are all derived from that row, so adding a
verb no longer means editing an argument walk, a switch scan, an arity refusal
and a usage string in four places.

Three things change for a user:

- **An option is read wherever it is written.** `flz relay stop --token t
  op1` now stops `op1` and carries the credential. It used to send `--token` as
  the relay id and drop the token entirely, because eight verbs decided
  which words were options by their position.
- **Every failure is written to stderr.** The `relay` and `plugin` families
  printed theirs to stdout, so a run piped into a file or `jq` collected the
  error as if it were the answer. Exit codes are unchanged. Every failure now
  also carries one prefix (`error: `) instead of the three the surface had, and
  a reason that is already a sentence is no longer quoted.
- **Usage lines are rendered from the row.** The twenty-nine hand-written ones
  are gone; a verb invoked with too few arguments prints exactly the positionals
  and options its row declares. `flz audit read`, `flz observe enable` and
  `flz observe tail` with no arguments now print that line and exit non-zero,
  where they used to print the whole command screen and exit 0. A verb with no
  screen of its own shows its usage line above its command's screen, so every
  option it takes is discoverable from `--help`.

`flz controller status`, `flz observe sources` and `flz observe sinks` now name
`--output text|json` in their help screens; they always accepted it.

### Changed — every configuration knob is one row of one table

Every environment variable and application-env key the controller, its SDKs, its
crates and its shell wrappers read is now declared once in
`Flezha.Config.Keys` — its name, key, variable, type, default, the runtimes that
read it and the operations surface that shows it — and read through
`Flezha.Config.get/1`. `config/runtime.exs` derives the whole `:flezha`
configuration from that table in one call instead of translating each variable by
hand, and a per-environment default is a field on the row rather than a line in a
per-environment config file. Adding a knob is writing a row.

Several documented variables now do what they say. `FLEZHA_SOCKET_FILES_PATH` is
honoured under `mix test`, where a boot hook used to write the key before the
ladder could read it; fourteen more, among them `FLEZHA_HEALTHZ_BIND` and the
Prometheus and OTLP settings, were skipped entirely in the test environment. A
value that is not a number, an address or a member of an enumeration now refuses
the boot naming the variable, the value and the type — a pool size or a flag
still falls back to its default, because `"0"` already means "unset" in every
runtime. `/healthz` binds `0.0.0.0` everywhere, the address the documentation has
always given.

Two variables that were set by configuration and read by nothing —
`FLEZHA_CONVERGENCE_PLUGIN_STRIKE_THRESHOLD` and
`FLEZHA_CONVERGENCE_TICK_LATENCY_THRESHOLD_MS` — are removed with their
documentation rows.

### Changed — a plugin provider is one declaration

A provider now declares its whole lifecycle: every option it takes as a row with
a type, a default and its documentation, and the worker it runs. The extension
table drives every phase from that declaration rather than probing which
callbacks a provider exports, starts the declared worker when the plugin is
activated rather than when it is prepared, and stops it the same way on disable
and on teardown. A prepared plugin now holds no process at all, so a plugin on a
relay whose SDK side never binds costs nothing.

Adding a provider is one module and one row of `Flezha.Plugin.Providers`; adding
an option is one row of the provider. The `enabled_plugins` map in
configuration, the three per-provider supervisor modules, and every hand-written
option allow-list are gone. An option's default now has one home, and each
provider's documented option table is rendered from its declaration and gated
against it, so a documented key that is not a row and a default that differs both
fail the suite.

Every option refusal reads the same way on the CLI path and on the `--plugins`
spec path, which also means a value that reaches `--plugins` is now checked where
it used to pass through unconverted. Kafka's one tuple refusal became a sentence.
Every phase change emits `[:flezha, :plugin, :phase]`.

### Changed — one topology check, in every SDK

A builder no longer decides what a topology may be. Every SDK records the links
its author declared — a transform, a key, a name, a sink, a consumer, a merge, a
branch — and submits that record to one check in the core, which answers with the
derived declaration or with named refusals. The refusal vocabulary is
`flz_core::host::issue`; the C ABI gained the read-only `flz_core_check_class`
beside `flz_core_declare_class`. The Elixir SDK is its own core and re-expresses
the same check against the same fixture.

`test/fixtures/reference_topologies.txt` now carries one row per refusal, and it
is what every SDK's conformance run is measured against, so a new refusal is one
variant and one row rather than an edit to seven builders.

The record changed shape: `channel.consumers` became `channel.chains`,
`class.ack_granularity` became the raw `class.checkpoint_ack`, and tasklets
carry `change_is_nochange`. Any checkpoint knob now implies the checkpoint
granularity, stated once in the core instead of once per SDK.

Erased hosts are refused three declarations they used to be given: a
`mailbox_capacity` of 0 or above 1 048 576, a correction window with no
effectively-once tier, and a logical clock over a class that journals nothing.
Each is a shape the reference SDK has always refused.

- Every wait before a retry in the controller and the Elixir SDK is one declared
  row of `Flezha.Config.Backoff`, and every timeout, interval, grace and budget
  one row of `Flezha.Config.Durations`. Waits and resets are emitted on
  `[:flezha, :backoff, :wait]` and `[:flezha, :backoff, :reset]`. Values are
  unchanged except for three named jitter differences (the cluster reconnect and
  the Kafka produce retry now use the one jitter rule, and the SDK reconnect's
  jittered wait no longer exceeds its 30 s cap). `FLEZHA_HEALTHZ_BIND` has one
  default again, the documented `0.0.0.0`.

### Added — run a declared relay graph in your own process, with no controller

`LocalRelay` (Rust, Java) and `Flezha.SDK.Local` (Elixir) take the relay
class you would deploy, run the `channels()` graph it already declares in the
host process, and hand each value released on a sink to a callback. There is no
local-mode member on the class and no second declaration: the same type
registers against a controller unchanged. Input is fed by the caller on a
declared entry channel and enters where a frame off the wire enters, so the
channel's decode, the parse ladder and admission all run; feeding blocks while
the consumer's mailbox is full, exactly as the deployed entry path does.

What runs is the deployed machinery: edge transforms and keying, one Cell per
resolved key with its own mailbox and overflow policy, the retry ladder, and
inter-tasklet delivery performed by the runtime through the same routing seam
live delivery and replay travel. Local mode installs a different outbound sink
rather than branching anything: a new `FlezhaStream::Local` variant takes what
production writes and delivers it in process, and a null durable sink discards
every record at the outbound edge, so the change log, the fold and the effect
path still run.

**Local mode holds state in memory and nothing is durable.** Every run says so
on stderr before it does any work, from the runtime core rather than from each
host SDK, and it is not suppressible by ordinary configuration. Durability,
effectively-once delivery, plugins, placement and migration, the dead-letter
queue, observability export, authz and multi-runner scale need a controller; a
declaration asking for one of those is refused at startup, naming the
capability. A value released on a sink with no registered callback is reported
by name rather than discarded, and an idle wait that reaches its budget is an
error rather than a silent success.

New: `docs/quickstart.md`, `sdk/rust/examples/local_word_count.rs`, Java example
`E13_LocalMode`, and `scripts/check-local-mode-naming.sh`, which fails a
local-mode symbol or documentation page named as testing material. Node,
Python, Ruby and PHP do not carry local mode yet.

### Changed — one health verdict per subject, read by every surface

The controller now judges "is this healthy" once per tick and publishes one
record per subject — a runner, a relay, a plugin, a channel or a driver —
in one closed vocabulary (`healthy`, `degraded`, `failed`, `unbound`, `parked`,
`suspended`, `unknown`). `/healthz`, `flz relay info`, `flz relay list`,
`flz cluster status` and the published `Flezha.Service.Relay.relay_rows/2`
all read that record, so they can no longer describe one relay in
contradictory words. A relay that is parked, bound to a runner the roster no
longer holds, or missing a declared plugin reads `degraded` on `/healthz` — with
`parked`, `dead_runner` or `plugin_unhealthy` among the joined reasons — where it
could previously read `healthy`; a dead binding is no longer counted in
`bound_relays`, and the body gains `unknown_relays`. `flz relay info`
reports `parked` as a relay's health where it reported `degraded`.

Every detector's `[:flezha, …]` event now reaches the observability bus by the
detector table's own declaration of it, including the control-plane watchdog's
liveness timeout, the plugin fail-open, the catastrophic-dataflow stop and the
four runner heal events; `[:flezha, :health, :verdict]` is emitted on every
verdict-state transition.

### Fixed — three frames the Elixir SDK wrote to nothing are now held and replayed

A Cell evicted while no secondary stream was bound lost its
`delete_cell_state`, so the controller kept state for a Cell the runner had
already destroyed. The same eviction lost every change record that Cell was
still holding, while the output of the very dispatch that produced them was
replayed by the Relay — the controller saw a result whose cause it never
heard about. And the steps driver's two writes forwarded through the runner, a
staged output and a saga marker, were dropped whenever no stream was bound or
the write was refused: one behind a log line, one behind a discarded verdict.

All three are now held and replayed on the next bound stream, on the terms every
hold on this wire already used — bounded by `FLEZHA_RECONNECT_RING_CAPACITY`,
oldest dropped past that and counted as a drop-oldest shed. Because a hold that
dies with the process that owned it is not a hold, a Cell that stops in an
orderly way hands what it could not ship to its Relay, which outlives it and
replays those frames first: ahead of the outputs of the same dispatch, and ahead
of anything a replacement Cell for the same key writes.

This is the Elixir SDK only. It is its own core and inherits nothing from the
shared `flz_core`, where the steps relay writes were already held; the one matching
hole there, an evicted Cell's delete, is recorded for a follow-up.

### Fixed — the Kubernetes readiness probe stops reporting a booting controller ready

The `readinessProbe` in `kubernetes/flezha-daemonset.yaml` did `GET /healthz`,
and the health endpoint returns `503` for one verdict only. In a controller's
first tick every count it reads is zero and no hub is recovering, so the ladder
falls through and the probe read `200 {"status":"healthy"}` — before any
namespace hub existed and before anything had been restored. Kubernetes marked
the pod Ready and routed traffic to it. The window is the whole boot, not a
race: the boot sequence runs in a continuation while the supervisor keeps
starting children, and the health listener is up throughout.

One endpoint was being asked two questions with opposite correct answers.
`/healthz` answers whether the kubelet should restart the node and must stay
`200` through a planned recovery, so it is unchanged — every verdict, status
code and body field it had, it still has. The controller now also serves
`GET /readyz`, which answers whether traffic should be routed to it: `200` only
once the boot sequence has completed, every namespace hub has left the recovery
state machine and the durable registry has been reloaded, and otherwise `503`
with a body naming the preconditions still unmet. It shares the health
endpoint's listener, port and `FLEZHA_HEALTHZ_ENABLED` switch and adds no
configuration key. The DaemonSet's readiness probe now reads it; the startup and
liveness probes still read `/healthz`.

Every precondition is decided on each read rather than latched at boot, so a
controller whose bounded boot replay wait elapsed becomes ready when the hub
finally reaches `:normal`, with no restart.

### Fixed — a corrupt durable record is refused instead of silently deleted

A record whose crc32 failed anywhere in a WAL or an input log was treated as a
torn tail: the reader truncated the file back to the last record that verified,
deleting every acknowledged record after it, at every boot, with nothing logged
and no telemetry. A retention or realign cut took the same wrong boundary, and a
restore payload assembled from such a log shipped the records before the failure
as though they were the whole history.

A record log is now truncated only where the reader can prove no complete record
can begin after the failure point. That proof holds for the torn tail a crash
mid-append leaves — still repaired, and now reported with the byte count it
discarded. It does not hold for a record whose crc fails with complete records
behind it: the bytes are left untouched, the process that owns the file stays
alive and refuses to serve rather than being restarted into the same file, and
one error line plus one `[:flezha, :state, :record_frame, :refused]` telemetry
event name the file, the last seq that verified and the byte offset. The
Cell's restore is refused the same way, so the relay is left unbound
rather than binding on a partial history — the behaviour a refused restore
already had. Recovery stays a user decision with the bytes still in hand.

### Fixed — `flz relay rescale` prints its summary again

The rescale summary line still asked the result for the count of forwarding
plugins it had re-pointed, a field the command stopped returning when forwards
started following a rescale through their own health verdict instead of a nudge.
Every successful rescale therefore ended in a `KeyError` instead of the summary,
after the work had already been done. The line now reports what the result
carries, and a CLI test in the default suite pins it — the drill that caught this
runs only in the nightly tier.

### Changed — every module lives in the directory that owns its feature (`one-directory-per-feature`)

`lib/flezha/` now says what the system is made of. The CLI — sixteen
modules, the whole `flz` surface — has left `service/`, which was both the
control-plane RPC layer and the entire command surface, and become `cli/`. Three
shared process primitives that sat loose at the top level became `process/`, the
compile-time constants became `config/`, and the two directories that held one
file each (`socket/`, `diagnostics/`) folded into `transport/` and
`observability/`. Seven more loose files moved to the directory that owns them.
Every file that remains at the top of `lib/flezha/` is either the OTP callback
or the public facade beside a directory of its own name.

Thirty-four files changed path and not one line of code changed with them: no
module was renamed, because module names here are durable state on disk. The
layer gate reads where a file sits rather than what its module is called, so its
recorded debt fell from 135 entries to 126 — seven loose modules that now have a
home, one upward call that stopped being upward, and two calls to the outbound
HTTP client, which the gate now ranks as the foundation it is rather than as the
top layer its neighbour occupies.

### Added — a gate that a lower directory never names a higher one (`settle-the-structure-ratchet`)

The repository has had a written dependency order for its Elixir controller and
nothing that checked it: the structure instrument counts dependency cycles,
which say a cycle exists and never say which way an edge points.
`scripts/check-layer-direction.sh` now fails when a file in one directory names
a module whose own file lives in a higher one, and when a module still lives
loose at the top of `lib/flezha/`. It reads where a file sits rather than what
its module is called, because module names here are durable and a restructuring
moves files without renaming anything — so the count falls when a file moves
into the directory that owns its feature. What the tree holds today is recorded
in `scripts/layer-direction-baseline.txt`; the gate blocks anything new and
that list may only shrink.

### Fixed — a dying hub can no longer exit the fleet manager from the post-start path (`settle-the-structure-ratchet`)

Seeding a new hub's recovery snapshot reached into the hub outside every
liveness guard. A hub torn down in that window exited `FleetManager`, which
serves every runner on the node, taking the runner roster and the acceptor with
it. The whole post-start body is now inside the one guarded seam.

### Fixed — the test harness resets the state root the writers actually use (`settle-the-handover-race`)

The full test suite was red in roughly one run in three, always somewhere in
`FlezhaTest.Handover` and on a different test each time, while the module passed
alone at every seed. The reset every one of its tests starts from resolved the
state root a second time, on its own, instead of asking
`Flezha.State.Paths.state_root/0` like the writers, the bind-time restore and
the relay teardown do. Once any teardown had deleted the `:state_root`
application key — the run configures it and twenty-six teardowns delete rather
than restore it — the reset emptied a directory in the checkout while the live
state tree sat elsewhere, so it did nothing and the module ran with no isolation
between its tests. Whether it landed after the first such teardown was decided
by the seed, never by load. The reset now uses the one resolver, which also
covers the durable input log, since that lives under the same root.

### Changed — every remedy is a planner op, and every teardown one ordered walk (`route-every-remedy-through-the-planner`)

Choosing what to do about a failure used to happen in nine places besides the
convergence loop, each with its own retry ladder, counter or clock, and the loop
never saw any of them. Now every site that notices something — a runner
disconnected, a bind parked, a compaction given up, a seq gap opened, a
subscriber crashed, a registry row rewritten, the node draining — simply says so
through one intake and decides nothing. The loop reads those facts beside what
it probed, picks the remedy, and applies it as one of ten primitives under one
cooldown table. Two primitives are new: clearing a runner's stale control
channel so its next data stream can claim it, and re-writing a durable row that
a late registry write left naming the wrong runner.

Users see a few differences. A failed bind is now one attempt: the relay
parks with its reason and the loop retries it on the standard 5 s → 30 s → 60 s
→ 300 s schedule instead of six jittered attempts inside the relay, and a
runner arriving in the namespace releases every parked relay at once. A
controller losing its last runner for a namespace moves its relays one by
one through the consent gate instead of draining the hub under a five-second
budget, and once it holds none of them it leaves that namespace's cluster, so a
node with no fleet is not handed work it cannot run.
A crashed plugin subscriber is restarted by the loop, per plugin, so one
crashing plugin no longer delays its siblings. Two knobs are gone,
`:overwatch_retry_backoff_ms` and `:wal_resync_retry_interval_ms`, because
nothing has a second schedule to configure any more.

Stopping things is one mechanism too. A user stopping a relay, an
Overwatch terminating, a namespace being stopped and a node draining all fold
over the same declared, ordered list of teardown steps under one budget, so a
plugin table is torn down once instead of twice, on-disk state is deleted
through one guard instead of four places with three different ones, and adding a
resource with a teardown means adding one step rather than editing four paths.

### Changed — every administration verb is declared once, and one seam applies it

Authorizing an administration verb used to be decided at every site, in six
spellings, and thirty-two verbs never got a decision — five of them mutations,
`flz relay start` among them. Every verb is now one row of a declaration
table beside the policy (its action, where it is judged, how it is audited),
and one function applies the row on the `flz` wire, the published service API
and the runner session alike. A wire name without a row does not compile, a
published function without one fails the suite, and an action no row names
fails it too. Every `flz` request ends with one options list that carries the
credential, so the thirteen verbs that could not receive a token now can, and
`relay start` takes `--plugins` and `--kind` in that list. A verb scoped to
a stored row refuses an unbound caller before reading it, so `schedule run` no
longer says whether a name exists. Every declared audit source is durable by
construction, and one record shape names the actor, the action and the
outcome from either surface. Three actions were added (`relay.send`,
`relay.replay`, `state.records`) and six dead ones deleted; the runner role
holds `runner.connect` alone. Users of the console: four controller-wide
reads are now judged in the caller's own namespace, and the journal, inbox,
completion and time-travel reads carry actions.

### Fixed — five development instruments that misreported or collided (`close-five-small-tooling-defects`)

Five small defects in the tooling, each reproduced before it was touched. The
Python coverage report had been measuring almost nothing of the package: the
`filterwarnings` entry in `pyproject.toml` imported `flezha` before coverage
started tracing, and moving the filter into `tests/conftest.py` takes the
package total from 61 % to 93 % — the filing had counted two modules, the
measurement says the whole package. Node's coverage table lists only `dist/`
now instead of the fixtures the suite writes under `/tmp`. One reader,
`scripts/manifest_pin.sh`, replaces the five inline patterns CI used to pull a
pinned version out of a manifest, and refuses a range. The test node is named
the way the release names its node — `FLEZHA_NODE_NAME@FLEZHA_NODE_HOST` —
with a base derived per run, so two suites on one box are both distributed.
The Python state guard's delegation inside the runtime window has a test.

### Fixed — `flz schedule run` is an authorized relay start, and the schedule flags travel as one options document (`gate-schedule-run-and-fold-its-flags`)

`flz schedule run <name>` fired an instance with no authorization under any
posture, and its request had nowhere to carry a token. It now takes `--token`
(or `FLEZHA_TOKEN`) and is judged as `relay.start` in the schedule's own
namespace before anything spawns. That makes it the one schedule verb not
judged in `_system`: an `ops@_system` token can add, list and remove a
schedule but cannot run one; a token bound to the schedule's namespace with
`relay.start`, or `admin@_system`, can.

- **One options document.** `flz schedule add`'s optional flags — `--namespace`,
  `--init_values`, `--kind`, `--plugins` — and the credential reach the
  controller as one keyword list instead of positional arguments with defaults,
  and an absent flag is absent from the request. The user-facing flags do
  not change; `--plugins` arrives as the text the user wrote and is stored
  verbatim.

### Fixed — a runner's self-report is counted once per runner (`count-a-runner-self-report-once`)

**The value of eight `:flezha_transport` metrics changes on any runner with more
than one secondary stream**: `credit_parked`, `mailbox_blocked`,
`mailbox_drops_newest`, `mailbox_drops_oldest`, `mailbox_pending_pushes`,
`restore_discarded_inputs`, `timers_armed` and `timers_fired`. A runner sends the
same heartbeat on every secondary stream it holds and the controller mirrors it
into every channel row, so each of these read the runner's value multiplied by
its stream-pool size and moved when the pool was resized. They now read the
runner's value once per runner, summed across runners; a per-channel counter
(`credit_active_sessions`, `credit_overrun_total`, `send_gate_held`, the
`channel_*` io counters) is unchanged.

- **One collapse, owned by the slot layout.** `Flezha.Transport.LivenessCounters.by_runner/1`
  collapses a runner's channel rows to one; the transport source and the
  convergence stall classifier both read it, and which slots are runner-global
  is derived from the layout rather than listed.

### Fixed — an Elixir Cell holds a change record the wire refused (`hold-a-failed-record-in-the-elixir-sdk`)

The Elixir SDK already held a plugin-bound output no stream would take and
replayed it on the next bound one. The Cell's change records were still lost
outright, on both arms — the write refused, and no stream bound at all — so
after a reconnect the controller could receive an output with no record of the
change that produced it, and nothing counted the loss.

- **A refused record is held and replayed** on the stream the rebind wires, in
  the order the records were written, so a `change_seq` is never inverted.
- **The hold reads `FLEZHA_RECONNECT_RING_CAPACITY`** (default 1024), drops the
  oldest past it, and counts the shed as `drops_oldest` — the same terms and the
  same knob the output hold uses. That counter now moves for records too.
- **A dispatch's records still reach the wire before its output frame**, on a
  replay as on a live stream: the Relay rebinds every Cell and waits before
  replaying its own outputs.
- **The credit window is unchanged**: a frame is debited when it is written, not
  when it is held, so a held record does not consume the window and a replay
  debits the session the reconnect armed rather than the one that failed.

### Changed — the Kafka producer's queue-overflow option joins the policy vocabulary (`rename-kafka-queue-overflow`)

**BREAKING for a producer plugin that configured `:queue_overflow` explicitly.**
`:block` is now `:drop_nothing`; `:drop_oldest` and `:drop_newest` are unchanged.

- **Rename, no behaviour change.** The option's three values answer the same
  question as `OverflowPolicy` — what does a full queue discard? — and now spell
  it the same way. `:drop_nothing` is already true of the mechanism: a full
  queue holds the record and makes room only from a drained batch.
- **No compatibility alias.** `--Xqueue_overflow=block` is refused with
  `Invalid value for queue_overflow: block`. A stored plugin row carrying the
  retired atom must be re-enabled with the new spelling; because `:block` was
  the default, a row carries it only if a user set the value already in
  force.
- **The parking mechanism keeps its own names.** `:queue_block_timeout_ms`
  still bounds a synchronous `enqueue/4` caller's wait, and the worker's
  `blocked` queue and `:block_timeout` message are unchanged — they name the
  park, not the policy.
- **`scripts/check-overflow-naming.sh`** now catches this surface: it matched
  the policy list only when the retired name sat next to a sibling, which is
<!-- overflow-naming-record:begin -->
  why it passed over `queue_overflow: :block` for the whole of its life.
<!-- overflow-naming-record:end -->

### Added — Python SDK (`python-sdk`)

A sixth host for `flz_core`, over PyO3: a `python` feature on `flz_core_ffi`
beside `jni`, `napi` and `ruby`, not a crate of its own. `pip install flezha`
needs no Rust toolchain — an `abi3` wheel per platform targeting the 3.11 floor,
and per-version free-threaded wheels from `cp314t` (PyO3 refuses the
free-threaded build below 3.14). Both tiers build from one source with one API,
and the runner names the tier it is running in its startup line.

The authoring surface is decorators — `@flezha.tasklet`, `@flezha.state`,
`@flezha.relay`, `@flezha.relay` — over a handler that
is a plain function when it is a transform and a class when it has hooks. **Type
hints are the declaration**: the input, output, state and change types all come
from the annotations, and the package ships `py.typed` with generic `Edge[T]` /
`Tasklet[I, O]`, so a wrong edge is a pyright error before it is a
registration refusal. That pairing is what Python has in place of Rust's
`tests/ui` compile-fail pins, and both halves are pinned — by a fixture that
must fail type-checking and by a refusal that names the handler and the defect.

Durable `State` and `Change` serialize as MessagePack from author-declared
dataclasses, Pydantic models or attrs classes — **never `pickle`**, whose
loading executes code and whose payloads embed class paths; its absence is
asserted over the loaded source. A class declared `@flezha.state` rejects direct
attribute assignment at runtime, naming the field and `ctx.change`, which is the
enforcement level the authoring contract now records for Python.

Concurrency is the Rust host's model, because CPython's situation is the Rust
host's: a base pool of threads each looping `poll → dispatch → complete` on
itself, plus a supervisor parked in the starvation wait, reusing
`FLEZHA_WORKER_POOL_SIZE` / `FLEZHA_WORKER_POOL_MAX`. **Every blocking core call
releases the GIL**, so the lock bounds only handler bodies — and the
free-threaded tier lifts even that. An opt-in asyncio executor
(`FLEZHA_PYTHON_EXECUTOR=asyncio`) hands each activation to one shared loop,
fetching on real threads and completing on the loop thread; the suite runs under
both and the ordering scenarios are identical. Under either executor the
supervisor's answer to starvation is one more fetch-and-run **thread**, because
a stalled loop cannot rescue itself. The SDK spawns no processes and refuses any
core call after a fork, naming the rule.

`test/apps/python_runner/` claims seven of the eight capability families every
other veneer SDK defers — the persistence family, the effectively-once tier,
timers, correction windows, keyed channels, dynamic-cell eviction and schema
evolution — each proved by the live suite over UDS and QUIC.
Claiming `keyed_channels` required minting the `flz.cell` / `flz.seq` /
`flz.key` identity stamps in the SDK, which Java does and Node, Ruby and PHP do
not.
`mix bench.roundtrip --runtime python` lands with the change.

`examples/python/durable_agent/` is the worked example: a durable agent loop
with a model call, a retry ladder, a compensation and a suspending steps relay
session.

### Added — PHP SDK (`add-php-sdk`)

A fifth host for `flz_core`, and the first to consume the C ABI **as a C ABI**:
`ext-ffi`, no compiled veneer. PHP has no stable ABI across minor versions, so a
native extension would be a PHP-version × thread-safety × platform matrix that
grows with every release; `ext-ffi` needs one library per platform — the five
that already ship for Java.

`composer require flezha/sdk` pulls nothing: `php >= 8.4` and `ext-ffi`, and
that is the whole requirement. The authoring surface is a `#[Task]` attribute
for the scalar declarations and a fluent `channels()` builder for the graph,
with the optional hooks read from the methods a tasklet **overrides** so a
declaration and its implementation cannot disagree. Durable `State` and
`Change` serialize as JSON through author-declared `toArray()` / `fromArray()`
— never PHP's `serialize()`, whose embedded class names and mangled property
names turn a class rename into a broken restore.

Two executors ship behind one seam that carries **loop multiplicity, not work
distribution** (a `Swoole\Thread` cannot receive a closure, so `submit(callable)`
has exactly one possible implementation). `SyncExecutor` is the default: one
loop on the calling thread, on the non-thread-safe build every distribution
ships. `SwooleExecutor` runs the same loop on a pool of threads over one shared
context, growing on the core's own `flz_core_wait_starvation` signal, so a
single relay can exceed one core. Which one is an axis of the *run*
(`FLEZHA_PHP_EXECUTOR`), never a property of a file: no test, fixture or
manifest entry exists in a per-executor variant.

`SDK_LANG_PHP = 6` and a JSON change-format id join the allocation table in
`flz_core/src/header.rs`; `FORMAT_ID_KRYO = 4` is recorded there too, so the
table of record is what makes a new id provably free. The one Rust behaviour
change is none — this is a constant, a test and a consumer.

Measured, not assumed: `ffi.enable=preload` (PHP's default) permits FFI from the
CLI SAPI, and a `Swoole\Thread` is a fresh interpreter that is not it, so the
threaded executor requires `ffi.enable=true` and says so before the runner
connects.

### Changed — `DropNothing` withholds work instead of holding a host thread (`nonblocking-drop-nothing-backpressure`)

Under `DropNothing`, a push into a full consumer mailbox parked the **calling
host thread** in a bounded retry loop. Room only appears when somebody runs the
consumer's round, so if the parked thread was the only one that could have run
it, the process stopped. The typed Rust host escaped through
`routing::install_inline_serve` — a Rust `fn` pointer, so it could not cross
the C ABI and no FFI host ever had it. Measured with the hook claimed by a
no-op, exactly the state a C-ABI host leaves it in: **0 of 20 payloads
delivered, wedged 15 s** on a one-worker host.

A push a producing Cell cannot deliver is now appended to that Cell's
**push holdback**, its handout gates stay held until the holdback drains, and
the producing thread returns immediately — the state
`PollCore::retry_after_failure` already put a cell in during a backoff
window, re-armed by the consumer's own wait list. Ordering is preserved by
construction: a gated Cell is handed no further activation, and a push made
while its holdback is non-empty appends rather than attempting. While a Cell
owes a push the flusher holds its durable records, so a consumed offset can
never become durable ahead of an edge output derived from it.

`INLINE_SERVE`, `install_inline_serve`, `serve_inline`, `ACTIVATION_DEPTH`,
`in_activation` and `poll_pool::serve_slot` are deleted, and with them three
latent defects the hook carried: an inline serve called `try_assemble` directly
and so bypassed `FLEZHA_MAX_INFLIGHT_ACTIVATIONS`; it re-entered an activation
on the producing thread with no depth bound, so a chain of full mailboxes
nested; and it ran a consumer's completion, durable record included, inside the
producer's still-outstanding activation.

Reaches Rust and Java below the seam (both complete through `flz_core`). The
Elixir SDK needs no mechanism change and that is a decision, not a gap — it
already defers the caller's reply into a `waiters` FIFO — but it takes the
reporting half, because heartbeat keys are a cross-SDK contract. The data
heartbeat gains `pending_pushes` and `pending_push_oldest_ms` on all three
SDKs; a gated producer holds no handout, so `inflight` is blind to it.

**Behind `FLEZHA_DROP_NOTHING_NONPARKING`, default off** — a rollout switch,
not a fourth policy: nothing an author declared changes either way. With the
flag off the producer parks as before, minus the inline serve, and the
starvation supervisor is the rescue. The entry-path transport reader keeps
parking in both: it has no producing Cell, and its stalling is what carries
ingress backpressure to the controller.

**In plain terms, for whoever meets this at 3am.** A `DropNothing` channel
discards nothing, so a push into a full consumer mailbox has to wait — this
change moves *what* waits. Flag off (the default): the pushing thread parks
until room appears, and if that thread was the only one that could have run the
consumer, nothing ever frees the slot. Flag on: the value goes onto a
per-producer holdback queue, the thread returns immediately, and the producing
Cell is handed no further work until the queue drains. Nothing is discarded
and order is preserved either way. **The default is off; nothing changes until
someone sets it.** Turn it on where a thread cannot be spared — a PHP
`SyncExecutor`, a Node host on the event loop, where the park is not a risk but
a guaranteed freeze and no supervisor can rescue a runtime with no spare thread
to buy. Do not turn it on for speed.

**The mechanism buys the freed thread and costs throughput under saturation.**
Measured (`hybrid_channel_throughput`, 5 000 messages per iteration): at the
default mailbox capacity the flag costs nothing (7.19 ms with it on against
7.36 ms off, p = 0.46), but at `FLEZHA_MAILBOX_CAPACITY=4` — a mailbox
saturated continuously — it is **30–36% slower than the park** (25.78 ms
against 18.92 ms for one consumer, 59.75 ms against 46.10 ms for three). The
proposal predicted the opposite; its figure was measured single-threaded and
excluded what dominates here, that the re-arm is a scheduling round-trip where
the park was a futex wake straight back into the emit. This is why the flag
ships **off** and why flipping its default is gated on three things, all named
recorded separately: a ≥ 2 h soak with
it on against a workload that actually saturates a consumer, the Java
round-trip bench run under both settings (**owed** — not run in this change, so
the numbers above prove one runtime only), and that 30–36% cost either closed
or accepted in writing.

Two windows this does **not** close, both named in
`docs/resilience/data_durability.md`: the consumer's volatile mailbox between a
landed push and the consumer's own durable record, and the controller's send
buffer, which still discards the newest frame when a runner stops reading.

### Fixed — a due wake no longer wedges a poll host (`close-overflow-silent-failures`)

`PollCore::promote_due` runs *inside* `poll` and pushed a due timer or an
interval close under `DropNothing`. With a full mailbox and no inline-serve
hook — the situation of **every** C-ABI host — the poller parked there. Because
`pollers_inside` is held across the whole call and `starving()` answers `false`
while it is non-zero, that poller read to the starvation wait as a thread about
to take work: the runner stopped and hid that it had. Measured at 1 of 3
firings delivered, wedged 15 s, with the pool ceiling free to grow.

A wake that cannot be admitted is now re-recorded at a later due time and the
pass continues — the wake list itself is the holdback — so the condition is
both impossible and *reportable*, since a due entry is starvation with no
dwell. A deferred firing keeps the timestamp it was armed with. No mechanism
was added and `routing::in_activation` leaves `poll_core.rs`.

### Added — the Java SDK brings a starvation supervisor (`close-overflow-silent-failures`)

`flz_core_wait_starvation` existed in the ABI and nothing in `sdk/java`
referenced it, so a Java runner whose fetchers were all held processed nothing,
recovered from nothing and reported nothing — measured at 0 of 20 payloads,
forever. `FlzCore.waitStarvation` is now bound and `PollHost` parks one
platform supervisor thread in it, starting one more fetcher per observation up
to `FLEZHA_WORKER_POOL_MAX` (default 10× `FLEZHA_WORKER_POOL_SIZE`, never below
it — the same shape and spelling the Rust pool uses). Every growth logs at
`INFO`; the first refusal at the ceiling logs at `WARN` and latches.

This does **not** cover the conformance `pool_wedge` condition: no ceiling
outruns user code that never returns.

### Fixed — an edge output's offset can no longer become durable ahead of the output (`close-overflow-silent-failures`)

**FFI hosts only** (Java and every proposed FFI SDK); the typed Rust path emits
during `run()` and was already correct. `resolve_item` folded the invocation's
changes, plugin outputs *and consumed offset* into the effect record and only
then routed the edge outputs. `OutputTarget::Edge` is excluded from the
transactional outbox, so a crash inside the flusher's 5 ms linger left the
offset durable, the input skipped on replay, and the consumer never fed — a
permanent loss while the tier reported effectively-once. The two blocks are now
in the other order: an item with an undelivered edge output is unresolved, and
an unresolved item contributes nothing durable. A fan-out holds its record
until every consumer's push has landed.

### Added — every overflow outcome is observable (`close-overflow-silent-failures`)

`drops_newest_total`, `drops_oldest_total` and `pushes_blocked_total` were
written by the kernel and read by nothing: `CellSlot::counters()` had zero
production callers, so no heartbeat, export or log ever carried a drop.

- **Three counters ride the data heartbeat.** Process-wide aggregates beside
  `dispatches` and `credit_parked`, mirrored by the controller into
  `hb_blocked_parks` / `hb_drops_newest` / `hb_drops_oldest`. The Elixir SDK
  reports the same keys from its own counters.
- **`blocked_parks` now counts wait iterations, not episodes.** A runner held
  in one long block episode showed a frozen dispatch count beside a frozen
  block count — byte for byte the picture a wedged runner shows.
  `runner_pool_wedged` reads the pair: dispatches frozen with blocks climbing
  is backpressure, dispatches frozen with blocks flat is a wedge.
- **`DropOldest` counts its admission.** A force-push that admits now bumps
  `pushes_total` (it is a successful enqueue by that counter's own definition;
  `processed` used to exceed `admitted` for this policy) and the evicted item's
  identity is named in a `flezha::mailbox` debug line instead of discarded.

### Changed — a dropping policy is refused where its loss is unaccountable (`close-overflow-silent-failures`)

**BREAKING for a class that declared both `effectively_once` and a dropping
overflow policy** — a combination that was silently lossy and is now a
declaration error naming the channel and both declarations. Refused in the Rust
and Java builders and again in `flz_core`'s declaration validation, so a
hand-written host declaration cannot slip past. A correction window requires
the tier, so it is covered by the same rule. Shedding stays fully available to
a class that promises neither.

Independently of the declared policy, a **replay-slice member always takes the
`DropNothing` path**: a dropped member left the slice open and short, the slice
still closed on the end-of-slice broadcast, and the correction folded whatever
arrived — a wrong answer with nothing reported.

### Changed — the overflow policy names its guarantee (`rename-block-to-drop-nothing`)

**BREAKING for every relay that declares an overflow policy.** The policy
formerly spelled `Block` is now `DropNothing` — searching for `Block`,
`BLOCK`, `"block"` or `:block` on the overflow surface lands here.

- **Rename, no behaviour change.** `OverflowPolicy` now answers one question
  three times — *what does a full mailbox discard?* — instead of answering it
  twice and describing the producer's thread once. `DropNothing` is already
  true of the shipped mechanism, which retries until the push lands and
  discards nothing.
- **Every surface moves together**: Rust `OverflowPolicy::DropNothing`, Java
  `OverflowPolicy.DROP_NOTHING`, Elixir `:drop_nothing`, the `HostClassDecl`
  wire literal `"drop_nothing"`, and `on_full = "drop_nothing"` /
  `onFull = "drop_nothing"` in the tasklet attribute. An unrecognised
  spelling is refused at declaration; there is no compatibility alias.
- **The parking mechanism keeps its own names.** `block_parker`,
  `BLOCK_PARK_TIMEOUT` and `pushes_blocked_total` name the entry-side park,
  which still exists and is honestly named. So do the Kafka producer's
  `:queue_overflow`, the observability bus's per-sink `overflow:` and the
  transport write buffer — separate mechanisms, unchanged.
- **`scripts/check-overflow-naming.sh`** fails a build that reintroduces the
  retired spelling on the policy surface, and runs in the Naming CI job.

### Changed — nothing drops by default (`close-silent-data-loss-defaults`)

**BREAKING for relays and producer chains that ran above capacity.** Two
defaults invert from drop to block, so a saturated pipeline now back-pressures
its source (visibly slower ingest) instead of silently discarding records with
every health surface green.

- **Entry channels default to `DropNothing`** in both SDKs (`OverflowPolicy`). A
  relay that wants to shed under saturation declares it —
  `.on_full(OverflowPolicy::DropNewest)` / `.onFull(OverflowPolicy.DROP_NEWEST)`
  — and the loss is a stated intent at the declaration site. The cost: a
  blocked entry parks the shared wire reader, so siblings on the same
  connection wait with it. Inter-tasklet channels already blocked.
- **The Kafka producer defaults to `:queue_overflow = :drop_nothing`** for every
  record, not only delivery-guaranteed ones. `:drop_oldest` / `:drop_newest`
  remain available and must be chosen. `Producer.stats/1` now reports
  `dropped_total`, so a chosen drop is readable synchronously, not only via
  telemetry.
- **A cold-takeover baseline pull retries** on the resync interval
  (`:wal_resync_retry_interval_ms`, default 1 s) instead of latching: a pull
  that failed to dispatch no longer strands the Cell in permanent shed.
- **Removed** the producer's inert `:partitioner` option and the documented
  but unimplemented `kafka_msg_partition` metadata override. One plugin
  instance serves one `(topic, partition)`.

### Changed — a production controller must choose a durability posture (`durability-tier-default-on`)

**BREAKING for users.** `FLEZHA_DURABILITY_BACKEND` no longer defaults to
`none` in production. A prod release booting with it unset refuses to start and
prints the two valid moves; dev and test are unchanged.

- **Set one line before rolling the new image.**
  `FLEZHA_DURABILITY_BACKEND=postgres` (or `s3`, or `local_path`) turns the
  remote durability tier on; `FLEZHA_DURABILITY_BACKEND=none` is the explicit
  local-only opt-out and reproduces the previous behaviour exactly — no tier
  processes, no backend connection, no change on any ack path. Rollback stays
  lossless: local state was authoritative throughout.
- **Local-only is visible.** `/healthz` carries `"durability":"local_only"` on
  every response and `flz controller status` prints
  `durability_posture: local_only`, so a fleet audit can find the controllers
  whose durable state has exactly one copy. Neither is an alarm.
- **The tier now ships the platform stores too.** A controller that dies with
  its disk used to lose its parked failures and its security state even with the
  tier on. Dead-letter parked records ship as one claim-fenced artifact per
  tasklet — on every park, drop, replay-mark and retention drop, so a resolved
  entry disappears from the backend rather than being resurrected — and hydrate
  with the relay that owns them. The control-plane stores (authz roles,
  tokens, namespaces, quota counters and the durable enabled flag; the schedule
  definitions) ship on the backstop sweep as per-writer replicas and hydrate
  **before** any hub starts or any connection is accepted, so a resurrected
  controller enforces from its first verb and the one-shot bootstrap claim reads
  already-claimed.
- **`bootstrap.token` never leaves the node.** The shipped file list is explicit
  rather than a directory scan: the one-shot plaintext secret beside the authz
  DETS is excluded by construction, along with the break-glass `REBOOTSTRAP`
  marker and the TLS material.
- **Every durable store is now accounted for.** `docs/operations/remote_durability_tier.md`
  carries the shipped set and the accepted-exclusions table (the audit log,
  delegated to external log shipping, plus the four non-stores above). A store
  on neither list is a defect.

### Changed — declared intent lives in the hub's declared set (`durable-relay-intent`)

- **Every relay start is declared, every stop is remembered for good.**
  Starts pass `durable: true` into ProcessHub's declared-children set; stops
  and steps completions ride the hub stop path, which removes the entry
  before the process terminates. A whole-cluster cold start restores the
  declared set (spawned steps instances included), and a deliberately
  stopped relay can no longer be resurrected by a node returning after an
  arbitrary absence.
- **Removed knob:** `FLEZHA_PROCESS_HUB_STOPPED_ROW_TTL_MS` (and
  `:process_hub_stopped_row_ttl_ms`). Stop knowledge is declared-set absence
  and never expires, so there is no 30-day tombstone window left to outwait.
  A deployment still setting the variable keeps booting; the value is
  simply no longer read.
- **Off-cluster intent copy.** With the durability tier enabled, the declared
  set ships through it (`remote_manifest` over `Flezha.State.DurabilityTier`),
  so intent survives the loss of every cluster disk. New tier backend
  `:local_path` (file storage, e.g. an NFS mount) joins `:postgres` and
  `:s3`; any `{module, opts}` implementing the backend behaviour remains the
  extension point.
- **Reroute is placement-consistent and atomic.** A disconnected runner's
  relays are rerouted per-relay through the factory-filtered HRW
  placement, and the registry binding switches only on a confirmed migrate —
  never to a random runner, never on a discarded result.
- **Cross-node re-placement.** A relay on a node with no runner
  advertising its factory is moved — state intact, consent-gated — onto a
  node whose fleet has one, instead of retrying a doomed local rebind forever.

### Changed — authentication and authorization finished end to end, enforced by default (`finish-platform-authn-authz`)

**BREAKING for users.** The trusting-LAN default is gone; every transport
surface authenticates its peer and the policy point enforces by default.

- **Transport authentication.** The QUIC server verifies a client certificate
  against a configured trust anchor (`FLEZHA_QUIC_CLIENT_CA`, default the
  generated `app_data/certs/ca.pem`; the cert generator now also mints a
  `client.pem`/`client.key` identity so single-host deployments work out of
  the box). The UDS control socket and its directory are owner-only. The
  release no longer ships a constant `RELEASE_COOKIE` — a node without a
  provisioned cookie refuses to start distribution — and TLS distribution is
  supported via `FLEZHA_DIST_TLS_OPTFILE`. `FLEZHA_TRANSPORT_AUTH=off` is the
  explicit wide-open opt-out, warned on every boot.
- **Enforcement by default.** `authorize/3` has three postures: `:open` (the
  explicit `FLEZHA_AUTHZ_ENABLED=false` opt-out, warned every boot),
  `:bootstrap_pending` (configured on, unclaimed — administration verbs refuse
  naming `flz auth bootstrap` while dataflow and runner reconnects continue),
  and `:enforced`. Destructive verbs (`state delete/rotate`, `dlq
  replay/drop`, `relay stop`, `controller stop`) and every plugin
  mutation are gated and audited.
- **Principals from the transport only.** CLI identity is a verified token
  (`flz --token` / `FLEZHA_TOKEN` → `caller_context.principal`); runner
  identity is `runner@<namespace>` minted from the connect handshake;
  caller-supplied principals are ignored.
- **Namespace isolation through the State subsystem.** Durable state lives
  under `app_data/state/<namespace>/…`, resolved via a class→namespace claim
  recorded at bind (conflicts refused; `_system`-migrated classes adopted).
  `Flezha.Boot.StatePathMigrator` lifts the flat layout under `_system/` once,
  stamped by a `.layout` marker. Listings and state verbs authorize against
  the resolved target namespace.
- **Remaining quota points.** `max_runners` enforced at runner connect
  (refusal observable), `max_state_bytes` (renamed from
  `max_disk_bytes_total`) accounted from the state tree, rebuilt on boot,
  enforced at relay allocation, with a latched Watchdog alarm — never a
  write-path kill — on breach of a running namespace; per-namespace
  `max_mailbox_bytes_total` caps the credit window.
- **CLI.** New `flz auth` (`bootstrap`, `whoami`, `rebootstrap`), `flz token`
  (`issue`, `revoke`, `list`), `flz role` (`bind`, `unbind`, `list`) families;
  `flz namespace` gains `create`, `delete`, `show`, `set-quota`. Break-glass:
  offline `flz auth rebootstrap` invalidates a lost `admin@_system` and
  re-enters the bootstrap flow on the next boot, audited.
- **Credential hygiene.** Kafka SASL credentials ride a closure into the
  client start (Postgres pattern): the returned/persisted opts are redacted,
  `sasl_password_env` resolves the secret from the environment at client
  start.

Runbook (cookie, certs, k8s manifests, break-glass):
`docs/operations/authn_authz.md`. Migration: provision the cookie + certs,
boot, claim the bootstrap, issue tokens; rollback is
`FLEZHA_AUTHZ_ENABLED=false` + the previous release.

### Added — tiered tasklet authoring: defaults you can always override but never must (`add-tiered-tasklet-authoring`)

Tasklet authoring had two tiers with a gap between them. The sugar
(`#[flezha::tasklet]` / `@Tasklet`) inferred everything from the `run`
signature but could not express multi-emit, so the most common real tasklets —
an OHLC aggregator closing one candle and opening the next, an SMA emitting per
window — fell off the sugar cliff straight into full ceremony. Three changes
close it, each tier a strict superset of the one above and explicit always
winning:

**Sugar covers multi-emit.** A sugared `run` may take a trailing tasklet
context and return unit; the body then performs any number of emits, and
`emits = Type` (Rust) / `emits = Type.class` (Java) declares the output type the
return value no longer can. The expansion stays exactly the code a hand-written
implementation would contain. A value return alongside the context parameter, a
`key = …` alongside it (the body keys its own emits), and an `emits` without it
are each refused by name rather than silently accepted.

**Java stops charging for erasure.** `inputType()`, `outputType()`,
`changeType()`, `clockType()` and `configType()` now default to the type
arguments read off the implementing class's generic supertype chain — a direct
`implements Tasklet<…>`, or a concrete subclass of a generic intermediate
class. `kryoTypes()` defaults to the resolved Change and State. That is ~24
lines of pure type-erasure tax removed from every hand-written Java tasklet.
An argument still generic at the concrete class is **never guessed**:
`defineRelay` fails naming the class and the parameter, and
nothing is registered — a startup error, not a dispatch error far from its cause.

**Rust gets a simplified tier.** `SimpleTasklet` declares only `Input`,
`Output`, `State` plus `name` / `init_state` / `run`; a blanket impl lifts it to
`Tasklet` with `Clock = WallClock`, `Config = ()` and the state's derived
change type. Implementing `Tasklet` directly stays fully supported and is the
way to declare anything non-default; a type cannot sit in both tiers, because
the impls overlap and the compiler refuses it.

Authoring surface only — no runtime behaviour, wire format or journaling change,
and every existing declaration keeps its exact meaning.

### Added — `route_latest`, the latest-document router building block (`add-route-latest-building-block`)

`c.route_latest(documents, traffic, key_fn)` (Rust) /
`c.routeLatest(Doc.class, documents, traffic, keyFn)` (Java) turns a
relay-wide document stream into per-key deliveries: every key seen on the
traffic stream receives each distinct document exactly once, on that key's own
next traffic item. Rust returns a stream of `(key, document)` pairs, Java a
stream of `RoutedDocument<D>`; the consumer keys by that key.

It exists because a keyed tasklet cannot receive a broadcast — every keying
declaration is total, and inside a partitioned member the computed key is
admitted against that member's key-group range — so this shape was being
re-derived by every application that hit the wall. The subtle part is the one
now encoded in the SDK: the router delivers on the key's **own traffic**, never
by fanning out over a remembered key set, because a remembered set outlives
key-group ownership (a rescale, or one out-of-range item seen during a reconcile
window, would make later deliveries park against the partition guard).
Deliver-on-own-traffic makes over-delivery impossible by construction.

The price, documented and deliberate: **one tick of latency per key** — a key
learns of a new document on its next traffic item, and a key whose traffic has
stopped is never delivered to. A full broadcast primitive (retained documents,
delivery to late-born Cells, rescale-time replay) was considered and
rejected: it would add machinery to exactly the recovery paths with the worst
incident history, and the deliver-on-own-traffic construction covers those cases
without it.

Sugar, not a runtime feature: the block expands to a merge into one packaged
single-cell tasklet whose `{document, version, delivered}` state rides the
standard change journal and passes the replay-determinism harness. No new
runtime semantics, no wire change, no new replay path. A document equal to the
retained one is inert (so a document source's re-poll after a reconnect is
silent downstream); a differing one bumps the version and every key is owed one
delivery again. The document is never parsed — parsing stays with the consumer.

Java-side notes: the document `Class` token is required because the router
retains the document in journaled state and only the wiring knows its type; the
Kryo state codec is now built from the tasklet *descriptor*'s type list (which
starts as a copy of `kryoTypes()`, so nothing else changes) rather than from a
freshly instantiated Tasklet, which is what lets the wiring register that
type.

### BREAKING — one plugin-sink method: `sink` + `sealed` (`unify-plugin-sink-api`)

`also_output(name)` / `also_output_with(name, f)` and the terminal
`output(name)` collapse into ONE method: `sink(name)` / `sink_with(name, f)`,
always returning the stream handle. A channel is the plugin-bound terminal by
having no consumers wired after it, not by which method declared it — the
runtime derives that from the wiring, which is what the two spellings were
encoding by hand. `.sealed()` is the optional terminator, returning the
non-chainable `Channel<T>` the old `output` returned; it is purely a
compile-time statement that nothing follows and changes no topology.

No shim, no coexistence. Migration is mechanical: `also_output_with` →
`sink_with`, `also_output` → `sink`, `output(name)` → `sink(name)` (append
`.sealed()` where the non-chainable return type was relied on). Wire frames,
descriptor field names, plugin bindings and error identifiers are unchanged; a
terminal declaration's frames are byte-identical to what `output` wrote, and a
unit fixture holds both shapes against those literals.

Consequences worth knowing: `ChannelDescriptor` lost `plugin_name` (dead) and
`plugin_serializer` — `plugin_sink` is now the one representation for both
shapes. A sink declared on a channel that a *later* statement also wires a
consumer into is now correctly hybrid; before, the terminal `output` had
already frozen the kind and those consumers silently received nothing.
`.on_full(...)` after `sink(...)` compiles now (it did not after `output`) and
is still never consulted on a channel with no mailbox — `.sealed()` refuses it
as before.

### Fixed

- **The remote durability tier is now correct with more than one controller**
  (`fix-durability-gc-multi-writer`). Two controllers sharing one backend is the
  configuration the tier exists to serve, and four defects made it unsafe there.
  All were latent — `FLEZHA_DURABILITY_BACKEND` defaults to `none` — but every
  one becomes a live data-loss path the moment the backend is enabled.

  - **Orphan collection deleted a peer's in-flight upload.** Every node ran its
    own hourly pass that listed the whole store, and the tier is manifest-last
    by design, so "uploaded but not yet referenced" — a state every ship passes
    through — was indistinguishable from an orphan. The victim then wrote a
    manifest naming a deleted blob, never re-uploaded it (`from =
    shipped_through + 1`), and at recovery the Cell came back **empty** while
    hydration returned `:ok`. Collection is now scoped to the references a node
    holds local state for AND refuses anything younger than `gc_grace_ms`
    (default 1 h); artifacts of a reference no node holds any more are still
    reclaimed past `gc_abandoned_after_ms` (default 7 d), so scoping cannot
    leak. Neither property alone is the safety argument.
  - **Blob names were position-addressed, so two writers overwrote each other's
    bytes.** An ordinary migration leaves the source node holding a full copy
    (`pull_relay/3` copies; only `move_cell/3` deletes), so two nodes
    legitimately held one reference at the same WAL epoch and wrote
    `r-<epoch>-<from>-<to>` with different records. Names now carry the content
    hash, as the registry cspec already did.
  - **The WAL epoch could never fence two writers.** It lives in the
    `.outbox.dedup` side-file, which `WalTransfer` copies and `Hydrator`
    restores verbatim, so both nodes held the same value and equal epochs
    overwrite. Manifest writes are now conditional on a **claim counter** that
    exists only in the remote store; bidding for a lineage requires the node to
    have actually adopted the relay, and losing one is reported as a routine
    handover rather than a permanent fence — the old latch would have left an
    RPO breach and a `degraded` `/healthz` after every migration.
  - **The registry cspec was a cluster-shared key at epoch 0.** `hub_id` is
    namespace-derived, so every controller shipped its own replica to one key
    with no fencing and last writer won — a node with a stale registry could
    erase the record of which relays exist. The key now carries the writer;
    hydration picks the newest replica.
  - **`commit_sync/1` conflated relay id and class**, so a class declaring
    RPO 0 could have its barrier answer `:ok` having shipped neither the Cell
    WAL nor the input log, and `sync_class?/1` never armed the gate at all.

  Also: a manifest naming an artifact the backend does not hold is now an
  alarm-grade `[:flezha, :state, :artifact_missing]` rather than a silent cold
  start, and the durability telemetry family reaches the observability bus for
  the first time — including `rpo_exceeded`, which the docs already called
  alarm-grade.

  **Breaking for an existing store**: blob key layout and the `put_if/4` backend
  contract both change. Free today because the tier is enabled nowhere; if you
  had enabled it, wipe the bucket or table — local state is authoritative, so
  nothing is lost. New knobs: `FLEZHA_DURABILITY_GC_GRACE_MS`,
  `FLEZHA_DURABILITY_GC_ABANDONED_AFTER_MS`, `FLEZHA_DURABILITY_WRITER_ID` (set
  this under a Deployment, where the pod name changes on every reschedule).

### Measured

- **How many steps instances a runner holds, answered with a number**
  (`steps-driver-as-state`). One OS driver thread per actively-running
  steps instance was the last thread-per-instance cost in the SDKs, and the
  proposal to delete it — turning the chain's position from a call stack into
  stored state — was gated on measuring it first. It was measured, and the
  verdict is **no-go**: the thread is ~1.4 KiB of a steps instance's ~73 KiB
  resident cost (the rest is heap), 10 000 blocked instances cost ~734–884 MiB
  and start in 0.65 s, and a driver blocked at a stage barrier — the agent-turn
  shape the proposal was named for — costs 0.5 % of one core at that
  population. Cutting the driver stack from 2 MiB to 256 KiB moves no resident
  byte, because a blocked driver's stack was never resident, so that stopgap is
  not adopted either. What the measurement *did* find is now documented in
  `docs/resilience/relay.md`: interference tracks total OS thread
  count rather than instance count (an agent turn holds two threads in Rust,
  one in Java, three if the class declares `max_duration`), and the CPU a
  blocked population burns comes entirely from three waits that re-check every
  200 ms — the inbox wait, the commit-ack wait and the duration watcher — not
  from the thread model. Kept: `sdk/rust/tests/steps_scaling.rs`, an
  `#[ignore]`d instance-count scaling probe, and
  `FLEZHA_STEPS_DRIVER_STACK_KIB`, unset by default and an address-space
  lever only. No behaviour change.

### Changed

- **A keyed channel's plain emit carries the Cell's own key**
  (`default-emit-to-cell-key`). In a keyed Cell — the OHLC cell for
  `BTC/USD`, the SMA cell for `BTC/USD` — the natural key of essentially
  every output *is* the key the input routed by, and the SDK already holds
  it. `ctx.emit(value)` / `emit_with_meta` on a `.keyed()` channel (Java:
  `emit` / `emitWithMeta`) now stamp `flz.key` with that key instead of
  failing with `KeyRequiredOnKeyedChannel`, so the stamped key equals the
  routing key by construction and cannot drift from it. A drifted key was
  never a local error: it ships as the record key and, inside a partitioned
  group, hashes into an arbitrary key-group and parks at the downstream
  partition guard. `emit_keyed(key, value)` is unchanged and remains the way
  to emit under a key that differs from the Cell's; both spellings share
  one stamping path. Both SDKs also expose the key directly
  (`ctx.cell_key()` / `ctx.cellKey()`). Unchanged refusals: a
  single-cell Tasklet's key-less emit on a keyed channel (there is no
  Cell key to default to) and `emit_keyed` on a channel not declared
  `.keyed()`. Backwards compatible — every currently-legal call keeps its
  behaviour; one error case became the safe default.

- **Start and enable verbs are idempotent**
  (`idempotent-start-and-enable`). `flz relay start` against a live
  instance now exits 0 as an explicit no-op (it previously exited non-zero
  with "relay id already in use"); a claim refused with no live winner —
  a stale registry row — still errors. `flz relay start --partitions N`
  over an existing group converges: only the missing members start, iff N
  matches the identity every survivor declares (else it refuses naming
  `flz relay scale`); a fresh group start and its all-or-nothing rollback
  are unchanged. `flz plugin enable` on a plugin id already registered for the
  same relay exits 0 untouched — no cspec write, no re-subscribe; the same
  id held by a different target still errors, and a group enable counts
  already-enabled members as success. Two id collisions previously accepted
  are now refused: a plain start of a live group's base id (it minted a rogue
  singleton beside the group) and a partitioned start over a live
  unpartitioned relay. Together these make re-running a cold-start script
  the supported heal after a warm restart. Scripts keying on the old non-zero
  "already exists" exit must use `flz relay info` instead.

- **The control plane has one actor, and every failure signal has one consumer**
  (`close-control-plane-silent-wedges`). Three silent-wedge classes closed
  subtractively, under one rule: sensors sense, exactly one actor acts,
  `/healthz` reads the actor's snapshot, the orchestrator restarts on
  `/healthz`.

  `Flezha.State.Watchdog` is a pure sensor again. Its direct
  `State.Recovery.resync/2` on an output stall, its `last_resync` dedup map and
  `recently_resynced?/4` are **deleted** — they were a second, uncoordinated
  remediation path racing the convergence Loop's own rebind with no shared
  backoff. The remedy is not lost: the Loop's output-stall response is now a
  ladder, `resync_runner` (a new `Heal` primitive re-running the
  `RestoreInitState` handshake against the runner the relay is already
  bound to — no migration) escalating to `rebind_relay` when the stall
  survives it. Both rungs share ONE cooldown key so escalating walks a single
  5 s → 30 s → 60 s → 300 s ladder, and the rung is read off that entry's
  attempt count rather than a second counter. Measured end to end: 0.84 ms from
  sweep to remedy decision, because the Loop ticks on the stall event rather
  than waiting out `tick_ms`.

  The data-plane stall classifier's verdict — computed every tick and, until
  now, read by nothing — is passed into `Diff.compute/3` in-process (no
  telemetry subscription, no side channel) and mapped by remedy: wedged runner
  → `evict_runner`, wedged connection → `resync_runner` for that runner's
  relays, wedged relay → `rebind_relay`. Ops ride the standard
  per-target backoff, and a target the classifier *proved* wedged is no longer
  protected by the guards that exist to spare a healthy one — neither
  `skip_rebind?`'s active-runner check nor `FleetManager.evict_runner/2`'s
  "never evict a live runner" rule, which had made a pool-wedged (still
  connected) runner unevictable.

  Controller-fabric verdicts (`driver_loop_frozen`, `nif_queue_backing_up`)
  reach `/healthz` as `503 data_plane_fabric_wedged` once sustained for
  `FLEZHA_FABRIC_503_POLLS` consecutive polls (default 3; `0` disables the
  fabric 503 and is the rollback switch). No BEAM-level heal reaches a wedged
  NIF driver thread — every heal this controller could issue travels through
  it — so the orchestrator's restart is the remedy. Verdicts the pipeline owns
  stay `200 degraded`.

  Sensors are now watched with timestamps rather than another process: a
  periodic sensor stamps its own last completed pass into
  `Convergence.Snapshot`, and `/healthz` reports one older than 3× its cadence
  as `200 degraded, reason: sensor_stale`. A Watchdog that wedges without
  crashing is visible within one poll instead of silently turning detection
  off.

  Start authority is one claim: `RelayManager.start_relay/3` now mints
  the `:duplicate` verdict itself (the CLI, schedules, tasklet-requested
  spawns and the ProcessHub orphan reconcile all resolve through the same
  registry check), and a start that loses it verifies the winner is live and
  does nothing to it. Previously the loser took the generic failure path, which
  called `stop_child_quietly/2` on the id — **stopping the winner**. A reconcile
  round completing with `started > 0` now triggers one adopt-and-converge tick,
  and `reconcile_duplicate` is alarm-grade with its kept/stopped nodes.

  Also fixed: a restarted `Convergence.Loop` left the previous incarnation's
  telemetry handler registered, so `attach_many` refused as `:already_exists`
  and the new Loop never heard another sensor event — a silently deaf actor, of
  exactly the class this change closes.

  One config key added (`FLEZHA_FABRIC_503_POLLS`); no new module, loop,
  GenServer or telemetry subscription; no wire, storage or SDK change.

- **Steps stage work runs on the SDK's workers, in both languages**
  (`steps-steps-through-sdk`). The steps driver used to fan a stage out
  onto a second, core-owned thread pool, and a Java step cost TWO threads: one
  of those pool threads submitted the crossing and then blocked on the reply
  while a Java thread ran the code. Both are gone. The driver now hands each
  cell partition out as a **step activation** on the poll seam's direct
  queue — a typed class runs it on its poll workers, a C-ABI host receives the
  same ordinary `Work` (or `CompensateRun`) activation it already knew — and
  collects the answers at the stage barrier it already owned. Every step
  therefore costs exactly one thread, no core-owned thread runs or waits on
  user code, and bounded work inherits the adaptive pool / virtual threads
  from `sdk-owned-concurrency`. The cell-retry ladder moves with it: a step
  runs ONE attempt and releases its worker, and the driver times the
  re-submission at its barrier, so a backoff window no longer occupies a
  worker (policy, classification, exhaustion, telemetry and stop
  responsiveness are unchanged). `WorkStealingPool` and every handle threaded
  through the tree are deleted — the change is a net deletion. No wire,
  journal, persistence or authoring-API change; Java bytecode is untouched.

- **Blocking user code no longer halts the runner** (`sdk-owned-concurrency`).
  The worker count used to be the limit on how many jobs could be underway:
  with the pool sized to CPU count, CPU-count slow calls (an LLM turn, a slow
  HTTP fetch) stopped everything else — not slowed, stopped. That limit now
  belongs to the core and is decoupled from any thread count: one process-wide
  cap on work activations underway (`FLEZHA_MAX_INFLIGHT_ACTIVATIONS`, default
  1024), enforced at handout and released on completion. Direct activations
  (restore folds, snapshot requests, steps relay steps) stay exempt — they are
  barrier-bounded by their own drivers. Each SDK then services that
  with whatever its runtime does best: **Rust** parks one supervisor thread in
  the core's new starvation wait (work queued, nobody inside `poll`) and grows
  its pool one thread at a time up to `FLEZHA_WORKER_POOL_MAX` (default 10×
  cores), with the grown threads retiring after an idle streak; **Java**'s
  poller threads become two platform *fetchers* that hand every activation to
  a **virtual thread**, which dispatches and completes it, so waiting costs a
  stack and the starvation condition cannot arise. No wire, controller,
  persistence or authoring-API change; a workload that never starves observes
  exactly the pool it does today.

- **Neither a retry backoff nor a processing-time timer holds a thread**
  (`sdk-deferred-wake`). One deferred-wake list inside the poll core now
  answers "do something with this cell at time T", serviced by the threads
  already waiting in `poll` — no thread of its own. A retryable failure
  records its backoff and returns the completing worker immediately (it used
  to sleep there, so a dependency outage that failed every cell could put
  every worker to sleep at once, and the sleep also delayed shutdown); the
  cell's exclusivity gates stay held across the window, so ordering and the
  head-of-line holdback are unchanged. The per-Relay timer wheel thread is
  deleted: a Relay registering timers across any number of Cells now
  costs no thread. Firing semantics are unchanged — onto the Cell's mailbox,
  serialized against `run`, ascending scheduled order, `fired_at` still the
  scheduled timestamp — except that a deadline passing while every worker is
  inside user code is promoted when the first one re-polls. Both SDKs get this
  from the shared core; no wire, controller or API change.

### Added

- **The authoring sugar can now feed a `.keyed()` channel.** A keyed output
  channel accepts only `emit_keyed`, but `#[flezha::tasklet]` / `@Tasklet`
  always generated a plain `emit` — so every tasklet with a keyed sink had to
  be hand-written as a full `Tasklet` impl, and a partitioned relay wants
  keyed sinks on *all* of its outputs. Both SDKs gain a `key` opt-in naming a
  function from the output to its natural key: Rust
  `#[flezha::tasklet(key = my_fn)]` with `fn(&Output) -> String`, Java
  `@Tasklet(key = "myMethod")` with `static String myMethod(Output)`. The
  generated emit then carries the key and the runtime stamps `flz.key`.
  Declaring `key` on a tasklet with no output is a compile-time /
  annotation-processing error. Nothing changes for a tasklet that does not
  declare it.

- **The testnet WS server speaks v2 subscriptions.** It understood only
  its own `{"event_type":"subscribe",…}` payload, whose generators were global
  to the server — so a partitioned relay could not be exercised against it:
  every member would have received every symbol and parked the out-of-range
  half. It now also accepts
  `{"method":"subscribe"|"unsubscribe","params":{"channel":…,"symbol":[…]},"req_id":N}`,
  acks once per symbol echoing the `req_id` the subscription source correlates
  on, and runs generators **per connection per symbol**. Generator tuning moves
  server-side (`FLEZHA_TESTNET_CONFIG`: per-channel generator + cadence,
  per-symbol overrides layered on shared defaults), since a v2 subscribe
  carries no room for it. The legacy payload is unchanged, and a disconnect now
  stops that connection's generators instead of leaking them.

- **One flow relay can now run across N machines**
  (`add-partitioned-relays`). A flow relay bound to exactly one
  runner, so a single logical stream could never use more than one machine —
  the only way past that box was hand-sharding into N unrelated relays with
  hand-split source configs, no routing-correctness check, no unified view, and
  no way to change N later without orphaning cell state.

  `flz relay start ohlc candles --partitions 3` materializes one logical
  relay as three **ordinary** instances (`ohlc#p0` … `ohlc#p2`), each with
  its own Overwatch, ProcessHub child, registry row, runner binding, input log,
  state tree, plugins and DLQ identity. Placement, convergence, rebind,
  migration and recovery act per member, unchanged. There is no group record:
  each member's cspec opts carry `group`/`partition`/`partitions` and its owned
  key-group range, and every group view is recomputed from the member rows.

  Keys hash into 256 key groups via the FNV-1a-128 already frozen by
  `StableId`; each partition owns a contiguous range, shipped explicitly rather
  than derived from `(index, count)`. Three implementations (`Flezha.Partition`,
  `flz_core::util::key_group`, Java's `StableId`) are pinned against one shared
  fixture-vector set.

  - **Sources filter the ask, not the data.** The websocket subscription
    source filters its desired set to the partition's range before reconciling
    (new static `subscription_items` opt supplies a literal list through the
    same machinery, mutually exclusive with `subscription_url`); `http_poll`
    gains `{{partition_index}}` / `{{partition_count}}` URL placeholders
    rendered once at init, with URL validation running on the rendered value.
  - **`relay_forward` routes per message** by `hash(flz.key)` to the owning
    member, one worker per target member. A message with no `flz.key` is parked
    with `partitioned target requires keyed emit` — never round-robined.
  - **The SDK refuses a mis-routed key on its first frame**, naming the key,
    its key group, the owning partition and the contract, before any state for
    that key exists. Additive `AddRelay` fields (`partition_index`,
    `partition_count`, `key_group_from`, `key_group_to`) carry the range; a
    class with no keyed entry-fed tasklet is refused with
    `PartitionsWithoutKeyedTasklet`.
  - **`flz relay scale <group> M`** rescales under an explicit quiesce
    (sources off, input logs consumed, DLQs empty), moves every Cell whose
    key group changes owner (rename on one controller, `WalTransfer` pull
    across controllers), then starts M members with the recomputed ranges. A
    refusal changes nothing; a crash mid-move is re-runnable.
  - **CLI**: `flz relay list` shows one row per group with per-member
    placement, `info` validates that all 256 key groups are owned exactly once
    and renders gaps/overlaps as warnings, `stop <group>` fans out, and
    `flz state get <group>/<tasklet>/<key>` resolves the owning member by
    hash. `flz plugin enable/disable <group>` fans out with partition opts
    injected and `#pK`-suffixed plugin ids.

  `--partitions 1` (the default) is byte-identical to before; all proto fields
  are additive with zero values meaning "unpartitioned". See
  `docs/sdk/partitioned_relays.md`.

- **A controller can now survive dying with its disk**
  (`add-remote-durability-tier`). Every durable byte flezha owned lived on the
  owning controller's local disk — Cell WALs and snapshots, steps relay
  task-journals, durable inboxes, the effectively-once input log, and the
  ProcessHub registry cspec that records which relays exist at all — and
  `WalTransfer` moves a subset only at migration time. Losing the disk lost all
  of it. The new `Flezha.State.DurabilityTier` ships those as immutable
  artifacts to a remote backend and hydrates them back when a controller binds
  a relay whose local state is absent.

  **Off by default.** With `FLEZHA_DURABILITY_BACKEND=none` (the default) no
  process starts, no backend is contacted, and every acknowledgement path gates
  on local fsync exactly as before. Backends are `postgres` (over the
  already-present `postgrex`) and `s3` (hand-rolled sigv4 over `:mint`); a
  backend implements only `put`/`get`/`list`/`delete`/`put_if` and never learns
  WAL semantics.

  Shipping reuses the boundaries that already existed — compaction completion,
  checkpoint-interval close, steps relay `TaskCommit`/`IterCommit` — plus one
  backstop sweep (`ship_interval_ms`, default 30 s) for cells that reach
  none of them. A shipped WAL artifact is a **seq-addressed record bundle** read
  at ship time, never a file byte range, because compaction and correction
  rewrite the live WAL in place. The per-reference manifest is written **last**
  through an epoch-conditional write, so a crash mid-upload leaves the previous
  entry authoritative and the orphan blobs invisible to recovery.

  A steps class may declare `SYNC_DURABILITY` (Rust) / `syncDurability()`
  (Java): the controller then completes the remote ship of a commit barrier
  before acking it, so the ack the driver blocks on implies the commit survives
  losing the disk (RPO 0). Flow classes are refused it at registration.
  Everything else ships asynchronously and never delays an append, ack or
  watermark advance; a stalled shipper raises the alarm-grade
  `[:flezha, :state, :rpo_exceeded]`, surfaced in `flz controller status` and
  `/healthz`. A stale-epoch manifest refusal fences that reference and alarms
  rather than overwriting a claimant that superseded it.

  Rollback is `backend: none` — local state is authoritative throughout, so
  disabling the tier alters nothing on disk. See
  [`docs/operations/remote_durability_tier.md`](docs/operations/remote_durability_tier.md).

- **Every controller node is a valid entry point for every namespace command**
  (`schedule-spawn-without-runner`). A new cluster directory
  (`Flezha.Cluster.Directory`, one `:pg` scope — no new dependency) lets each
  node advertise which factories its runners serve and which namespace hubs it
  hosts; reads are local on every node. Steps starts (`flz relay add`,
  schedule fires, `ctx.spawn_steps`) submitted on a node that cannot satisfy
  them locally are forwarded once to a capable node and re-validated there, so
  a schedule no longer fails forever when a rolling update moves its runner to
  another controller node. The command plane routes the same way: `flz
  relay send`/`stop`/`info`, `flz state get` and `flz dlq replay` resolve
  their target cluster-wide. A failed schedule slot is retried with capped
  exponential backoff under the same deterministic instance id until the next
  slot supersedes it, and `flz schedule list` gained `LAST FIRE (UTC)` /
  `LAST RESULT` columns. `factory_unknown` and "no runners" refusals are now
  honest cluster-wide verdicts. The dead `Flezha.Service.RunnerManager` was
  deleted (`Flezha.list_runners/1` now reads through the directory).

- **The Java SDK is rehosted on the poll core**
  (`rehost-java-sdk-on-poll-core`). The Java runtime's kernel mirror —
  command handlers, framing, reconnect, flusher, credit, DLQ park, timer
  wheel, the steps driver — is deleted; JVM poller threads drive
  `flz_core` through a JNI binding over `flz_core_ffi` (`FlzCore`,
  schema-negotiated at context creation). The core's host layer grew from
  v1 to full capability for this: the five command activation kinds
  (restore fold, snapshot, live read, apply-state, migration serialize),
  the prepare crossing for keys/event times, static/dynamic cell modes,
  producer-evaluated inter-tasklet edges, the effectively-once and
  checkpoint ack-granularity tiers, late-input correction, per-cell
  timers, and steps relay lifecycles (run-sugar sessions + structured chains)
  — all reachable through the C ABI and pinned by the ABI scenario suite.
  Activation schema bumped to v2 (prepare answers, `change_seq`,
  `factory`, `Output.event_time`, steps runs); both SDKs' fixture
  suites pin the same bytes.

- **The Rust SDK's kernel is now a language-agnostic poll core**
  (`extract-sdk-poll-core`). A new `flz_core` crate owns transport,
  ordering, durability, retries, timers, credit, routing and the steps relay
  driver; host SDKs drive it through one surface — `poll() -> Activation` /
  `complete(ActivationResult)` — and the core never calls host code. The
  Rust SDK is rebuilt as a typed layer over the crate (public API
  unchanged); `flz_core_ffi` exposes the same engine over a versioned C ABI
  (creation-time schema negotiation, any-thread poll/complete, shutdown
  wake), with a CI scenario driving connect → inputs → timer → shutdown
  entirely through the ABI. No wire, controller, or WAL format change.
  See `docs/sdk/poll_core.md`.

- **A live websocket's subscription set can change without reconnecting**
  (`add-websocket-subscription-source`). `Plugin.Provider.Websocket` accepts an
  optional `subscription_url` publishing the desired item set
  (`{"version": <integer>, "items": [<string>, …]}`). The provider polls it every
  `subscription_interval_ms`, diffs it against what it has applied, and sends
  user-supplied `subscribe_template` / `unsubscribe_template` frames — with
  `{{items}}` and `{{req_id}}` substituted — for the difference, on the
  connection it already holds. Previously the only way to change a subscription
  was `plugin disable` then `plugin enable`: a full teardown that dropped every
  other stream on that socket to move one item, and spent the upstream's
  reconnect budget on a change needing no reconnect.

  Item strings are opaque — never parsed, validated or interpreted — so all
  exchange vocabulary lives in the templates and in the source's response, not in
  the framework. A delta commits into the applied set only when an inbound frame
  bearing its `req_id` reports success; failed, malformed and unanswered
  requests are simply left uncommitted, so the next cycle re-derives an
  identical delta. Convergence is the retry mechanism, and there is no bespoke
  retry state. Protocols echoing no correlation id use
  `subscription_ack_mode: "none"`, committing on send.

  The applied set is persisted to the plugin cspec after every commit, so a
  plugin restart, a relay handover and a controller restart all resume with
  what the socket actually holds. That also fixes a correctness edge predating
  the feature: the data-liveness watchdog resent `post_connect`, the *boot-time*
  payload, so any runtime change would have been silently reverted by the very
  mechanism meant to heal a stalled subscription. Connect and both watchdog
  tiers now assert the current applied set.

  A failed poll is *no information*, never an empty desired set: it can never
  cause an unsubscribe. An explicitly empty `items` array is a real instruction
  and does. `flz relay info` reports the applied set, last reconcile outcome
  and last error; each reconcile that sent a frame emits
  `[:flezha, :ws, :reconcile]`. Fully backwards compatible — with no
  `subscription_url` there is no poll timer, no applied set, and no subscription
  state on the per-frame path. See `docs/plugins/websocket_subscription.md`.

- **A Cell's current value is readable without a serving tier**
  (`cell-state-live-query`). `flz state get <relay/tasklet[/cell_key]>`
  returns one live Cell's in-memory state as JSON together with the
  `change_seq` it was serialized at. Until now the only read was
  `flz state at`, which serves the controller's WAL at a point in the *past*, so
  anything needing the current value had to wait for it to reach a sink and read
  that copy — a second source of truth, always behind, free to diverge.

  The value is the freshest one there is: the SDK serializes under the Cell's
  state lock and does **not** drain pending changes, so the reply includes
  changes applied but not yet flushed, and is therefore at or ahead of what is
  durable. The seq is how a caller tells versions apart and sees how far ahead of
  durability it is. A read triggers no compaction, snapshot or WAL truncation.

  A Cell that is not held in memory is **refused naming why** — idle-evicted,
  instance suspended, no Cell by that key, or Relay unbound — and points at
  `flz state at` as the durable path. There is deliberately no durable fallback:
  dynamic-mode eviction is destructive (the recreate cold-starts), the controller
  cannot decode the SDK's WAL bytes, and a fallback would mean binding a runner
  and restoring a Cell as a side effect of a read. `cell_evicted` and
  `cell_not_found` are distinguished by a bounded per-SDK trace of what the
  reaper evicted, since eviction otherwise leaves nothing behind.

  The reply is bounded by the existing `:wal_high_watermark_bytes`; over it the
  read is refused with the byte count and the selector that would narrow it. No
  new configuration key, no new control command, no new payload-id: `CellState`
  gained two optional selector fields, and an unset selector encodes
  byte-identically, so the migration/handover caller is untouched. Dynamic-mode
  Cells are covered on both runtimes, closing a gap in the serialization
  requirement. Authorized as `:"state.get"` (viewer). Docs:
  `docs/internals/state_persistence.md`; examples:
  `sdk/rust/examples/live_state_read.rs`, Java `E11_LiveStateRead`.

- **A relay can be fed from an HTTP resource**
  (`add-http-poll-source-provider`). The built-in `relay_http_poll` source
  provider (`Flezha.Plugin.Provider.HttpPoll`) polls a URL on an interval and
  injects the response body as an ordinary journaled entry frame — but only when
  the body's SHA-256 changes, so a steady endpoint costs one request per interval
  and zero frames. The first successful poll after the plugin starts always
  injects, which converges a relay that has just been started, recovered, or
  handed over onto current content within one interval; consumers of a polled
  channel must therefore treat its frames idempotently. Until now runtime
  configuration could reach a running relay only by restarting it with new
  `init_values` or by a user hand-pushing `flz relay send`.

  Delivery uses the relay's confirming ingest and the hash advances only once
  the relay has taken the payload, so a frame dropped by a parked relay
  or an unbound stream is re-offered on the next poll instead of being lost until
  the endpoint next changes. Timeouts, connection errors, statuses outside 2xx
  (redirects included — they are not followed, since `:httpc` would replay the
  configured headers to an unvalidated host and downgrade TLS), an oversized
  body, a non-UTF-8 body, and a refused ingest all inject nothing, retain the
  last delivered hash and ETag, and count toward the standard provider health
  contract; the next success clears the run and delivers the change the outage
  hid. `ETag`/`If-None-Match` is passed through for bandwidth, with the local hash
  still the injection gate. The size cap aborts the transfer as the body arrives,
  so it bounds controller memory and not merely what the relay receives.
  HTTPS is verified against the system CA store. Attach refuses a non-http
  scheme, a hostless URL, credentials embedded in the URL, a sub-second interval,
  a timeout that could outlast its own interval, a non-positive tunable, a body
  cap above the ceiling, and any unknown option — the last on `init_provider/1`,
  the path a `--plugins` spec actually takes.

  **Controller-only**: no SDK, wire, or journal change. See
  `docs/plugins/http_poll.md`.

- **A tasklet can consume more than one channel**
  (`add-multi-input-tasklets`). `c.merge::<I>()` (Rust) / `c.merge(I.class)`
  (Java) subscribes two or more source channels to one tasklet whose `Input`
  is an author-declared sum type, each subscription supplying a pure mapping
  from its payload into that type. Entry channels and inter-tasklet channels
  are both valid sources on a flow relay, freely mixed, so a detector
  can take its data feed and
  a live config channel without the relay being restarted with new
  environment. Per-source FIFO is preserved and nothing is ordered across
  sources; `cells_for` sees the mapped value; the mappings run identically on
  live delivery and WAL replay, so a warm restart restores the same state.
  Fewer than two sources, the same channel subscribed twice, or a source that
  would re-wire an already-declared edge all fail relay definition with a
  named topology error. A **steps relay** relay merges its tasklets' output
  streams — its entry channel seeds the root from the spawn args and names no
  channel, so an entry channel as a steps relay merge source is refused by name.
  Single-input wiring is unchanged.

  **No wire change and no controller change**: the merge is a subscription-time
  construct in the SDK, frames are still journaled per source channel, and the
  controller has no model of a channel's consumers. See
  `docs/sdk/multi_input_tasklets.md`.

- **A tasklet can ask for a steps relay**
  (`add-tasklet-requested-spawn`). `ctx.spawn_steps` (Rust) /
  `ctx.spawnSteps` (Java) spawns-or-delivers a steps instance on one
  `(namespace, instance_id)` from inside any tasklet: a free id starts the
  factory with the payload as its first input, a taken id receives the payload
  as one more input (`if_running: deliver`) or refuses (`error`). Concurrent
  requests race-resolve at the relay registry to exactly one instance.
  Failures — quota, unknown factory, unreachable controller — resolve the
  caller's async handle and never fault the requesting tasklet; an
  uninspected handle logs the failure once. Spawning is a live-processing side
  effect: a replayed invocation issues nothing. Until now a steps relay
  could only be started from outside the cluster, so every pipeline that needed
  one grew its own external dispatcher. See
  `docs/sdk/tasklet_requested_spawn.md`.

  **Wire**: two additive `CommandRequest` variants, `SpawnStepsRequest`
  (tag 34, SDK → controller) and `SpawnStepsResponse` (tag 35,
  controller → SDK). Additive-tag compatibility as with every prior wire
  change: an older controller ignores the unknown variant (the requester times
  out), so the controller image and both SDKs are released together.

- `flz schedule add --plugins <json>` — a schedule entry can now carry the
  same plugin-spec array `flz relay start --plugins` accepts
  (`schedule-entries-carry-plugins`). The spec is validated against the
  enabled providers at `add` time, stored verbatim, and decoded at every
  firing; each firing stamps its namespace and instance id on every plugin
  and prefixes the plugin id with the instance id so overlapping slots never
  collide. Without this a cron-fired steps relay job had no output channel at all
  — a steps instance finishes in milliseconds, so plugins must be attached
  at spawn. A spec that fails to decode at fire time starts nothing and is
  reported on `[:flezha, :schedule, :spawn_failed]` rather than firing a run
  whose output is dropped. The schedule row gained a field, so the
  delete-`schedules.dets`-and-re-add note below applies to this change too.

### Changed (BREAKING)

- Scheduled steps spawns are now driven by **one schedule process per
  schedule, hosted in a dedicated `:flz_schedules` ProcessHub**, replacing the
  per-controller polling evaluator (`single-owner-schedule-processes`). The
  hub's `Singularity` redundancy makes exactly one process exist cluster-wide
  per schedule name, so a slot fires once without leader election and without
  cross-node state sync: every controller asserts the definitions its own
  node-local store holds and duplicate assertions no-op. This closes the
  documented multi-controller double-fire hole. `Flezha.Schedule.Evaluator`
  is removed; the schedule entry's `last_slot` watermark is gone (a process
  holding an armed timer *is* the watermark) and with it
  `FLEZHA_SCHEDULE_TICK_MS`. **A `schedules.dets` written by an earlier build
  must be deleted (`rm app_data/schedules/schedules.dets`) and its schedules
  re-added** — the row shape changed and no migration is provided.
  `flz schedule list` gains a `NODE` column. On start a schedule process
  fires the single most recent slot inside `FLEZHA_SCHEDULE_MISFIRE_GRACE_MS`
  to cover restart and migration gaps; older slots are still never
  back-filled.

- New guarded SDK entry point in both SDKs (`runner-lifecycle-singleton-api`,
  superseded before release by `sdk-runner-entrypoint-redesign` — the shipping
  surface is the one described here). `Flezha.newInstance(namespace)` (Java) /
  `flezha_runner::new(namespace)` (Rust) and the
  `Flezha.registerFlow`/`registerSteps` /
  `register_flow::<R>()`/`register_steps::<T>()` registration surface
  are removed with no back-compat shim. The replacement is a runner handle —
  no builder, no freeze step:
  `Flezha.createRunner(ns).defineRelay(A.class).run()` (Java) /
  `Flezha::create_runner(ns)?.define_relay::<A>()?.run()?` (Rust).
  `RunnerBuilder` and both Java registry facades
  are gone.
  The declared set is per-runner and starts empty each generation;
  `define_*`/`remove_*` (`removeFlow`/`removeSteps` /
  `remove_flow::<R>()`/`remove_steps::<T>()`) are legal before and
  after `start()`, a post-start change shipping the full factory set as a
  fire-and-forget `UpdateFactories`. A removal refuses while live instances
  of the factory exist (`RelayTypeInUseException` /
  `FlezhaError::RelayTypeInUse`). Starting with zero declared classes is
  legal.
  The singleton slot is claimed at `start()`, not at create: any number of
  unstarted runners may exist, and a second `start()`/`run()` fails loudly
  (`IllegalStateException` / `FlezhaError::AlreadyRunning`) naming the running
  namespace instead of silently corrupting the process-global
  identity/sink/credit state; after `close()`/`Drop` a new runner may start.
  `create_runner` is what returns the `FlezhaRunner` handle — `start()`
  returns `Result<&Self, FlezhaError>` (bind it: a chained
  `…create_runner(ns)?.start()?` drops the temporary at statement end),
  `run()` = `start()` + `awaitTermination()` (standalone binaries — the
  `Thread.currentThread().join()` / stdin-loop keep-alive hacks are gone),
  and `Flezha.currentRunner()` / `Flezha::current_runner()` exposes the
  active runner. Rust declaration, connect, and lifecycle failures are now
  ONE typed `Result` error, `FlezhaError` (wrapping `TopologyBuildError` /
  `BuildStepsError`), instead of panics and the removed
  `RunnerStartError`/`RunnerError`; the per-issue topology enum is
  `TopologyIssue`. Java's annotation processor generates
  `GeneratedFlowRelays.defineAll(FlezhaRunner)`.
  Naming rules for future SDK surface: `docs/sdk/naming_conventions.md`.

### Fixed

- **A rescale left everything forwarding into the group routing by the old
  member set.** `relay_forward` resolves its target's members once, at
  `init_provider/1`, and nothing re-ran it: the workers stayed alive, so the
  plugin reported healthy while the worker for a removed member retried
  forever against a relay that no longer existed — until its queue
  saturated and back-pressured the **source** relay's output chain. A
  forward whose resolved members no longer match the group now reports itself
  unhealthy, and the convergence loop restarts it so it re-reads the member
  set. Surviving members keep their workers
  and their queued payloads; a removed member hands its queue back before its
  worker stops and each payload is re-routed to whoever owns its key now, in
  order and without loss. A restarted provider that exports no `activate/1`
  also returns to `:running` rather than sitting `:prepared` — where
  `plugin_health/3` would have reported it healthy without probing until the
  next bind.

- **Two concurrent `flz relay scale` calls on one group could leave it half
  started** — each read the member set the other was rewriting, so the loser's
  rollback stopped the members the winner had just started. A rescale now takes
  a cluster-wide lock on `{namespace, group}` and a second one is refused by
  name. The lock is released if the holder dies, which is the crash case the
  re-runnable move sequence already assumes.

- **A rescale could restart a member before its id was released.** The stop
  replies before ProcessHub finishes unregistering the child, so reusing the id
  — every rescale reuses `#p0` — could lose the claim to the member it had just
  stopped. The stop now waits for the same uniqueness predicate the restart
  checks, and gives up before anything durable has moved.

- **A dynamic Cell never restored its durable state on a rebind**
  (found implementing `add-partitioned-relays`). The SDK's restore handler
  resolved the named Cell by walking `instance.cells` — the static and
  single slots — while a dynamic Cell lives in its Tasklet's registry and
  has no slot until its first input. Every `RestoreInitState` for one was
  therefore acked with its terminal seq and discarded, and the next input
  cold-started the Cell on top of its own WAL. The handler now materializes
  the named dynamic Cell and folds the state into it. Eviction is unchanged
  and does not conflict: it deletes the files, so no restore is ever shipped for
  an evicted Cell. This is also what carries a key across a partition
  rescale, where its state moved into another member's tree before that member
  ever saw the key.

- A controller that lost **every runner** for a namespace could neither keep its
  relays nor move them, and the healing machinery hid the damage
  (`fix-last-runner-migration-state-loss`). Three defects compounded:
  `hub_migrate` had been reduced to a vacancy check that no-ops exactly when the
  hub still holds relays — i.e. whenever there is something to migrate;
  `:runner_migrate` committed the binding *before* the runner answered, so a
  failed heal left the relay naming a **live but unreachable** runner, which
  made `Convergence.Diff` stop classifying it misbound and emptied the delta
  permanently; and because a handover serializes its state from the **runner**,
  a migration caused by that runner's death shipped an empty payload while still
  marking `arrived_via_hotswap?`, which suppressed the WAL restore *and* the
  explicit repair path. The binding is now committed only on success, the flag
  only when the handover carried state, and `hub_migrate` drains the hub
  (`HubManager.drain_hub/2`, the seam graceful shutdown already used) so
  relays leave through a live HotSwap handover. New
  `Flezha.State.WalTransfer` moves what the handover cannot: a
  destination-pulled, chunked `:erpc` copy of the Cell's durable files,
  written to a `.transferring` sibling that is fsynced and atomically renamed,
  with names resolved through the Cell's own file set so a peer cannot read
  arbitrary paths. `state_root` stays node-local and is never assumed shared.
  One-shot at migration time, not replication — a controller that dies together
  with its disk still takes its state with it.

- A durable steps relay whose tasklets hold **no per-Cell state**
  went silent after its first turn. The task-journal is instance-scoped but rode
  `RestoreInitState.task_journal`, a per-Cell frame — and such a class has no
  Cell WAL, so the controller shipped no restore at all. Every wake resumed
  at iteration 0, and each turn's commit collided with iteration 0's
  `outbox_flushed` marker (the flush marker is keyed `{type, task_idx, iter}`),
  so its staged output was journaled and never delivered. Only the first turn
  of such a relay was ever emitted; it bit any instance that parked
  between turns, on both SDKs.
  `TaskJournalState` moves from `RestoreInitState` (field 6, removed) to
  `AddRelay` (field 6, added) — instance-scoped state on the bind, beside
  `resume_cell_count`, delivered once per bind instead of once per persisted
  Cell and before the driver spawns. No compatibility shim: an SDK and a
  controller must be upgraded together.

### Changed

- A relay may now declare **more than one entry channel** (both SDKs). The
  `sdk-graph-authoring` graph-shape rule is relaxed from "exactly one entry" to
  "one or more entries, every node reachable from some entry"; the
  `MultipleEntryChannels` topology error is removed. The runtime already routed
  each inbound frame to its entry by `metadata["channel"]` (falling back to the
  first-declared entry), so this only lifts the build-time refusal — enabling a
  single relay to ingest several external sources (e.g. a market pipeline
  reading a trade feed and an order-book feed on distinct channels). Steps relay
  relays keep their single-entry linear-chain rule.

- Plugin subscriptions added via `flz plugin enable` now survive any
  Overwatch (re)start automatically. The ProcessHub child-spec is the
  source of truth for a relay's enabled plugins; the runtime
  `Overwatch.state.extensions` table is rebuilt from it on every
  `init/1`. Previously, plugins enabled after `flz relay start`
  were dropped on runner-pod restart, agent restart with intact
  ProcessHub state, or any in-place restart that ran through
  `HubManager.alter_child_data/2` (e.g. `kubectl rollout restart
  daemonset/<sdk-daemonset>`), forcing runners to re-run the enable
  commands manually before the data plane recovered. See
  `openspec/specs/plugin-subscription-persistence/spec.md`.

  **Upgrade note for clusters with already-running relays.**
  Relays whose plugins were enabled dynamically before this
  upgrade have an empty `opts[:plugins]` in the cspec. They keep
  working as long as no Overwatch (re)start happens before the next
  `flz plugin enable` / `flz plugin disable` call, which canonicalises
  the cspec. Runners who want to canonicalise immediately can run
  `flz plugin disable <id> && flz plugin enable <id>` for each
  plugin. No automatic migration job is shipped.

### Added

- Controller-native cron scheduling of steps relays
  (`add-scheduled-relay-spawns`). `flz schedule add <name> --cron
  "<expr>" --tz <iana> --factory <factory_id> [--namespace ns]
  [--init_values json]` registers a durable schedule (node-local DETS at
  `app_data/schedules/schedules.dets`; names are capped at 11 bytes so the
  composed instance id fits the 32-byte relay id limit); `list` /
  `remove` / `run <name>` complete the family. A per-controller evaluator
  (`Flezha.Schedule.Evaluator`) fires each matching slot through the
  standard steps start path with deterministic instance id
  `<name>@<slot-iso>` — a duplicate fire attempt no-ops on the id, which is
  the cluster-wide exactly-once guard. Slots are evaluated in the entry's
  IANA time zone (new `tz` dependency): a spring-forward gap slot skips, a
  fall-back ambiguous slot fires once at the first occurrence. Slots older
  than `FLEZHA_SCHEDULE_MISFIRE_GRACE_MS` (default 120 s) at evaluation —
  e.g. missed while the controller was down — are skipped and logged with
  their timestamps, never back-filled; a still-running previous slot never
  blocks the next one (ids differ; bound runtime via `max_duration`). Only
  steps relays are schedulable. Telemetry: `[:flezha, :schedule,
  :fired | :skipped | :spawn_failed]`. Guide in
  [`docs/operations/schedules.md`](docs/operations/schedules.md).

- Hardened first-party Kafka producer plugin
  (`Flezha.Plugin.Provider.Kafka.Producer`). The previous thin
  `KafkaEx.produce`-per-call wrapper is replaced with a per-`(plugin_id,
  topic, partition)` `ProducerWorker` GenServer that runs linger-based
  batching, bounded outbound queueing with configurable overflow strategy
  (`:drop_oldest` / `:drop_newest` / `:block`), automatic retries with
  exponential backoff for every transient broker error class, active
  metadata refresh on `:not_leader_for_partition` / `:no_broker` /
  `:unknown_topic_or_partition`, idempotent producer semantics via Produce
  v5+ (`InitProducerId` + per-partition sequence numbers), and auto-create
  on first publish for unknown topics. Underlying `KafkaEx.New.Client`
  workers are deduplicated across plugin instances by connection identity
  via the new `Flezha.Plugin.Provider.Kafka.WorkerRegistry`. New plugin
  parameters: `:retries`, `:retry_backoff_ms`, `:retry_max_backoff_ms`,
  `:delivery_timeout_ms`, `:linger_ms`, `:max_batch_bytes`, `:queue_max`,
  `:queue_overflow`, `:queue_block_timeout_ms`, `:idempotent`,
  `:auto_create_topic`, `:auto_create_partitions`,
  `:auto_create_replication_factor`, `:topic_configs`,
  `:metadata_refresh_timeout_ms`. Six new telemetry events under the
  `[:flezha, :plugin, :kafka, _, _]` prefix surface every state
  transition. Synchronous counters available via
  `Flezha.Plugin.Provider.Kafka.Producer.stats/1`. Full runner-facing
  contract in [`docs/plugins/kafka_producer.md`](docs/plugins/kafka_producer.md).

### Changed (BREAKING)

- `:required_acks` default for the Kafka producer plugin is now `-1`
  (acks=all). Was `1`. Combined with the new default `:idempotent =
  true`, this is the durability-correct default — retries cannot create
  duplicates on the broker, and acks=all stays equivalent to acks=1 in
  steady state on clusters with `min.insync.replicas = 1`. Callers that
  depended on the old fast-but-lossy single-broker-ack latency profile
  should explicitly set `required_acks: 1, idempotent: false` per the
  migration table in [`docs/plugins/kafka_producer.md`](docs/plugins/kafka_producer.md).
- `Flezha.Plugin.Provider.Kafka.Producer.init_provider/1` now returns
  `{:ok, opts, producer_pid}` where `producer_pid` is the
  `ProducerWorker` GenServer (was the kafka_ex worker pid). The
  underlying kafka_ex worker is shared across plugin instances and is no
  longer directly addressable from plugin opts. Plugin host code that
  relied on holding a direct kafka_ex worker pid should use
  `Producer.stats/1` or attach a `:telemetry` handler instead.

- SDKs ship a final state snapshot to the agent on graceful shutdown so
  rolling deploys preserve the latest state. Rust and Java SDKs now emit
  one `RunnerShutdownNotification{relay_id, serialized_state}`
  per running relay on the control channel when the SDK is dropped,
  receives SIGTERM (Kubernetes pre-stop), or is closed explicitly via
  `Flezha.close()` (Java) / handle drop (Rust); the agent's
  `Flezha.Runner.ControlChannel` overwrites
  `Flezha.Relay.Overwatch.state.snapshot` with the supplied binary
  and emits an `RunnerShutdownAck`. Per-relay ack wait is bounded
  at 2 s, parallelised across relays. SIGKILL bypasses as expected;
  agent falls back to the last periodic snapshot. Disabled-snapshot
  deployments (`overwatch_state_snapshot_interval_ms == 0`, e.g.
  `flezha_market`) get a one-shot shutdown handoff that survives until
  the next `AddRelay` reissue. New protobuf messages
  `RunnerShutdownNotification` (tag 12) and `RunnerShutdownAck`
  (tag 13) are additive — older agents/SDKs interoperate by falling
  back to the periodic-snapshot freshness shipped by
  `heal-connections-on-restart`.
- Runner ↔ agent connections now heal automatically across either-side
  restarts; relays resume traffic within 5 s of reconnect without
  runner intervention. Stream rebind is enforced as a hard contract on
  `Flezha.Runner.StreamChannel`, the UDS/QUIC acceptors atomically
  bind incoming streams only to nil-ref channels, and
  `Flezha.Runner.FleetManager.runner_connect/3` re-issues
  `AddRelay` for relays ProcessHub still has tagged to a dead
  runner_id matching the connecting runner's profile. Rust and
  Java SDK `AddRelay` handlers are now idempotent on duplicate
  `relay_id` (log + skip).
- `mix test.connection_healing` opt-in alias that runs
  `test/scripts/connection_healing.sh` against the dev compose stack to
  exercise both restart paths end-to-end. Requires Docker on the host;
  not part of the default `mix test` budget.
- `test/flezha/deployment/k8s_manifest_test.exs` k8s manifest sanity test
  that asserts the daemonset / configmap fields the connection-healing
  flow relies on (`flezha-sockets` volume, `FLEZHA_SOCKET_FILES_PATH`,
  `FLEZHA_RUNNER_KEEPALIVE`) stay well-formed.

## 0.1.0 — Cluster snapshot & restore (replaces DETS recovery)

Restart-survival is now runner-driven via `flz cluster snapshot/restore`.
The previous DETS-based automatic recovery subsystem has been removed
along with the SYSMODE coordinator GenServer that drove it.

### Added

- New top-level CLI commands `flz cluster snapshot <file>` and
  `flz cluster restore <file>` for explicit, runner-driven cluster-wide
  state capture and restore. Both commands accept `--namespace NS` for
  per-namespace tasklets; default behaviour without the flag is
  cluster-wide. (Both verbs were later removed; `flz boot
  import-legacy-snapshot` reads the format they produced.)
- `Flezha.Cluster.Snapshot` and `Flezha.Cluster.Snapshot.Coordinator`
  Elixir modules implementing the gather/apply RPC fan-out and the
  atomic ETF file write.
- `Flezha.Cluster.Snapshot.PluginRecoverability` predicate that decides
  which plugins survive a snapshot/restore cycle.
- `Flezha.Relay.Builder` now recognises `:initial_state_binary` in
  opts; `Flezha.Relay.Overwatch` sends a new `CellApplyState`
  control command to the compute engine before the first input is
  forwarded so a re-spawned relay begins with primed state.
- New protobuf message `CellApplyState` and corresponding handler
  in the Rust and Java SDKs. Default impl returns the canonical
  `apply_state_not_supported` error so older SDK builds are detectable;
  the agent logs once per relay on apply failure.
- `:flezha, :snapshot_state_cap_bytes` configuration key (default 8 MB),
  with `FLEZHA_SNAPSHOT_STATE_CAP_BYTES` env-var override.

### Removed (BREAKING)

- The entire DETS-based recovery subsystem:
  `Flezha.Recovery.Store`, `Flezha.Recovery.PluginStore`,
  `Flezha.Recovery.Coordinator`, `Flezha.Recovery.SysMode`. The agent no
  longer writes child specs to a DETS file on relay start, no longer
  reads DETS at boot, no longer maintains a SYSMODE state machine, and
  no longer exchanges SYSMODE with peers. Recovery from an unattended
  crash now requires the runner to run `flz cluster restore` against
  a previously-captured snapshot file.
- `Flezha.Relay.InitState.Providers.Sibling` along with its
  registration in `:enabled_plugins`, its capability spec
  (`relay-init-state-sibling`), and the `lib/flezha/relay/init_state/`
  tree. Restart-survival init-state seeding is now handled by
  `flz cluster restore` priming `:initial_state_binary` directly. The
  generic `relay-init-state` provider chain remains for non-snapshot
  sources (HTTP, S3, computed defaults, custom modules implementing
  `on_relay_init/2`).
- Application env keys: `:flezha, :recovery_window_ms`,
  `:flezha, :recovery_data_dir`, `:flezha, :recovery_enabled`,
  `:flezha, :recovery_autostart`, `:flezha, :prefer_explicit_restore?`.
  Env-var overrides removed: `FLEZHA_RECOVERY_*`, `FLEZHA_DATA_DIR`,
  `FLEZHA_PREFER_EXPLICIT_RESTORE`.
- ProcessHub `post_children_start_hook` and `child_unregistered_hook`
  callbacks into the recovery coordinator.
- `failure-recovery` capability and its spec.
- `scripts/recovery_e2e.sh` shell-driven recovery smoke test.
- `docker/prod/docker-compose.local-test.yml` overlay.

### Plugin installer

- `Flezha.Plugin.Installer.Store` no longer co-locates its DETS file
  under the (now-removed) `recovery_data_dir`. New config knob:
  `:flezha, :plugin_installer_data_dir` (defaults to the installer's
  `:plugin_root`, which itself defaults to `app_data/plugins`).
