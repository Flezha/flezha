# Changelog

## Unreleased

Nothing has been published yet. The first entry is written when 0.1.0 is
released; until then, this section names what changes for a user of the
v0.1.0-alpha.1 pre-release.

### Changed

- Relays move between controllers by cold swap by default: a relay stops on the
  controller it leaves before it starts on the new one, which copies its saved
  state first, so only one copy ever runs. During the move the relay answers
  nothing; a sender that waits for the answer sends again, and input sent
  without waiting is lost, as in a crash. A namespace that needs the relay to
  keep answering while it moves sets `flz namespace set-migration <name> hot`,
  and `flz namespace show` reports the setting
  ([Namespaces](docs/public/topics/core/namespaces.md#how-its-relays-move)).
  Upgrade every controller together to this release.
- The console is now named Flezha Console, in the `flezha_console` repository.
  The build setting that points at a console checkout is `FLEZHA_CONSOLE_PATH`,
  which replaces `FLEZHA_UI_PATH`.
- `flz template list` and `flz template create` are removed. Each release now
  publishes every SDK language's starter project as
  `flezha-<language>-starter.tar.gz`, downloaded and unpacked with one
  `curl ... | tar xz` and nothing else installed
  ([Starter projects](docs/public/topics/core/starter_projects.md)). The MCP
  tool `flezha_new_project` answers the same files as before; its refusal of an
  unknown template now lists the valid ones.
- `Relay.overwatch_state` no longer sends a relay's own data: its init values,
  snapshot, held outputs and seed answer `nil`. A relay started with large init
  values, such as a long price history, made each read copy all of it, enough
  to exhaust a console that read it every few seconds.
- `Controller.liveness_counters` answers the counters of every connected
  controller, not only the one asked. A console pinned to one controller saw
  no counters for a relay running on another, so its relay graph never showed
  data moving.
- `flz` sends each verb to the controller as one request over the controller's
  control socket, the Unix socket its runners attach to, instead of starting
  an Erlang VM for every command. Verbs start faster, need no Erlang cookie, and
  a controller whose runners use QUIC still opens the socket for `flz`. A shell
  reaches the controller whose `FLEZHA_SOCKET_FILES_PATH` it shares
  ([CLI](docs/public/topics/cli/cli.md#how-it-reaches-the-controller)). Use the
  `flz` of the controller's own release: an older controller is reported as
  `cli.protocol_unsupported` after five seconds. `flz controller start`, the
  `flz plugin install | uninstall | sync` verbs and `flz mcp`'s knowledge tools
  with no controller still start a VM of their own.

### Fixed

- With a durability backend, a busy controller's remote copy no longer falls
  minutes behind while `/healthz` reports no lag. Every workflow step and
  checkpoint asked for a ship of each file its relay held, and under load
  those requests queued faster than they shipped, holding the periodic ship
  and the RPO alarm behind them. A `durable: local` relay's changes now ship
  on the periodic ship alone (`FLEZHA_DURABILITY_SHIP_INTERVAL_MS`, 30 s), so
  its copy is about one interval behind, and a copy behind past the RPO target
  shows on `/healthz` and `flz controller status`.
- A network partition that heals no longer leaves a namespace unable to start
  relays. A controller's hub could crash as the partition healed and come back
  with an empty table of which controllers run which factory, while its peers
  kept the old rows, so starts were refused until the controller restarted.
  The hub no longer crashes on a peer that is slow to read, and a hub that
  restarts for any other reason rebuilds the table from the runners connected
  now and from its peers. A start that lands before the table is back answers
  `relay.factory_unserved`, which may be retried.
- A relay stopped from another controller right after its start answered is
  stopped, instead of the stop answering `relay_not_found` while the relay
  kept running. The start answers once the relay's own controller registered
  it, before the others have heard of it; a controller that does not know the
  relay now asks the others before it answers not found.
- A controller that restarts into a running cluster gets its relays back
  instead of seeing them started on a peer. Its first recovery round could run
  before its own runners reconnected and place them on the peers already
  serving their factories, or the peers started them themselves as it rejoined,
  so a workflow could run its steps twice. The peers now keep its factories
  for it and its first recovery round waits for one of its own runners, both
  for at most `FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS`; a start that reaches
  it first answers `relay.factory_unserved`.
- A `flz` verb called over Erlang distribution from another node writes its
  refusal to that node's stderr, beside its answer. It went to the
  controller's log, so the caller saw only the exit code.
- A relay moved live to another controller keeps its pending timers. The move
  shipped the Cells' values without their timers or change sequence, so a
  session timer set before the move never fired, and a compaction there could
  truncate the record that held it. A compaction is now also reported complete
  only once its snapshot is on disk.
- A large message to a runner is no longer dropped after the send buffer
  reopens. The buffer reopens at half full, and a message too big for the room
  left was answered as sent and then thrown away; a Cell restored that way
  came back empty. The message now waits until it fits.
- A relay stopped and started again under the same id no longer comes back
  with its old Cell state. The runner ships each Cell's last change as it
  drops the relay, and that change could arrive after the stop had deleted the
  relay's files, writing them again for the next start to restore. The stop
  now waits for the runner's answer and takes those changes in before it
  deletes anything.
- A disabled plugin no longer comes back on a relay that moved to another
  controller. A controller that had not yet heard of the disable could start
  the plugin again for a moment, the convergence loop bound the relay to it,
  and nothing took it down afterwards. The loop now stops a running plugin
  whose record is gone.
- Controllers no longer disagree for good on which of them serve a factory. A
  controller that joined while another changed what it serves could miss that
  change, and relay starts it placed were then forwarded without an answer. A
  joining controller is now sent each served set once it is in the cluster.
- The convergence loop no longer lets heals that keep waiting fill every tick.
  Twenty or more relays it could not place, such as relays whose factory no
  runner offers, took the whole per-tick budget, so a plugin whose controller
  died was never restarted elsewhere. A heal that found its precondition unmet
  now waits behind the others on the next tick.
- A `ctx.effect` result kept while the relay's input log stops is answered to the
  runner instead of failing with `internal.uncatalogued`.
- A plugin copy a disable stopped is no longer still listed on its controller
  for a moment after `flz plugin disable` answers.
- A plugin disabled through one controller no longer comes back on a relay
  hosted by another. The disable now waits until every controller of the
  namespace has dropped the plugin's record before it tells the relays, so a
  convergence tick on a slower controller cannot bring the binding up again.
