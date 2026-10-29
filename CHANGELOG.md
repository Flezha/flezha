# Changelog

## Unreleased

Nothing has been published yet. The first entry is written when 0.1.0 is
released; until then, this section names what changes for a user of the
v0.1.0-alpha.1 pre-release.

### Changed

- The controller keeps a relay history: one `started` record when a relay run
  is started on purpose (by an operator, a partition group start, a schedule, a
  tasklet's spawn, a relay's call or a rescale) and one `ended` record when it
  ends for good, naming who or what started it, how it ended (`completed`,
  `undone`, `failed`, `stopped`, `timed_out`, `rescaled_away`, `cancelled`)
  and how long it ran; a start that fails writes `start_failed`. Moves,
  failovers, crash restarts, sleeps and wakes write nothing. It is on by
  default, works with observability off, and lives in
  `storage/app/relay_history/`, capped at 1 GiB
  ([Running and testing](docs/public/topics/core/running_and_testing.md#the-relay-history)).
- `flz logs` reads every durable log (`kinds`, `read`, `tail`, `files`,
  `stats`, `rotate`) across the whole cluster, newest first with a `next`
  cursor; `flz audit` is now `flz logs ... audit`, and `flz audit read`
  without a file reads the cluster newest first. Breaking for a direct service
  caller: `Flezha.Service.AuditLog` is replaced by `Flezha.Service.Logs`, each
  call naming its `kind`. Audit records carry wall-clock `ts_us` and the
  controller that wrote them in `node`; files written before keep being read
  through their `ts_iso`.
- The audit sink's bus filter is now the sources its allow-list names, so the
  bus no longer calls it, and no producer waits on it, for any other signal.
  Concurrent audit records share one sync. `:audit_log, :fsync_mode` takes
  `:interval`, which now syncs on its interval, with `:batched` kept as its
  spelling.
- `RelayCompleted` gains `undo` and `SpawnStepsRequest` gains
  `parent_relay_id`: every SDK picks both up when it is rebuilt against the
  new core, so a rolled-back run is told apart from a failed one and a spawned
  run names the relay that asked for it. An older runner sends neither.
- Large listings can be read one page at a time. `Relay.list`,
  `PluginManager.enabled`, `Dlq.list` and the steps reads `journal`,
  `committed_iterations`, `cells` and `undelivered` take an optional `opts` of
  `limit`, `after`, `sort` and `filter` and then answer `rows`, `total`, a
  `next` cursor and `unreachable`; a paged `Relay.list` adds the namespace's
  `summary`. `Controller.liveness_counters` takes a filter of named relays.
  `flz relay list`, `flz plugin list` and `flz dlq list` gain `--limit`,
  `--after` and filter flags ([CLI](docs/public/topics/cli/cli.md#listing-by-the-page)).
  Dead-letter stats are kept in memory and read no entry file. Every answer
  read without `opts` is unchanged.
- Reads answer for the whole cluster, whichever controller the caller is
  connected to: a relay's health is its hosting controller's verdict, a
  controller outside a namespace's hub lists its relays, the factory list holds
  every controller's factories, and steps reads, `Relay.info` and
  `Relay.diagnose` run where the relay or its state is
  ([Clustering](docs/public/topics/deploying/clustering.md#how-it-works)). The
  list answers of `Relay.list`, `Relay.relay_rows`, `Relay.relay_plugins`,
  `Relay.list_namespaces`, `Fleet.factories`, `Fleet.runners_for_factory` and
  `Controller.controllers` are now `{rows, unreachable}` instead of a bare list,
  so every SDK's `list_relays` answers that shape too; `unreachable` names the
  controllers that did not answer, and each row carries its `node`.
  `Relay.info`, `Controller.cluster_status` and
  `Controller.liveness_counters` gain `unreachable`. `Relay.list_namespaces`
  rows gain `nodes`, and `running` means running on any controller.
  `Controller.cluster_status` no longer answers `leader`, `is_leader` or
  `local_node`, and its node rows no `leader`; it answers `answered_by`.
  `Fleet.factories` rows gain `nodes`.
- The operational reads answer for the whole cluster too
  ([Clustering](docs/public/topics/deploying/clustering.md#how-it-works)).
  `Dlq.list`, `Dlq.stats` and `Dlq.tasklets` answer every controller's dead
  letters as `{rows, unreachable}`, each row with its `node`, and
  `Dlq.tasklets` rows are maps; `Dlq.get`, `Dlq.drop` and `Dlq.replay` act on
  the entry wherever it is parked. `Schedule.list` answers `{rows,
  unreachable}`, each row with the `node` holding the definition and
  `fires_on`, the node whose job fires it (formerly `node`); `Schedule.run` and
  `Schedule.remove` act on the controller holding it, and `Schedule.add`
  refuses a name any controller holds, naming that controller
  (`schedule.already_exists` gains `node`). `Observability.query` takes
  `since_us`, a wall-clock bound in microseconds, instead of `from_ts`, and
  answers `{rows, unreachable}` merged across controllers; every signal it or
  a tail hands on carries `node` and `at_us`. `Observability.list_sources`
  answers one row per source with `statuses` per controller in place of
  `status`, `list_sinks` answers `{rows, unreachable}`, and `enable_source` and
  `disable_source` apply on every controller and answer `{applied,
  unreachable}`. `Namespace.quota` answers `{total, by_node, unreachable}`.
  `Controller.status`, `boot_status`, `mem_top`, `durability_breaches`,
  `durability_status` and `Fleet.installed_plugins` answer `{rows,
  unreachable}`; `Fleet.providers` answers `{rows, refused, unreachable}`,
  each provider with its `nodes` and `refused` per controller.
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

- Stopping a halted steps relay removes it at once, instead of waiting for an
  answer its runner never sends.
- The convergence loop's start of a relay that ran nowhere no longer announces
  it as started.

- A cold move no longer loses input the old copy had already taken. A relay
  without an input log now stops behind the last input it was sent: its runner
  applies everything ahead of the stop and the records reach disk before the
  relay's files are handed over, within the stop's usual time limit.
- A relay that answers a move's readiness check after the check gave up no
  longer leaves an error line in the controller's log, and its late "ready"
  can no longer be taken as its answer to a later check.
- A controller shut down just after its cluster changed now hands its relays
  to a peer. The drain used to give up when the hub still had queued work,
  so the relays stopped and were restored from disk later instead of moving
  live; it now waits its turn behind that work inside the drain's time budget.
- Listing a namespace's relays or its enabled plugins no longer slows with the
  square of the fleet. At 2,000 relays a relay listing took about 14 seconds,
  past the caller's timeout, and a console showed the namespace as empty; it
  now answers in well under a second.
- A relay whose move between controllers was never finished no longer keeps
  confirming input it does not run. Once a move takes a relay's state its
  runner drops the relay, and when the hub went down before finishing the
  move, the relay stayed without it and the healthy runners were restarted
  as wedged. A relay no replacement took over within 30 s now binds to its
  runner again and restores from its saved state; one whose replacement runs
  all the same stops.
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
