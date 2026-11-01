# Changelog

## Unreleased

Nothing has been published yet. The first entry is written when 0.1.0 is
released; until then, this section names what changes for a user of the
v0.1.0-alpha.1 pre-release.

### Added

- Distribution over mutual TLS, tested: `deploy/dist-tls/dist_tls.conf` is an options file
  that checks peers on both sides and offers TLS 1.3, and `deploy/dist-tls/make-certs.sh`
  makes a distribution authority and node certificates
  ([distribution with mutual TLS](docs/public/topics/core/authorization.md#distribution-with-mutual-tls)).
- The controller warns on every boot when distribution off loopback is plain TCP, and, over
  TLS, when its options file cannot be read, does not check peers, offers only TLS 1.2, or
  trusts the runner transport's authority. Nothing is refused.
- `FLEZHA_DIST_PORT` pins the distribution port of a started controller, so a firewall can
  name it; `flzvm rpc` and `remote` do not take it, and unset nothing changes.
- The Kubernetes manifests pin the distribution port to 9100 and ship
  `flezha-networkpolicy.yaml`, which admits EPMD and that port only from controller and
  console pods. `deploy/dist-tls/kubernetes/` is a cert-manager recipe that gives each
  controller pod its own certificate naming its IP.
- `.github/SECURITY.md` describes the security model and a checklist for hardening a
  production cluster, and the clustering, console, configuration and plugin pages describe
  ports, firewalls, WireGuard, certificate rotation, cutting a node off and what plugins can
  reach.

### Changed

- **Breaking:** file plugins read and write in a folder per namespace,
  `<root>/<namespace>/` (`<prefix>/<namespace>/` under an S3 root), and a plugin reaches
  only its own namespace's. To upgrade, stop the relays with file plugins, move each
  namespace's files one folder down, and start them again. A plugin enabled with a relative
  path needs nothing more, and a file source keeps its read position across the move, and
  across a disable and enable of the plugin. A plugin restored with an absolute path outside
  its namespace's folder is refused with `plugin.file_path_outside_namespace`, naming where
  the file now belongs ([file plugins](docs/public/plugins/file.md#moving-files-from-an-earlier-release)).
- A runner connecting to a controller whose authorization is on but not yet bootstrapped
  must prove its namespace with a credential or certificate, as after the claim; before, the
  namespace it named was taken as given.
- The [feature maturity](docs/public/publishing/maturity.md) record gains a
  preview label, for a part that stays while its surface may still change;
  nothing carries it yet. The record now also says what the compatibility
  promise means before 1.0: an interface may still change between releases,
  and every such change is named here.
- Each release carries the controller archive for Linux x86_64 and for macOS
  on Apple Silicon and Intel, each built on its own platform and started there,
  with a runner connected, before it is published. Check a download against the
  release's `SHA256SUMS.txt`, and fetch it with `curl` on a Mac, which keeps
  macOS from refusing the unsigned binaries
  ([deploying a controller](docs/public/topics/deploying/deploying_a_controller.md)).
  `make release-archive` packs the same archive from a checkout.
- The longest socket path is the platform's own: 107 bytes on Linux as
  before, 103 on macOS, where a socket directory may therefore be at most 56
  bytes ([files and sockets](docs/public/configuration/files_and_sockets.md)).
  The Rust code compiles for both Apple chips; a build on a Mac is not yet
  tested. Building from source lists the C compiler, cmake and protoc it
  needs, and how to get them on macOS
  ([building from source](docs/public/topics/deploying/building_from_source.md)).
- Relay liveness counters are kept per namespace, so two relays with the same
  id in different namespaces no longer share one row. Each relay row of
  `Controller.liveness_counters` carries its `namespace`, and the read takes
  `filter: [namespace: ns]` for every relay of one namespace; with `limit: 0`
  it answers no rows, only `totals`, each relay count summed over those relays
  on every controller. A reader that keys relay rates by `relay_id` alone
  should key them by namespace and id.
- A finished relay keeps nothing by default: its state directory is deleted
  when it ends, flow and steps alike, and `flz relay info` answers its status
  and error from the relay history. A class that needs its Cells, journal or
  result after the end declares `keep_finished` under `durable` (hours, or
  `true` for `FLEZHA_STEPS_RETENTION_HOURS`); `FLEZHA_KEEP_FINISHED_BY_DEFAULT=true`
  keeps every class as before. A tasklet's spawn repeated by a crash re-run
  after its instance ended is found in the relay history and starts nothing;
  a controller that cannot answer that lookup refuses the spawn as retryable
  (`relay.history_unreachable`). Breaking for a direct service caller:
  `Flezha.Service.Steps.completed_retention_hours` drops its metadata argument
  ([relay settings](docs/public/configuration/relay_settings.md)).
- A factory can be read in full with no instance running:
  `flz relay factory <factory_id>` and `Flezha.Service.Fleet.factory/3` answer
  how it runs, what its class declares (where it keeps its work, how long an
  idle instance waits before it sleeps, its time limit, and a flow's duplicate
  rule, acknowledgement, correction window and keying), its runners and
  controllers, and its graph, which now marks the tasklets that compensate,
  each dynamic tasklet's Cell bounds, and each channel's retries and call
  deadline. The rows of `Fleet.factories` carry `each`, `on_failure`,
  `declarations` and `declarations_disagree`. Runners announce these with
  their factories; a runner built before this reads as "not announced"
  ([Relay commands](docs/public/topics/cli/relay_commands.md#flz-relay)).
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
- A paged `Relay.list` filters by run kind, `filter: [run: "flow" | "steps"]`,
  and `flz relay list` takes `--run`; any other run kind is refused.
- A page of a large listing is read faster: each row's sort key is made once,
  and a relay id of plain printable ASCII is ordered without `inspect/1`, in
  the same order as before. One page of 5,000 relays fell from about 110 ms to
  about 65 ms on a development controller.
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

- The reference Kubernetes manifests boot again: the ConfigMap now puts the relay history
  (`FLEZHA_RELAY_HISTORY_DIR`) on the state volume, where the read-only root filesystem no
  longer stops the controller at its first boot.
- A relay start or schedule whose plugin spec only binds a plugin already enabled, naming no
  provider, no longer fails with `internal.uncatalogued`; its one option, `channel`, is read.
- A graceful stop no longer waits out its whole drain budget for a relay that
  ended on the stopping controller instead of moving, and its `migrated` count
  names only the relays that moved
  ([durability](docs/public/topics/core/durability.md)).
- A controller no longer leaves a namespace while a relay it is moving by cold
  swap has not reached its new controller, which then started the relay empty
  instead of from the state it left with.

- An effectively-once relay no longer counts an input twice when its sender
  never saw the answer and sends it again after the input was processed, as
  happens across a network cut. The input log keeps a record carrying a
  `flz.msg_id` for `FLEZHA_CALL_RETENTION_MARGIN_MS` (5 minutes) after it was
  written, so a repeat within that window is absorbed
  ([durability](docs/public/topics/core/durability.md#effectively-once)). A
  relay fed with ids keeps that much more input on disk.
- A controller whose state disk fills no longer loses input it had already
  acknowledged. An input the disk refuses is refused back to its sender, which
  sends it again. State changes written since the last disk sync are cut from
  the file, and the controller pulls the cell's current state from its runner;
  no repair other than a bind or a correction pushes a saved copy over a live
  cell any more. The input log and the WAL writer keep running, and take input
  again once space frees
  ([durability](docs/public/topics/core/durability.md#what-happens-in-each-situation)).
- With `FLEZHA_CLUSTER_DISCOVERY=k8s_pod_discovery`, a controller whose
  service account has no `token` or `ca.crt` mounted now refuses to start.
  It used to skip checking the API server's certificate when the CA was
  missing, while still sending the service-account token.
- Kubernetes discovery also refuses to start when the API server name,
  selector, node base name, service name, cluster name or namespace is blank,
  or the API server port is outside 1 to 65535. Such a setting used to boot
  a controller that logged a failed query and never found its peers.
  `FLEZHA_K8S_MASTER_NAME` now defaults to `kubernetes.default.svc`, the API
  server's address inside a cluster, so leaving it unset still boots.
- An effectively-once flow relay whose data stream closes, or whose data
  channel is killed, while its runner stays connected no longer loses or
  doubles acknowledged inputs. The data-plane rebind repairs it without
  flagging a stall: each relay with a durable input log holds its input until
  its stream is back, then sends the undelivered input again, the frames the
  broken stream swallowed first. A running relay bound again onto the runner
  it runs on keeps its live state, and a stalled relay is rebound instead of
  restored in place.
- A runner that no bind gave a cell can no longer replace the cell's saved
  history with its own: its writes are refused, and the `flezha_state`
  `wal_realign` event is gone.
- A controller that restarts while its runners keep running no longer restores
  saved state over a relay instance that had moved on, nor keeps one that was
  stale. Each bind gives the instance a seat token, and only an instance still
  holding the token this controller stored after its last restore is kept. The
  `AddRelay` answer's `kept` metadata is replaced by `seat`.
- A replayed slice that closed while its Cell was between rounds is no longer
  left buffered: the Cell delivers it instead of waiting for input that may
  never come. It affected every SDK built on the shared core.
- A run that completed while its relay was being bound to a runner again no
  longer ends as `stopped`: the rebind keeps the stop's cause and outcome.
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

### Security

- The controller image is built on Erlang/OTP 28.5.0.8 and Elixir 1.20.5, which fix
  published advisories in `ssl` (one critical), `public_key`, `erts`, `asn1`, `inets` and
  `stdlib`. The build fetches `rebar3` 3.28.0 by version and checks its SHA-256, and the image
  no longer ships the unused `releases/COOKIE`.
- The Java SDK uses Jackson 2.18.11 and AssertJ 3.27.7, clearing their advisories.
- A `spawner` can no longer enable a plugin by naming its provider in a relay start: the
  start is judged as `plugin.enable` too, and refused with nothing enabled or started.
- A pid-typed plugin option accepts only a pid, never a process name, so no caller can direct
  a plugin's messages at a process of the controller.
- A namespace admin can bind `runner@<ns>` only in `<ns>` (`authz.runner_principal_foreign`).
- The HTTP server plugin refuses the port of the controller's HTTP API and its health port
  (`http.port_reserved`).
- `/healthz` serves at most 64 connections at once and answers one more `503`.
- Generated runner-transport keys are owner-only before a key byte is written.
- The Kubernetes init container no longer opens the certificate and state host paths to
  other users: it sets ownership and closes the top directories (`0700`).
