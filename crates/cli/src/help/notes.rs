//! The hand-written part of each help screen: examples, and what only the CLI
//! knows. Everything above them is rendered from the rows (`help.rs`), so a
//! note never lists a verb, an option or a usage line.

use super::Help;

pub(crate) const AUDIT_READ: Help = r#"With --json the matching records are one JSON array."#;

pub(crate) const AUDIT_TAIL: Help = r#"With --json each record is one JSON line."#;

pub(crate) const AUTH: Help = r#"bootstrap reads the one-time token from its standard input, never from an
argument, which any local user could read:
    flz auth bootstrap < storage/app/auth/bootstrap.token

rebootstrap is break-glass: it recovers a lost admin@_system. Against a running
controller it writes storage/app/auth/bootstrap.token and prints its path;
against a stopped one it writes storage/app/auth/REBOOTSTRAP for the next boot.

Break-glass needs write access to storage/app/auth/ and nothing else, which is
already enough to stop the controller: a claim against a running controller
therefore grants no authority filesystem access does not already carry. It
unbinds every admin@_system and revokes its _system admin tokens before binding
the new admin, and it is audited as a recovery."#;

pub(crate) const BOOT: Help = r#"`await` also waits, inside the same timeout, for a controller it cannot reach
yet, such as one started as a daemon a moment ago. Once reached, the
controller's verdict is the answer. Still unreachable at the timeout, it exits 3
(controller.unreachable) naming what that can mean: not up yet, stopped after it
was reached, or listening in another socket directory."#;

pub(crate) const CLUSTER: Help = r#"EXAMPLES:
    flz cluster status
    flz cluster status --namespace prod --output json"#;

pub(crate) const CONTROLLER_MEM_TOP: Help = r#"OUTPUT (default text format):
    Top-20 processes by :erlang.process_info(self, :memory),
    sorted descending. Each row carries name / registered name /
    initial call / :message_queue_len / :heap_size / :total_heap_size.

    ETS tables sorted by :ets.info(tab, :memory) (bytes).
    DETS files under storage/app/process_hub/ sorted by file size.
    :recon_alloc.memory(:allocated_types) allocator breakdown."#;

pub(crate) const CONTROLLER_LIST: Help = r#"OUTPUT COLUMNS:
    NODE       The controller's node; `(this)` marks the one that answered
    HOST       The host half of its node name
    MACHINE    The machine it runs on (FLEZHA_HOST_NODE), `-` when unset
    VERSION    Its release version
    UP         How long it has been up
    RUNNERS    How many runners are connected to it
    A controller that does not answer in time shows `unreachable`, and a
    closing line names it."#;

pub(crate) const CONTROLLER_START: Help = r#"EXAMPLES:
    flz controller start
    flz controller start --mode daemon
    flz controller start -m attached_iex"#;

pub(crate) const CONTROLLER_STATUS: Help = r#"OUTPUT FIELDS:
    shutdown_in_progress       True while quiesce/drain/finalise is running
    shutdown_phase             :quiesce | :drain | :finalise | nil
    signal_handler_status      :ok | :degraded (degraded if :os.set_signal/2 unavailable)
    graceful_shutdown_enabled  Configured kill-switch state
    graceful_shutdown_timeout_ms  Configured drain timeout
    durability_backend         none | postgres | s3 | local_path
    durability_posture         local_only | remote_tier
    durability_rpo_breaches    References past the remote RPO target
    durability_levels          Each running class's durable level: memory | local | remote
    durability_superseded      References another controller now ships
    state_root                 Absolute directory holding this node's durable state
    file_plugin_root           Absolute folder file plugins may read and write"#;

pub(crate) const CONTROLLER_STOP: Help = r#"DESCRIPTION:
    Drives Flezha.Shutdown.Coordinator over the running controller's
    CLI socket (works for release, mix, and iex run modes). Streams
    drain progress to stderr and prints a final outcome line on stdout.

EXIT CODES:
    0   Clean shutdown (or `--force` finalised cleanly); a stop issued while one
        is in flight follows it and exits with its outcome
    3   Controller not reachable (controller.unreachable)
    4   Refused for want of a token or a role (authz.*)
    7   Partial drain: the timeout elapsed with Relays still local
        (shutdown.partial_drain)"#;

pub(crate) const FLZ: Help = r#"OUTPUT FORMAT:
    Under --json a verb that reaches a controller answers as JSON whether it
    reads or acts: the JSON its operation answers on every surface. Any other
    format is refused. `flz help --json` prints every such operation, as data,
    with no controller running.

ERRORS:
    A failure is written on stderr as `error: <message> (<code>)`, with a
    `hint:` line when there is a next command, or under --json as one
    {"error": {"code", "class", "message", "details", "hint"}} line. The exit
    code follows the class: 1 internal, 2 invalid, 3 unreachable, 4 denied,
    5 not_found, 6 conflict, 7 unavailable. Every code is in docs/public/errors.md.

EXAMPLES:
    flz controller start
    flz relay start my_factory --id my_relay --namespace prod
    flz plugin install /path/to/my_plugin-1.0.0.fpz
    flz plugin list --installed
    flz plugin enable kafka_producer provider1 Xhosts=localhost:9092
    flz namespace list --output json
    flz boot status
    flz boot await --timeout 60"#;

pub(crate) const MCP: Help = r#"An AI assistant starts this command and speaks MCP with it; it is not run by
hand. Five tools need no controller: flezha_guide, flezha_sdk_example,
flezha_error, flezha_docs and flezha_new_project. Once a controller answers,
the operations an HTTP caller may reach are tools too, called as the token's
principal (--token or FLEZHA_TOKEN). Stopping the controller, issuing tokens
and resetting are never offered.

EXAMPLES:
    claude mcp add flezha -- /path/to/release/bin/flz mcp
    claude mcp add flezha -e FLEZHA_TOKEN=<token> -- /path/to/release/bin/flz mcp"#;

pub(crate) const NAMESPACE: Help = r#"create and set-quota take each quota as --X<quota>=<value>.

Quota fields: max_relays, max_cells_per_relay, max_mailbox_bytes_total,
              max_message_rate_per_second, max_state_bytes, max_runners"#;

pub(crate) const OBSERVE_SINKS: Help =
    r#"Prints one row per registered sink (name, module, overflow, watermark)."#;

pub(crate) const OBSERVE_SOURCES: Help =
    r#"Prints one row per registered source (name, status, kinds, cardinality budget)."#;

pub(crate) const OBSERVE_TAIL: Help = r#"Signals arrive through the PubSub sink, which needs :pubsub_sink_enabled to be
true in :observability config. With --json, one JSON object per signal per line."#;

pub(crate) const RELAY_START: Help = r#"RUN:
    The factory's own: its class declares how it runs (flow or steps) and
    where its work is kept, and its runner announces it, so a start names only
    the factory.

ID:
    Without --id the controller generates one, <factory>-<16 characters>, and
    prints it: Started echo-k7m2qzxw4pd3fn2a. With --output json the answer is
    {"id": ..., "outcome": "started" | "already_running"}.

    A start without --id that is run again after its answer was lost starts a
    second relay, and a flow one runs until it is stopped. Pass --id
    when the start may be retried: a start of an id that is already running
    prints "<id> is already running, left untouched" and starts nothing.

INIT VALUES:
    A flow relay's document is keyed by Tasklet name, one entry per
    Tasklet to seed. Name only what you want seeded; a Tasklet you leave
    out keeps the state its class builds.

        --init-values '{"compare_filter_tasklet": {"comparable": "hi"}}'
        --init-values '{"ohlc": {"1m": {"high": 0}}}'   one state per declared key

    A name the class does not carry is ignored, not refused: the runner logs
    it and the start still succeeds, so check the spelling. A steps
    relay's tasklets take the document as state the same way.

FIRST MESSAGE:
    --text is the instance's first input, whichever way it runs; a steps one
    started without it waits for `flz relay send`. --channel needs --text.

EXAMPLES:
    flz relay start do_nothing
    flz relay start do_nothing --id my_relay
    flz relay start candles --id ohlc --partitions 3
    flz relay start order_aggregator -i orders --namespace prod -o json
    flz relay start order_executor --text '{"symbol":"EUR/USD","quantity":10}' --channel order"#;

pub(crate) const RELAY_LISTEN: Help = r#"OUTPUT:
    One line per frame: the channel, a tab and the payload. A frame from a
    keyed output adds a third field, cell=... seq=... key=..., and a frame
    with no channel prints - in its place. A part, a preview a tasklet sent
    while it ran, begins its third field part=<n> attempt=<a>; a dead-lettered
    input prints the channel, the error's message and error=<code>
    entry=<entry>. With --json each frame is one object carrying kind
    (output, input, part or dead_lettered), channel, payload (payload_base64
    for bytes that are not text, error for a dead-lettered input) and the
    frame's whole metadata map.

    With --calls a call prints its entry channel, its payload and call=<id>
    source=<source> (cli, ui, sdk, http, mcp, internal, plugin or relay, with
    caller=<relay> for another relay's call); its answer prints the answer's
    channel, the payload and call=<id> answered ms=<n>; a timeout or a refusal
    prints the entry channel, the error's message and call=<id> error=<code>
    ms=<n>. With --json the kinds are call, answered, timed_out and refused,
    and each object carries the call's id as call.

    With --tally one line arrives about once a second, a field per output
    channel: <channel>=<total> and the frames per second since the line before,
    or - on the first line and after the relay moved and its counts restarted.
    (other), shown once it is not zero, counts frames on a channel the relay
    does not declare as an output.
    The tally binds ahead of the relay's other output plugins, so a frame one
    of them skips is still counted; parts are not. With --json each line is
    one object of kind tally carrying totals, rates, read_at_ms and epochs.

    The listener is detached when the window closes, on Ctrl-C, when the
    caller goes away and when the relay ends. A partition group is
    listened to by its group id.

EXAMPLES:
    flz relay listen my_echo --duration-ms 10000
    flz relay listen my_echo --input
    flz relay listen pricing --calls
    flz relay listen pricing --tally
    flz relay listen ohlc --json"#;

pub(crate) const RELAY_CALL: Help = r#"OUTPUT:
    The answer and a newline: the first output the input caused, or its first
    on the answer channel when one is named, as the relay's API reads it. A
    channel that does not answer prints `accepted`. The output also reaches
    the relay's plugins and listeners. A failure is printed on stderr with its
    code and exits by its class: call.timeout 7, a denied call 4,
    relay.not_found 5, api.input_invalid 2.

EXAMPLES:
    flz relay call echo --text hello
    flz relay call orders-eu --channel place --arg pair=EUR/USD --arg amount=1000
    flz relay call prices --key EURUSD --text '{"pair":"EURUSD"}'
    flz relay call quoter --text EUR/USD --answer-channel quotes
    flz relay call slow --text x --timeout 100"#;

pub(crate) const RELAY: Help = r#"EXAMPLES:
    flz relay scale ohlc 5
    flz relay send orchestrator --text '{"task":"refactor"}'
    flz relay send my_echo --text 'hello world' --await 2000
    flz relay send prices --key EURUSD --text x
    flz relay send orders --text x --msg-id orders/0/41
    flz relay diagnose orders-eu
    flz relay graph orders-eu
    flz relay activity orders-eu
    flz relay activity orders-eu --cells book --limit 10
    flz relay rewind chat-1 --to-iteration 3
    flz relay replay candles/aggregate/BTC --from 1720000000000"#;

pub(crate) const PLUGIN: Help = r#"PLUGINS:
    A plugin has a name of its own in its namespace and is enabled once, with
    no relay; nothing runs until a relay is bound to it. A plugin runs
    on each controller where a relay bound to it runs, and one copy there
    serves every such relay when its provider can be shared. `--single`
    runs one copy in the whole cluster instead. A plugin whose last relay
    leaves stops and stays enabled; only `disable` turns it off. A provider
    that cannot be shared serves one relay, or one partition group.

    `enable --relay_id <op>` enables and binds in one command. `update`
    changes a running plugin in place when its provider declares the changed
    settings live, and restarts it otherwise, naming the relays bound to it.

PACKAGES:
    install, uninstall and sync change the plugin root, never a running
    controller, and work with no controller running. A change takes effect
    when the controller restarts; `flz plugin list --installed` shows each
    package as loaded, refused, pending or removed. An upgrade is an install
    of a newer package of the same id. While FLEZHA_PLUGINS is set, install
    and uninstall are refused: the declared list is the node's only truth. A
    relay bound to a plugin whose package a controller lacks runs there
    without it, and its health says so.

HOOKS:
    A hook is a point at which the relay calls its plugin, named by the
    callback the provider defines: on_input sends every payload the relay
    ingests, on_output every payload it emits, on_interval runs on a timer.
    A hook belongs to a binding.

    `--hook=<name>` names one and may be repeated. A hook's sub-options follow
    it as `--<name>.<key>=<value>`. on_interval has no default interval, so
    it needs `--on_interval.interval_ms=<ms>`.

    A binding naming no hook has no hooks. A plugin that only receives still
    feeds its relay: its connection is held by the provider's own worker,
    which no hook governs.

    enable reads --namespace, --single, --relay_id, --hook, --channel and
    the X-prefixed provider settings; bind reads --namespace, --relay_id,
    --hook and --channel; unbind --namespace and --relay_id; update
    --namespace and the X-prefixed settings; disable --namespace. Each
    refuses any other option.

EXAMPLES:
    flz plugin install ./flezha_example_plugin-0.1.0.fpz
    flz plugin list --installed
    flz plugin enable trades relay_websocket \
        --Xuri=ws://localhost:9000/feed --Xchannel=trades
    flz plugin bind trades --relay_id my_op
    flz plugin bind trades --relay_id other_op --channel ticks
    flz plugin enable stamp example_stamp --relay_id my_op \
        --Xpath=/tmp/stamped.txt --hook=on_output
    flz plugin update trades --Xsubscription_items='["BTC/EUR","ETH/EUR"]'
    flz plugin list --namespace default
    flz plugin unbind trades --relay_id other_op
    flz plugin disable trades"#;

pub(crate) const RESET: Help = r#"DESCRIPTION:
    By default clears:
        storage/app/state/        (per-Cell WAL/snapshot/outbox)
        storage/app/process_hub/  (ProcessHub durable registry)

    Clearing storage/app/auth/ removes every token (runner credentials
    included), every role binding and every namespace but _system. With
    authorization on and no remote durability backend, the next start
    writes a fresh bootstrap token. Until it is claimed, administration verbs
    are refused and every runner is admitted with no credential, into
    any namespace but _system it names: claim it at once.

    Never cleared, and reported as kept:
        storage/logs/

    Every store is <storage root>/app/<name>: a store moved by its own
    variable (FLEZHA_DLQ_DIR, FLEZHA_AUTHZ_DIR, ...) is neither found nor
    cleared. storage/logs/ is the log file's default directory, in the
    working directory, and --storage-root does not move it.

    The socket directory is resolved through the same ladder the Controller
    and its runners walk -- $FLEZHA_SOCKET_FILES_PATH, else
    $XDG_RUNTIME_DIR/flezha, else /run/flezha, else storage/app/sockets -- so it
    is frequently not under storage/app/ at all. Only the socket files Flezha
    itself creates are unlinked there, and a directory a Controller is still
    serving in is kept.

EXIT CODES:
    0   Success (apply succeeded, or --dry-run preview)
    1   A filesystem error: before any action (reset.path_unreadable), or after
        some roots were cleared (reset.partial)
    2   Invalid usage: missing --yes without --dry-run
        (reset.confirmation_required), conflicting flags, or a path it refuses
        to clear, such as the storage app directory itself (reset.path_refused)
    6   Refused: the Controller is up (reset.controller_running)

EXAMPLES:
    flz reset                          # plan only, exit 2
    flz reset --dry-run                # plan only, exit 0
    flz reset --yes                    # default scope (state, process_hub)
    flz reset --yes --all              # every opt-in clear too, auth included
    flz reset --yes --json             # machine-readable output"#;

pub(crate) const ROLE: Help = r#"A principal holds a set of roles in one namespace, and an action is allowed
when any of them grants it. A bind naming another namespace is refused."#;

pub(crate) const SCHEDULE: Help = r#"add refuses a factory a connected runner announced as flow. run fires one
instance at once with the stored config, judged as relay.start in the
schedule's namespace.

EXAMPLES:
    flz schedule add digest --cron "0 8 * * *" --tz Europe/Tallinn --factory ai_digest
    flz schedule add nightly --cron "0 2 * * *" --tz UTC --factory report --text '{"day":"today"}' --channel requests
    flz schedule list
    flz schedule run digest"#;

pub(crate) const STATE: Help = r#"EXAMPLES:
    flz state get MarketRelay/OhlcTasklet/M1
    flz state get MarketRelay/OhlcTasklet --output json
    flz state list MarketRelay
    flz state at MarketRelay/OhlcTasklet/M1 12345
    flz state at MarketRelay/OhlcTasklet/M1 2026-05-01T12:00:00Z
    flz state rotate MarketRelay/OhlcTasklet/M1
    flz state retention MarketRelay 6
    flz state delete MarketRelay/OhlcTasklet/M1
    flz state outbox MarketRelay/OhlcTasklet/M1
    flz state journal etl-job-42
    flz state inbox chat-session-7"#;

pub(crate) const TOKEN: Help = r#"A `runner` token is the credential a runner connection proves its namespace
with. It never expires and `--ttl` is refused for it: the holder is an
unattended process, and a credential that lapses fails it at a restart. A
namespace may hold several at once, so a rotation is issue, roll the fleet,
revoke the old one; revoking one closes the sessions it authenticated."#;
