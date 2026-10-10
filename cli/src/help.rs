//! Every help screen `flz` prints, as data rather than as 27 functions.
//!
//! The text is hand-aligned and not uniform across screens — `flz schedule
//! help` sets two description columns inside one block — so no renderer
//! computes a column here: a screen is its own bytes, and the only mechanism
//! is the table that names them. `cli/tests/golden/` holds the same screens
//! captured from the binary, which is what proves an edit here is deliberate.

/// One help screen, without its trailing newline.
pub(crate) type Help = &'static str;

pub(crate) fn render(help: Help) {
    println!("{}", help);
}

/// The catalogue of every operation a controller publishes, as
/// `mix flezha.catalogue` wrote it: what `flz help --json` prints, and what
/// every door call `flz` sends is held to.
pub(crate) const CATALOGUE: &str = include_str!("../catalogue.json");

/// `flz help`: the screen, or under `--json` the catalogue.
pub(crate) fn handle_help(read: &crate::args::Reading) {
    if read.output_format() == "json" {
        print!("{}", CATALOGUE);
    } else {
        render(FLZ);
    }
}

pub(crate) const AUDIT_LIST: Help = r#"flz audit list [--json]"#;

pub(crate) const AUDIT_READ: Help = r#"flz audit read <FILE> [OPTIONS]

OPTIONS:
    --json              Emit the matching records as one JSON array
    --source <S>        Match records whose source equals S
    --name <N>          Match records whose name equals N
    --since <TS_US>     Match records with ts_us >= TS_US (monotonic)
    --until <TS_US>     Match records with ts_us <= TS_US
    --limit <N>         Return at most N records (default 1000)"#;

pub(crate) const AUDIT_ROTATE: Help = r#"flz audit rotate [--json]"#;

pub(crate) const AUDIT_STATS: Help = r#"flz audit stats [--json]"#;

pub(crate) const AUDIT_TAIL: Help = r#"flz audit tail [--json] [--duration-ms N]"#;

pub(crate) const AUDIT: Help = r#"flz audit - Durable audit log inspection

USAGE:
    flz audit <COMMAND> [OPTIONS]

COMMANDS:
    list      List the on-disk catalog of rotated files
    read      Read records from a specific file with optional filters
    tail      Stream live audit records until a duration elapses
    stats     Show operational snapshot (file path, size, totals, drops)
    rotate    Force an immediate rotation of the active file
    help      Show this help message

OPTIONS:
    --json    Emit machine-readable output (one JSON line per record for
              tail). --output json is the same request."#;

pub(crate) const AUTH: Help = r#"flz auth - Authentication bootstrap and identity

USAGE:
    flz auth <COMMAND>

COMMANDS:
    bootstrap <token>   Claim the one-time bootstrap token as admin@_system
    whoami              Show the identity the current token resolves to
    rebootstrap         Break-glass: recover a lost admin@_system
                        (running: writes app_data/auth/bootstrap.token and prints it;
                         stopped: writes app_data/auth/REBOOTSTRAP for the next boot)
    help                Show this help message

Break-glass needs write access to app_data/auth/ and nothing else, which is
already enough to stop the controller: a claim against a running controller
therefore grants no authority filesystem access does not already carry. It
unbinds every admin@_system and revokes its _system admin tokens before binding
the new admin, and it is audited as a recovery.

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var."#;

pub(crate) const BOOT: Help = r#"flz boot - Boot/recovery commands

USAGE:
    flz boot <COMMAND>

COMMANDS:
    status                    Print the readiness verdict, its preconditions and per-hub detail
    await [--timeout SECS]    Block until the readiness verdict is ready (0 evaluates once)
    help                      Show this help message

`await` also waits, inside the same timeout, for a controller it cannot reach
yet, such as one started as a daemon a moment ago. Once reached, the
controller's verdict is the answer. Still unreachable at the timeout, it exits 3
(controller.unreachable) naming what that can mean: not up yet, stopped after it
was reached, or a cookie or node name that is not the controller's. A controller
that refuses the cookie logs two errors per attempt meanwhile, about four a
second."#;

pub(crate) const CLUSTER: Help = r#"flz cluster - Controller cluster status

USAGE:
    flz cluster <SUBCOMMAND>

SUBCOMMANDS:
    status              Show nodes, leader, per-node relay counts, relay health, recovery state
    help                Show this help message

OPTIONS:
    -n, --namespace <NS>   Namespace (default: "default")
    -o, --output <FMT>     Output format: text (default) or json
        --json             The switch spelling of --output json

EXAMPLES:
    flz cluster status
    flz cluster status --namespace prod --output json"#;

pub(crate) const CONTROLLER_MEM_TOP: Help = r#"flz controller mem-top - Snapshot Controller memory hot spots

USAGE:
    flz controller mem-top [--json]

OUTPUT (default text format):
    Top-20 processes by :erlang.process_info(self, :memory),
    sorted descending. Each row carries name / registered name /
    initial call / :message_queue_len / :heap_size / :total_heap_size.

    ETS tables sorted by :ets.info(tab, :memory) (bytes).
    DETS files under app_data/process_hub/ sorted by file size.
    :recon_alloc.memory(:allocated_types) allocator breakdown.

FLAGS:
    --json    Emit a single JSON document for machine consumption.
              --output json is the same request."#;

pub(crate) const CONTROLLER_START: Help = r#"flz controller start - Start the flezha controller

USAGE:
    flz controller start [OPTIONS]

OPTIONS:
    --mode <MODE>     Controller start mode
    -m <MODE>         Short form of --mode

AVAILABLE MODES:
    attached          Start in attached mode (default)
    daemon            Start as daemon
    attached_iex      Start in attached IEx mode
    daemon_iex        Start as daemon with IEx

EXAMPLES:
    flz controller start
    flz controller start --mode daemon
    flz controller start -m attached_iex"#;

pub(crate) const CONTROLLER_STATUS: Help = r#"flz controller status - Print controller status

USAGE:
    flz controller status [--output text|json] [--json]

OUTPUT FIELDS:
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

pub(crate) const CONTROLLER_STOP: Help = r#"flz controller stop - Stop the flezha controller gracefully

USAGE:
    flz controller stop [OPTIONS]

DESCRIPTION:
    Drives Flezha.Shutdown.Coordinator over the running controller's
    CLI socket (works for release, mix, and iex run modes). Streams
    drain progress to stderr and prints a final outcome line on stdout.

OPTIONS:
    --timeout-ms <N>  Override the configured drain timeout (ms)
    --no-wait         Issue the shutdown and exit without waiting
    --force           Skip drain — finalise (flush WAL + close transports) only

EXIT CODES:
    0   Clean shutdown (or `--force` finalised cleanly); a stop issued while one
        is in flight follows it and exits with its outcome
    3   Controller not reachable (controller.unreachable)
    4   Refused for want of a token or a role (authz.*)
    7   Partial drain: the timeout elapsed with Relays still local
        (shutdown.partial_drain)
    Every code is listed in docs/errors.md."#;

pub(crate) const CONTROLLER: Help = r#"flz controller - Controller management commands

USAGE:
    flz controller <COMMAND>

COMMANDS:
    start    Start the flezha controller
    stop     Stop the flezha controller (graceful)
    status   Print controller status (incl. graceful-shutdown state)
    mem-top  Snapshot top-memory processes / ETS / DETS / allocators
    help     Show this help message

Use 'flz controller <COMMAND> --help' for more information on a specific command."#;

pub(crate) const DLQ: Help = r#"Dead-letter queue: parked poison payloads (retry budget exhausted).

Usage:
  flz dlq list [relay]                        Parked entries, nearest-to-expiry first
  flz dlq show <relay/tasklet/entry-id>       Entry metadata + payload preview
  flz dlq replay <relay/tasklet/entry-id>     Re-dispatch through the normal input path
  flz dlq drop <relay/tasklet/entry-id>       Discard the entry (audited)

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var."#;

pub(crate) const FLZ: Help = r#"flz - Flezha CLI

USAGE:
    flz <COMMAND>

COMMANDS:
    controller     Controller management commands
    cluster        Controller cluster status (nodes, leader, relay health)
    relay       Relay management commands
    namespace      Namespace management commands
    runner         Runner management commands
    plugin         Plugin management commands
    boot           Boot/recovery status, await, and legacy snapshot import
    state          Per-Cell state queries (time-travel, retention, list)
    reset          Clear Controller durable runtime data on disk (controller must be stopped)
    mcp            Serve Flezha to an AI assistant over MCP, on stdin and stdout
    template       Start a project from a template the release carries (list, create)
    observe        Observability foundation inspection (sources, sinks, tail)
    audit          Durable audit log inspection (list, read, tail, stats, rotate)
    dlq            Dead-letter queue: inspect, replay, or drop parked payloads
    schedule       Cron-scheduled steps relay spawns (add, list, remove, run)
    auth           Bootstrap claim, identity (whoami), offline break-glass recovery
    token          Scoped-token management (issue, revoke, list)
    role           Principal → role bindings (bind, unbind, list)
    help           Show this help message

OUTPUT FORMAT:
    Every verb takes --json, or the same request spelled --output json / -o json.
    A verb that reaches a controller answers it as JSON whether it reads or
    acts: the JSON its operation answers on every surface. Any other format is
    refused. `flz help --json` prints every such operation, as data, with no
    controller running.

ERRORS:
    A failure is written on stderr as `error: <message> (<code>)`, with a
    `hint:` line when there is a next command, or under --json as one
    {"error": {"code", "class", "message", "details", "hint"}} line. The exit
    code follows the class: 1 internal, 2 invalid, 3 unreachable, 4 denied,
    5 not_found, 6 conflict, 7 unavailable. Every code is in docs/errors.md.

Use 'flz <COMMAND> help' for more information on a specific command.

EXAMPLES:
    flz controller start
    flz relay start my_factory --id my_relay --namespace prod
    flz plugin install /path/to/my_plugin-1.0.0.fpz
    flz plugin list --installed
    flz plugin enable kafka_producer provider1 Xhosts=localhost:9092
    flz namespace list --output json
    flz boot status
    flz boot await --timeout 60"#;

pub(crate) const MCP: Help = r#"flz mcp - Serve Flezha to an AI assistant over MCP, on stdin and stdout

USAGE:
    flz mcp [--token <token>]

An AI assistant starts this command and speaks MCP with it; it is not run by
hand. Five tools need no controller: flezha_guide, flezha_sdk_example,
flezha_error, flezha_docs and flezha_new_project. Once a controller answers,
the operations an HTTP caller may reach are tools too, called as the token's
principal (--token or FLEZHA_TOKEN). Stopping the controller, issuing tokens
and resetting are never offered.

EXAMPLES:
    claude mcp add flezha -- /path/to/release/bin/flz mcp
    claude mcp add flezha -e FLEZHA_TOKEN=<token> -- /path/to/release/bin/flz mcp"#;

pub(crate) const NAMESPACE: Help = r#"flz namespace - Namespace management commands

USAGE:
    flz namespace <COMMAND>

COMMANDS:
    list                                 List the auth catalog, with a running-hub column
    create <name> [--X<quota>=<value>]   Create a namespace in the auth catalog
    delete <name>                        Delete a namespace from the auth catalog
    show <name>                          Show a namespace's quotas
    set-quota <name> --X<quota>=<value>  Update quota overrides
    help                                 Show this help message

Quota fields: max_relays, max_cells_per_relay, max_mailbox_bytes_total,
              max_message_rate_per_second, max_state_bytes, max_runners

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var."#;

pub(crate) const OBSERVE_DISABLE: Help = r#"flz observe disable <SOURCE>"#;

pub(crate) const OBSERVE_ENABLE: Help = r#"flz observe enable <SOURCE>"#;

pub(crate) const OBSERVE_SINKS: Help = r#"flz observe sinks [--output text|json] [--json]

Print one row per registered sink (name, module, overflow, watermark)."#;

pub(crate) const OBSERVE_SOURCES: Help = r#"flz observe sources [--output text|json] [--json]

Print one row per registered source (name, status, kinds, cardinality budget)."#;

pub(crate) const OBSERVE_TAIL: Help = r#"flz observe tail <SOURCE> [--json] [--duration-ms N]

Stream live signals from SOURCE via the PubSub sink. Requires
:pubsub_sink_enabled to be true in :observability config.

OPTIONS:
    --json              Emit NDJSON (one JSON object per signal per line);
                        --output json is the same request
    --duration-ms <N>   Stream for at most N milliseconds (default 30000)"#;

pub(crate) const OBSERVE: Help = r#"flz observe - Observability foundation inspection

USAGE:
    flz observe <COMMAND> [OPTIONS]

COMMANDS:
    sources    List registered sources and their status / cardinality budget
    sinks      List registered sinks and their overflow policy / watermark
    tail       Stream live signals from a source via the PubSub sink
    enable     Enable a source (transitions its registry status to :active)
    disable    Disable a source (transitions to :disabled — Bus drops emits)
    help       Show this help message"#;

pub(crate) const RELAY_START: Help = r#"flz relay start - Start a relay

USAGE:
    flz relay start <factory_id> [OPTIONS]

OPTIONS:
    --id <id>, -i <id>          Id to start under (default: generated)
    --namespace <ns>            Namespace to start in (default: default)
    --init-values <json>        Seed state at start
    --text <payload>, -t        First message, delivered as `send` does
    --channel <ch>, -c          Entry channel the first message names
    --plugins <json-array>      Plugins to bind at start, enabling each named
                                with a provider when it is not enabled yet
    --partitions <N>            Start N partition members as one group
    --output <text|json>        Output format (default: text)
    --json                      The same as --output json

RUN:
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
    --text is the instance's first input, on the entry channel --channel
    names or the first declared one, whichever way it runs; a steps one started
    without it waits for `flz relay send`. --channel needs --text.

EXAMPLES:
    flz relay start do_nothing
    flz relay start do_nothing --id my_relay
    flz relay start candles --id ohlc --partitions 3
    flz relay start order_aggregator -i orders --namespace prod -o json
    flz relay start order_executor --text '{"symbol":"EUR/USD","quantity":10}' --channel order"#;

pub(crate) const RELAY_LISTEN: Help = r#"flz relay listen - Print the frames a relay emits or ingests

USAGE:
    flz relay listen <relay_id> [OPTIONS]

OPTIONS:
    --input                   Listen to what the relay ingests, not what it emits
    --duration-ms <N>         Stop after N milliseconds (default 30000)
    --output <text|json>      Output format (default: text)
    --json                    The same as --output json
    --namespace <ns>          Namespace the relay runs in (default: default)

OUTPUT:
    One line per frame: the channel, a tab and the payload. A frame from a
    keyed output adds a third field, cell=... seq=... key=..., and a frame
    with no channel prints - in its place. A part, a preview a tasklet sent
    while it ran, begins its third field part=<n> attempt=<a>; a dead-lettered
    input prints the channel, the error's message and error=<code>
    entry=<entry>. With --json each frame is one object carrying kind
    (output, input, part or dead_lettered), channel, payload (payload_base64
    for bytes that are not text, error for a dead-lettered input) and the
    frame's whole metadata map.

    The listener is detached when the window closes, on Ctrl-C, when the
    caller goes away and when the relay ends. A partition group is
    listened to by its group id.

EXAMPLES:
    flz relay listen my_echo --duration-ms 10000
    flz relay listen my_echo --input
    flz relay listen ohlc --json"#;

pub(crate) const RELAY_CALL: Help = r#"flz relay call - Send one input and print the relay's answer

USAGE:
    flz relay call <relay_id> (--text <payload> | --arg <name=value>...) [OPTIONS]

OPTIONS:
    -t, --text <payload>      The input's payload, sent as written
    --arg <name=value>        One field of a JSON input, typed by the channel's schema;
                              repeat it for each field
    -c, --channel <ch>        The entry channel to call (default: the first)
    --answer-channel <ch>     The channel whose output answers (default: the first output)
    --key <key>               The key whose member of a partition group takes the input
    --timeout <ms>            How long the relay has to answer (default 30000)
    --namespace <ns>          Namespace the relay runs in (default: default)

OUTPUT:
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

pub(crate) const RELAY: Help = r#"flz relay - Relay management commands

USAGE:
    flz relay <COMMAND>

COMMANDS:
    start       Start a relay
    scale       Rescale a partition group to N partitions
    stop        Stop a relay
    state       Get relay state
    list        List relays
    info        Get relay info, or one relay's API
    diagnose    Say what is wrong with a relay and what to do
    listen      Print the frames a relay emits or ingests
    send        Send a payload to a relay's input stream
    call        Send one input and print the relay's answer
    rewind      Rewind a steps loop to a committed iteration
    replay      Re-run a Cell's retained inputs from a point in the past
    help        Show this help message

EXAMPLES:
    flz relay start candles --id ohlc --partitions 3
    flz relay scale ohlc 5
    flz relay send orchestrator --text '{"task":"refactor"}'
    flz relay send my_echo --text 'hello world' --await 2000
    flz relay send prices --key EURUSD --text x
    flz relay send orders --text x --msg-id orders/0/41
    flz relay call echo --text hello
    flz relay diagnose orders-eu
    flz relay listen my_echo --duration-ms 10000
    flz relay rewind chat-1 --to-iteration 3
    flz relay replay candles/aggregate/BTC --from 1720000000000

Use 'flz relay <COMMAND> --help' for more information on a specific command.

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var."#;

pub(crate) const PLUGIN: Help = r#"flz plugin - Plugin management commands

USAGE:
    flz plugin <COMMAND>
    flz plugin enable <name> <provider_id> [--single] [X<key>=<value>]... [OPTIONS]
    flz plugin bind <name> --relay_id <op> [--hook=<name>]... [--channel <c>]

COMMANDS:
    install    Place a plugin package (.fpz) from a path or an https:// URL
    uninstall  Remove an installed plugin package
    sync       Make the plugin root hold exactly the packages FLEZHA_PLUGINS
               declares
    enable     Enable a plugin by name in a namespace
    bind       Connect a relay to an enabled plugin
    unbind     Take a relay's connection to a plugin away
    update     Change an enabled plugin's settings
    disable    Turn a plugin off
    list       List the plugins enabled in a namespace, the `--installed`
               packages or the `--providers`
    help       Show help for a specific plugin provider

PLUGINS:
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
    `flz plugin help <provider_id>` lists the hooks a provider has. A hook
    belongs to a binding.

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

pub(crate) const RESET: Help = r#"flz reset - Clear Controller durable runtime data on disk

USAGE:
    flz reset [OPTIONS]

DESCRIPTION:
    Clears the contents of the Controller's durable on-disk state so the
    next boot starts from a clean slate. Clears nothing while the
    Controller is up; stop it first with `flz controller stop`.

    By default clears:
        app_data/state/         (per-Cell WAL/snapshot/outbox)
        app_data/process_hub/   (ProcessHub durable registry)

    Opt-in clears (off by default):
        app_data/certs/         (--include-certs)
        app_data/dlq/           (--include-dlq)
        app_data/schedules/     (--include-schedules)
        app_data/auth/          (--include-auth)
        app_data/audit/         (--include-audit)
        app_data/plugins/       (--include-plugins)
        <socket directory>      (--include-sockets)

    Clearing app_data/auth/ removes every token (runner credentials
    included), every role binding and every namespace but _system. With
    authorization on and no remote durability backend, the next start
    writes a fresh bootstrap token. Until it is claimed, administration verbs
    are refused and every runner is admitted with no credential, into
    any namespace but _system it names: claim it at once.

    Never cleared, and reported as kept:
        logs/

    Every app_data/ root is <data root>/<name>: a store moved by its own
    variable (FLEZHA_DLQ_DIR, FLEZHA_AUTHZ_DIR, ...) is neither found nor
    cleared. logs/ is the log file's default directory, in the working
    directory, and --app-data-root does not move it.

    The socket directory is resolved through the same ladder the Controller
    and its runners walk -- $FLEZHA_SOCKET_FILES_PATH, else
    $XDG_RUNTIME_DIR/flezha, else /run/flezha, else app_data/sockets -- so it
    is frequently not under app_data/ at all. Only the socket files Flezha
    itself creates are unlinked there, and a directory a Controller is still
    serving in is kept.

OPTIONS:
    --yes                     Apply the reset. REQUIRED for any destructive action.
    --dry-run                 Print the plan without touching the filesystem (exit 0).
    --include-certs           Also clear app_data/certs/
    --include-dlq             Also clear app_data/dlq/ (every parked dead-letter record)
    --include-schedules       Also clear app_data/schedules/ (every schedule)
    --include-auth            Also clear app_data/auth/ (every token and role binding)
    --include-audit           Also clear app_data/audit/ (the audit trail)
    --include-plugins         Also clear app_data/plugins/ (every installed plugin)
    --include-sockets         Also clear Flezha's sockets in the resolved socket directory
    --all                     Every opt-in clear above, auth and audit included
    --app-data-root <PATH>    Override the `app_data/` root (else FLEZHA_APP_DATA_ROOT or ./app_data)
    --probe-timeout-ms <N>    Live-node probe timeout (default 1000)
    --json                    Machine-readable output (--output json is the same request)
    --help, -h, help          Show this help message

EXIT CODES:
    0   Success (apply succeeded, or --dry-run preview)
    1   A filesystem error: before any action (reset.path_unreadable), or after
        some roots were cleared (reset.partial)
    2   Invalid usage: missing --yes without --dry-run
        (reset.confirmation_required), conflicting flags, or a path it refuses
        to clear, such as the app_data root itself (reset.path_refused)
    6   Refused: the Controller is up (reset.controller_running)
    Every code is listed in docs/errors.md.

EXAMPLES:
    flz reset                          # plan only, exit 2
    flz reset --dry-run                # plan only, exit 0
    flz reset --yes                    # default scope (state, process_hub)
    flz reset --yes --all              # every opt-in clear too, auth included
    flz reset --yes --json             # machine-readable output"#;

pub(crate) const ROLE: Help = r#"flz role - Principal → role bindings

USAGE:
    flz role <COMMAND>

COMMANDS:
    bind <principal> <role> <namespace>   Add a role to a principal in its namespace
    unbind <principal> [<role>]           Remove one role, or every role when none is named
    list                                  List bindings visible to the caller
    help                                  Show this help message

A principal holds a set of roles in one namespace, and an action is allowed
when any of them grants it. A bind naming another namespace is refused.

Identity for these verbs rides `--token <token>` or the FLEZHA_TOKEN env var."#;

pub(crate) const RUNNER: Help = r#"flz runner - Runner management commands

USAGE:
    flz runner <COMMAND>

COMMANDS:
    list     List runners
    check    Report whether a runner on this host can reach a controller
    drop     Close a runner's session and refuse its process until a different one connects
    dropped  List the dropped runner processes
    readmit  Lift a drop, so the dropped process is admitted again
    help     Show this help message

Use 'flz runner <COMMAND> --help' for more information on a specific command."#;

pub(crate) const SCHEDULE: Help = r#"flz schedule - cron-scheduled steps relay spawns

USAGE:
    flz schedule <SUBCOMMAND>

SUBCOMMANDS:
    add <name> --cron "<expr>" --tz <iana> --factory <factory_id>
        [--namespace <ns>] [--init-values <json>] [--text <payload>] [--channel <ch>]
        [--plugins <json>]
                 Add a durable schedule of a steps factory; a factory a
                 connected runner announced as flow is refused.
                 --init-values is the state every fired instance starts with;
                 --text is the first message it receives, on the entry channel
                 --channel names or the first declared one.
                 --plugins takes the same array as `relay start --plugins`;
                 each firing binds its instance to the plugins it names.
    list         List schedules with their next fire time
    remove <name>  Remove a schedule
    run <name>     Fire one instance immediately with the stored config;
                   judged as relay.start in the schedule's namespace
    help         Show this help message

EXAMPLES:
    flz schedule add digest --cron "0 8 * * *" --tz Europe/Tallinn --factory ai_digest
    flz schedule add nightly --cron "0 2 * * *" --tz UTC --factory report --text '{"day":"today"}' --channel requests
    flz schedule list
    flz schedule run digest

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var."#;

pub(crate) const STATE: Help = r#"flz state - Per-Cell state inspection and time-travel queries

USAGE:
    flz state <SUBCOMMAND>

SUBCOMMANDS:
    get <relay/tasklet[/key]>             Current in-memory state of a live Cell, with its change_seq
    list <relay>                          List Cells under a Relay class
    at <relay/tasklet/key> <seq|ts>       Query state at a point in time
    rotate <relay/tasklet/key>            Force a compaction snapshot
    retention <relay> <hours>             Set time-travel retention
    delete <relay/tasklet/key>            Hard-delete a Cell's persisted state
    outbox <relay/tasklet/key>            Effectively-once outbox status (lag, cursor, watermark)
    journal <relay/instance>              Steps instance task-journal (committed tasks, iterations, park)
    inbox <relay/instance>                Steps instance durable inbox (offsets, undelivered tail)
    help                                     Show this help message

EXAMPLES:
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

pub(crate) const TEMPLATE: Help = r#"flz template - Start a project from a template the release carries

USAGE:
    flz template <COMMAND>

COMMANDS:
    list                                Print each language and example the release carries
    create <language> <dir> [OPTIONS]   Write a template's files into a new folder <dir>
    help                                Show this help message

CREATE OPTIONS:
    --example <name>   The template (default: starter)
    --tar              Write the files as a tar stream on stdout instead of a folder

<dir> must not exist or must be empty, and names the folder only: nothing in
the files is renamed. With no controller needed, the files come from this
release, and --tar hands them out of a container to the host.

EXAMPLES:
    flz template list
    flz template create java my-app
    docker exec flezha-controller /flezha/bin/flz template create python my-app --tar | tar x -k

With -k, tar keeps any file already there rather than overwriting it."#;

pub(crate) const TOKEN: Help = r#"flz token - Scoped-token management

USAGE:
    flz token <COMMAND>

COMMANDS:
    issue <principal> <namespace> <role>   Issue a token (printed once)
        [--ttl <seconds>|never]
    revoke <token-id>                      Revoke by id (prefix of the token hash)
    list                                   List tokens visible to the caller
    help                                   Show this help message

A `runner` token is the credential a runner connection proves its namespace
with. It never expires and `--ttl` is refused for it: the holder is an
unattended process, and a credential that lapses fails it at a restart. A
namespace may hold several at once, so a rotation is issue, roll the fleet,
revoke the old one; revoking one closes the sessions it authenticated.

Identity for these verbs rides `--token <token>` or the FLEZHA_TOKEN env var."#;
