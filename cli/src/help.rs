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

pub(crate) const AUDIT_LIST: Help = r#"flz audit list [--json]"#;

pub(crate) const AUDIT_READ: Help = r#"flz audit read <FILE> [OPTIONS]

OPTIONS:
    --json              Emit NDJSON
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
    --json    Emit machine-readable output (NDJSON for tail/read).
              --output json is the same request."#;

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
controller's verdict is the answer. Still unreachable at the timeout, it exits 1
naming what that can mean: not up yet, stopped after it was reached, or a cookie
or node name that is not the controller's. A controller that refuses the cookie
logs two errors per attempt meanwhile, about four a second."#;

pub(crate) const CLUSTER: Help = r#"flz cluster - Controller cluster status

USAGE:
    flz cluster <SUBCOMMAND>

SUBCOMMANDS:
    status              Show nodes, leader, per-node operative counts, operative health, recovery state
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
    0   Clean shutdown (or `--force` finalised cleanly)
    1   Partial drain (timeout elapsed; some Operatives still local)
    2   Already shutting down (the in-flight outcome is reported)
    3   Controller not reachable on the CLI socket"#;

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
  flz dlq list [operative]                       Parked entries, nearest-to-expiry first
  flz dlq show <operative/procedure/entry-id>    Entry metadata + payload preview
  flz dlq replay <operative/procedure/entry-id>  Re-dispatch through the normal input path
  flz dlq drop <operative/procedure/entry-id>    Discard the entry (audited)

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var."#;

pub(crate) const FLZ: Help = r#"flz - Flezha CLI

USAGE:
    flz <COMMAND>

COMMANDS:
    controller     Controller management commands
    cluster        Controller cluster status (nodes, leader, operative health)
    operative      Operative management commands
    namespace      Namespace management commands
    runner         Runner management commands
    plugin         Plugin management commands
    boot           Boot/recovery status, await, and legacy snapshot import
    state          Per-Entity state queries (time-travel, retention, list)
    reset          Clear Controller durable runtime data on disk (controller must be stopped)
    observe        Observability foundation inspection (sources, sinks, tail)
    audit          Durable audit log inspection (list, read, tail, stats, rotate)
    dlq            Dead-letter queue: inspect, replay, or drop parked payloads
    schedule       Cron-scheduled workflow operative spawns (add, list, remove, run)
    auth           Bootstrap claim, identity (whoami), offline break-glass recovery
    token          Scoped-token management (issue, revoke, list)
    role           Principal → role bindings (bind, unbind, list)
    help           Show this help message

OUTPUT FORMAT:
    Every verb that renders takes --json, or the same request spelled
    --output json / -o json. Any other format is refused, not rendered as text.

Use 'flz <COMMAND> help' for more information on a specific command.

EXAMPLES:
    flz controller start
    flz operative start my_factory --id my_operative --namespace prod
    flz plugin install /path/to/my_plugin-1.0.0.fpz
    flz plugin list --installed
    flz plugin enable kafka_producer provider1 Xhosts=localhost:9092
    flz namespace list --output json
    flz boot status
    flz boot await --timeout 60"#;

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

Quota fields: max_operatives, max_entities_per_operative, max_mailbox_bytes_total,
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

pub(crate) const OPERATIVE_START: Help = r#"flz operative start - Start an operative

USAGE:
    flz operative start <factory_id> [OPTIONS]

OPTIONS:
    --id <id>, -i <id>          Id to start under (default: generated)
    --namespace <ns>            Namespace to start in (default: default)
    --kind <stream|workflow>    Operative kind (default: stream)
    --init-values <json>        Seed state at start
    --plugins <json-array>      Plugin specs attached at start
    --partitions <N>            Start N partition members as one group
    --snapshot-interval <N>     Changes between snapshots
    --output <text|json>        Output format (default: text)
    --json                      The same as --output json

ID:
    Without --id the controller generates one, <factory>-<16 characters>, and
    prints it: Started echo-k7m2qzxw4pd3fn2a. With --output json the answer is
    {"id": ..., "outcome": "started" | "already_running"}.

    A start without --id that is run again after its answer was lost starts a
    second operative, and a second stream runs until it is stopped. Pass --id
    when the start may be retried: a start of an id that is already running
    prints "<id> is already running, left untouched" and starts nothing.

INIT VALUES:
    A stream operative's document is keyed by Procedure name, one entry per
    Procedure to seed. Name only what you want seeded; a Procedure you leave
    out keeps the state its class builds.

        --init-values '{"compare_filter_procedure": {"comparable": "hi"}}'
        --init-values '{"ohlc": {"1m": {"high": 0}}}'   one state per declared key

    A name the class does not carry is ignored, not refused: the runner logs
    it and the start still succeeds, so check the spelling. A workflow
    operative applies no state: the whole document seeds its entry channel,
    decoded into that channel's declared type.

EXAMPLES:
    flz operative start do_nothing
    flz operative start do_nothing --id my_operative
    flz operative start candles --id ohlc --partitions 3
    flz operative start order_aggregator -i orders --namespace prod -o json"#;

pub(crate) const OPERATIVE_LISTEN: Help = r#"flz operative listen - Print the frames an operative emits or ingests

USAGE:
    flz operative listen <operative_id> [OPTIONS]

OPTIONS:
    --input                   Listen to what the operative ingests, not what it emits
    --duration-ms <N>         Stop after N milliseconds (default 30000)
    --output <text|json>      Output format (default: text)
    --json                    The same as --output json
    --namespace <ns>          Namespace the operative runs in (default: default)

OUTPUT:
    One line per frame: the channel, a tab and the payload. A frame from a
    keyed sink adds a third field, entity=... seq=... key=..., and a frame
    with no channel prints - in its place. With --json each frame is one
    object carrying channel, payload (payload_base64 for bytes that are not
    text) and the frame's whole metadata map.

    The listener is detached when the window closes, on Ctrl-C, and when the
    caller goes away. A partition group is listened to by its group id.

EXAMPLES:
    flz operative listen my_echo --duration-ms 10000
    flz operative listen my_echo --input
    flz operative listen ohlc --json"#;

pub(crate) const OPERATIVE: Help = r#"flz operative - Operative management commands

USAGE:
    flz operative <COMMAND>

COMMANDS:
    start       Start an operative
    scale       Rescale a partition group to N partitions
    stop        Stop an operative
    state       Get operative state
    list        List operatives
    info        Get operative info
    listen      Print the frames an operative emits or ingests
    send        Send a payload to an operative's input stream
    rewind      Rewind a workflow loop to a committed iteration
    replay      Re-run an Entity's retained inputs from a point in the past
    help        Show this help message

EXAMPLES:
    flz operative start candles --id ohlc --partitions 3
    flz operative scale ohlc 5
    flz operative send orchestrator --text '{"task":"refactor"}'
    flz operative send my_echo --text 'hello world' --await 2000
    flz operative listen my_echo --duration-ms 10000
    flz operative rewind chat-1 --to-iteration 3
    flz operative replay candles/aggregate/BTC --from 1720000000000

Use 'flz operative <COMMAND> --help' for more information on a specific command.

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var."#;

pub(crate) const PLUGIN: Help = r#"flz plugin - Plugin management commands

USAGE:
    flz plugin <COMMAND>
    flz plugin enable <plugin_id> <provider_id> [--hook=<name>]... [OPTIONS]

COMMANDS:
    install    Install a plugin from a .fpz package
    upgrade    Upgrade an installed plugin to a new version
    uninstall  Remove an installed plugin
    enable     Enable a plugin (installed package or legacy provider)
    disable    Disable a plugin
    list       List plugins: `--installed` (catalog, text or json) or
               providers (default, text only)
    info       Show details for an installed plugin
    help       Show help for a specific plugin provider

HOOKS:
    A hook is a point at which the operative calls its plugin, named by the
    callback the provider defines: on_input sends every payload the operative
    ingests, on_output every payload it emits, on_interval runs on a timer.
    `flz plugin help <provider_id>` lists the hooks a provider has.

    `--hook=<name>` names one and may be repeated. A hook's sub-options follow
    it as `--<name>.<key>=<value>`. on_interval has no default interval, so
    it needs `--on_interval.interval_ms=<ms>`.

    An enable naming no hook has no hooks. A plugin that only receives still
    feeds its operative: its connection is held by the provider's own worker,
    which no hook governs.

    The enable reads --namespace, --operative_id, --hook and the X-prefixed
    provider settings, and refuses any other option.

EXAMPLES:
    flz plugin install /path/to/kafka_ingest-1.0.0.fpz
    flz plugin list --installed
    flz plugin list --installed --json
    flz plugin info kafka_ingest
    flz plugin enable kafka_ingest
    flz plugin upgrade kafka_ingest /path/to/kafka_ingest-1.1.0.fpz
    flz plugin uninstall kafka_ingest
    # Legacy provider enable (compile-time providers):
    flz plugin enable ws_out operative_websocket --operative_id my_op \
        --Xuri=ws://localhost:4500 --hook=on_output --on_output.position=0
    # Source only: it injects what arrives and sends nothing back.
    flz plugin enable ws_in operative_websocket --operative_id my_op \
        --Xuri=ws://localhost:4500
    flz plugin disable ws_out"#;

pub(crate) const RESET: Help = r#"flz reset - Clear Controller durable runtime data on disk

USAGE:
    flz reset [OPTIONS]

DESCRIPTION:
    Clears the contents of the Controller's durable on-disk state so the
    next boot starts from a clean slate. Clears nothing while the
    Controller is up; stop it first with `flz controller stop`.

    By default clears:
        app_data/state/         (per-Entity WAL/snapshot/outbox)
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
    writes a fresh bootstrap token. Until it is claimed, operator verbs
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
    1   Invalid usage (missing --yes without --dry-run, conflicting flags,
        malformed --app-data-root, refusal to delete the root itself)
    2   Partial success (some roots cleared, then a filesystem error)
    3   I/O error before any action
    4   Refused: Controller is up

EXAMPLES:
    flz reset                          # plan only, exit 1
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
    list    List runners
    check   Report whether a runner on this host can reach a controller
    help    Show this help message

Use 'flz runner <COMMAND> --help' for more information on a specific command."#;

pub(crate) const SCHEDULE: Help = r#"flz schedule - cron-scheduled workflow operative spawns

USAGE:
    flz schedule <SUBCOMMAND>

SUBCOMMANDS:
    add <name> --cron "<expr>" --tz <iana> --factory <factory_id>
        [--namespace <ns>] [--init-values <json>] [--plugins <json>] [--kind workflow]
                 Add a durable schedule (workflow operatives only).
                 --plugins takes the same array as `operative start --plugins`;
                 each firing prefixes the plugin ids with its instance id.
    list         List schedules with their next fire time
    remove <name>  Remove a schedule
    run <name>     Fire one instance immediately with the stored config;
                   judged as operative.start in the schedule's namespace
    help         Show this help message

EXAMPLES:
    flz schedule add digest --cron "0 8 * * *" --tz Europe/Tallinn --factory ai_digest
    flz schedule list
    flz schedule run digest

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var."#;

pub(crate) const STATE: Help = r#"flz state - Per-Entity state inspection and time-travel queries

USAGE:
    flz state <SUBCOMMAND>

SUBCOMMANDS:
    get <operative/procedure[/key]>          Current in-memory state of a live Entity, with its change_seq
    list <operative>                         List Entities under an Operative class
    at <operative/procedure/key> <seq|ts>    Query state at a point in time
    rotate <operative/procedure/key>         Force a compaction snapshot
    retention <operative> <hours>            Set time-travel retention
    delete <operative/procedure/key>         Hard-delete an Entity's persisted state
    outbox <operative/procedure/key>         Effectively-once outbox status (lag, cursor, watermark)
    journal <operative/instance>             Workflow instance proc-journal (committed procs, iterations, park)
    inbox <operative/instance>               Workflow instance durable inbox (offsets, undelivered tail)
    help                                     Show this help message

EXAMPLES:
    flz state get MarketOperative/OhlcProcedure/M1
    flz state get MarketOperative/OhlcProcedure --output json
    flz state list MarketOperative
    flz state at MarketOperative/OhlcProcedure/M1 12345
    flz state at MarketOperative/OhlcProcedure/M1 2026-05-01T12:00:00Z
    flz state rotate MarketOperative/OhlcProcedure/M1
    flz state retention MarketOperative 6
    flz state delete MarketOperative/OhlcProcedure/M1
    flz state outbox MarketOperative/OhlcProcedure/M1
    flz state journal etl-job-42
    flz state inbox chat-session-7"#;

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
