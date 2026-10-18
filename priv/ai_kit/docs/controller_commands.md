# Controller commands

The groups that look after the controller itself: starting and stopping it,
checking that it is up, the runners connected to it, its signals, and
clearing its data.

Each section shows what the group's help prints and, for the verbs used most,
a command with its answer. Unless a section names another page, the answers
come from the echo of [Up and running](../../getting_started/up_and_running.md#run-the-echo).
How `flz` reaches the controller, its options and its exit codes are on the
[CLI](cli.md) page, and every verb in one list is the
[command reference](command_reference.md).

## `flz controller`

Starts and stops the controller, and shows its settings and memory use. Explained in [`deploying_a_controller.md`](../deploying/deploying_a_controller.md), [graceful shutdown](../deploying/deploying_a_controller.md#graceful-shutdown), [Start the controller](../../getting_started/up_and_running.md#start-the-controller).

```sh
$ flz controller help
```

```output
flz controller - Controller management commands

USAGE:
    flz controller <COMMAND>

COMMANDS:
    start    Start the flezha controller
    stop     Stop the flezha controller (graceful)
    status   Print controller status (incl. graceful-shutdown state)
    mem-top  Snapshot top-memory processes / ETS / DETS / allocators
    help     Show this help message

Use 'flz controller <COMMAND> --help' for more information on a specific command.
```

`start` takes the way the controller runs:

```sh
$ flz controller start --help
```

```output
flz controller start - Start the flezha controller

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
    flz controller start -m attached_iex
```

The controller's settings, as the controller of Up and running printed them:

```sh
$ flz controller status
```

```output
node:                          flezha@127.0.0.1
shutdown_in_progress:          false
shutdown_phase:                nil
signal_handler_status:         ok
graceful_shutdown_enabled:     true
graceful_shutdown_timeout_ms:  60000
durability_backend:            unset
durability_posture:            local_only
durability_rpo_breaches:       0
durability_levels:             my_echo=local
durability_superseded:         0
state_root:                    /opt/flezha/app_data/state
file_plugin_root:              /opt/flezha/app_data/files
```

`stop` drains the relays first and reports each phase:

```sh
$ flz controller stop
```

```output
Stopping flezha controller...
[shutdown] phase=quiesce started
[shutdown] phase=quiesce completed duration_ms=1
[shutdown] phase=drain started
[shutdown] phase=drain completed duration_ms=3
[shutdown] phase=finalise started
[shutdown] phase=finalise completed duration_ms=2
[shutdown] complete duration_ms=7 outcome=clean
```

## `flz boot`

Says whether the controller has finished starting, or waits until it has. Explained in [`deploying_a_controller.md`](../deploying/deploying_a_controller.md#check-that-it-is-up), [after a restart](../core/running_and_testing.md#after-a-controller-restart).

```sh
$ flz boot help
```

```output
flz boot - Boot/recovery commands

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
second.
```

A script that starts the controller waits for it with `await`, and `status` says what is met:

```sh
$ flz boot await
```

```output
boot await: ready
```

```sh
$ flz boot status
```

```output
readiness: ready
  met    boot_sequence_complete
  met    hubs_recovered
  met    durable_registry_loaded
  met    declared_hubs_started

namespace                          recovery_state    registry_rows
----------------------------------------------------------------
_system                            normal           0
```

## `flz cluster`

Shows the controllers of a cluster, which one leads, and how their relays are doing. Explained in [`clustering.md`](../deploying/clustering.md), tutorial [Run a cluster](tutorials/run_a_cluster.html).

```sh
$ flz cluster help
```

```output
flz cluster - Controller cluster status

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
    flz cluster status --namespace prod --output json
```

One controller is a cluster of one:

```sh
$ flz cluster status
```

```output
Cluster status for namespace 'default':

  Leader:     flezha@127.0.0.1
  Local node: flezha@127.0.0.1(leader)

  Nodes:
    flezha@127.0.0.1: 1 relay(s) recovery_state=normal [leader]

  Relay health: degraded=1
```

[`clustering.md`](../deploying/clustering.md#example) shows two controllers.

## `flz runner`

Lists the runners connected to a namespace, drops a runner process and readmits it, and checks that a runner on this machine can reach the controller. Explained in [`connecting_a_runner.md`](../deploying/connecting_a_runner.md), [check that it connected](../deploying/connecting_a_runner.md#check-that-it-connected).

```sh
$ flz runner help
```

```output
flz runner - Runner management commands

USAGE:
    flz runner <COMMAND>

COMMANDS:
    list     List runners
    check    Report whether a runner on this host can reach a controller
    drop     Close a runner's session and refuse its process until a different one connects
    dropped  List the dropped runner processes
    readmit  Lift a drop, so the dropped process is admitted again
    help     Show this help message

Use 'flz runner <COMMAND> --help' for more information on a specific command.
```

Each verb's `--help` starts with its usage line:

```output
flz runner list [--namespace | -n <namespace>]
flz runner check
flz runner drop <runner_id> [--namespace | -n <namespace>]
flz runner dropped [--namespace | -n <namespace>]
flz runner readmit <stable_id> [--namespace | -n <namespace>]
```

`check` needs no controller token and no cookie, only the socket directory:

```sh
$ flz runner check
```

```output
controller reachable
  directory  /run/user/1000/flezha (from $XDG_RUNTIME_DIR/flezha)
  checked    /run/user/1000/flezha
             /run/flezha
             /srv/flezha/app_data/sockets
```

```sh
$ flz runner list
```

```output
Fetching runners for namespace 'default'...
Runners in namespace 'default':

  RUNNER                            PID      NODE                  CONNECTED AT          LATE
  default_06355a1b5a63              524527   flezha@127.0.0.1      2026-10-18T09:05:18Z  no
```

`drop` closes the runner's session wherever in the cluster it is connected,
without telling the runner to stop. Its relays are re-placed at once, and
every controller refuses the same process with `runner.dropped` until a
different process of the same runner connects or the drop is lifted with
`readmit`. The drop is recorded against the runner's stable id and the SDK
incarnation of its process, and it survives a controller restart. A runner
with no stable identity cannot be recognised when it reconnects, and is
refused with `runner.untrackable`. Both verbs need an `ops` token.

```sh
$ flz runner drop default_06355a1b5a63
$ flz runner dropped
$ flz runner readmit 6f1c0e3a-runner
```

## `flz observe`

Lists the controller's sources of metrics and events, and streams their signals live. Explained in [`hooks.md`](../plugins/hooks.md#controller-plugins-and-their-hooks).

```sh
$ flz observe help
```

```output
flz observe - Observability foundation inspection

USAGE:
    flz observe <COMMAND> [OPTIONS]

COMMANDS:
    sources    List registered sources: status, budget and what each one is
    sinks      List registered sinks and their overflow policy / watermark
    tail       Stream live signals from a source via the PubSub sink
    enable     Enable a source (transitions its registry status to :active)
    disable    Disable a source (transitions to :disabled — Bus drops emits)
    help       Show this help message
```

```sh
$ flz observe sources
```

```output
name	status	kinds	budget	description
flezha_http_push	active	metric,event	1000	The HTTP push plugin, which sends relay output to a URL.
flezha_plugin	active	event	1000	Plugins entering and leaving their lifecycle phases.
flezha_dlq	active	metric	1000	Dead-letter queues: messages parked and removed.
flezha_correction	active	event	1000	Corrections the controller refused to apply.
flezha_transport	active	metric	40	Runner transports and the data plane: connections, driver health and message counters.
...
```

`--json` answers each source with its `area`, every signal it can emit with its kind and
meaning (`signals`), and what it said recently (`recent`): per signal name, how many the
ring buffer holds within its window and how long ago the newest one arrived.

## `flz reset`

Deletes the controller's data on disk, so its next start is empty. Explained in [`running_and_testing.md`](../core/running_and_testing.md#start-from-a-clean-slate-flz-reset).

```sh
$ flz reset --help
```

```output
flz reset - Clear Controller durable runtime data on disk

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
    flz reset --yes --json             # machine-readable output
```

The plan for a stopped controller, with nothing cleared:

```sh
$ flz reset --dry-run
```

```output
would clear ./app_data/state/                                (0 files, 0 B)
would clear ./app_data/process_hub/                          (0 files, 0 B)
would keep  ./app_data/certs/                                (use --include-certs to clear)
would keep  ./app_data/dlq/                                  (use --include-dlq to clear)
would keep  ./app_data/schedules/                            (use --include-schedules to clear)
would keep  ./app_data/auth/                                 (use --include-auth to clear)
would keep  ./app_data/audit/                                (use --include-audit to clear)
would keep  ./app_data/plugins/                              (use --include-plugins to clear)
would keep  socket directory                                 (use --include-sockets to clear)
would keep  logs/                                            (not reset's to clear)
reset complete in 0 ms
```
