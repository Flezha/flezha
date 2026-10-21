# Controller commands

The groups that look after the controller itself: starting and stopping it,
checking that it is up, the runners connected to it, its signals, and
clearing its data.

Each section shows, for the verbs used most, a command with its answer.
Unless a section names another page, the answers come from the echo of
[Up and running](../../getting_started/up_and_running.md#run-the-echo).
How `flz` reaches the controller, its options and its exit codes are on the
[CLI](cli.md) page, and every verb in one list is the
[command reference](command_reference.md).

## `flz controller`

Starts and stops the controller, and shows its settings and memory use. Explained in [`deploying_a_controller.md`](../deploying/deploying_a_controller.md), [graceful shutdown](../deploying/deploying_a_controller.md#graceful-shutdown), [Start the controller](../../getting_started/up_and_running.md#start-the-controller).

`flz controller help` prints the group's verbs, and the [command
reference](command_reference.md) lists each with its usage line.

`start` takes the way the controller runs: `--mode attached` (the default),
`daemon`, `attached_iex` or `daemon_iex`, each described by
`flz controller start --help`.

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
state_root:                    /opt/flezha/storage/app/state
file_plugin_root:              /opt/flezha/storage/app/files
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

`flz boot help` prints the group's verbs, and the [command
reference](command_reference.md) lists each with its usage line.

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

`flz cluster help` prints the group's verbs, and the [command
reference](command_reference.md) lists each with its usage line.

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

`flz runner help` prints the group's verbs, and the [command
reference](command_reference.md) lists each with its usage line.

`check` needs no controller token and no cookie, only the socket directory:

```sh
$ flz runner check
```

```output
controller reachable
  directory  /run/user/1000/flezha (from $XDG_RUNTIME_DIR/flezha)
  checked    /run/user/1000/flezha
             /run/flezha
             /srv/flezha/storage/app/sockets
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

`flz observe help` prints the group's verbs, and the [command
reference](command_reference.md) lists each with its usage line.

```sh
$ flz observe sources
```

```output
name	status	kinds	budget	title	description
flezha_http_push	active	metric,event	1000	HTTP push plugin	The plugin that sends a relay's output to a URL.
flezha_plugin	active	event	1000	Plugin lifecycle	Plugins moving between the phases of their life.
flezha_dlq	active	metric	1000	Dead letters	Messages that failed and were set aside in dead-letter queues, and those that expired.
flezha_correction	active	event	1000	Refused corrections	Corrections to a relay that the controller would not apply.
flezha_transport	active	metric	40	Message flow (low-level)	Runner connections and the running totals of messages moving between runners and relays.
...
```

`--json` answers each source with its `area`, every signal it can emit (`signals`), and
what it said recently (`recent`): per signal name, how many the ring buffer holds within
its window and how long ago the newest one arrived. Each signal carries its kind, its
meaning (`description`), a plain `label`, an `outcome` (`normal`, or `failure` for a
signal that reports something going wrong) and, for the signals that fire in normal
operation, a `sentence` template such as `"{runner} connected to {namespace}"`, whose
`{key}` names a label or measurement of the signal and `{key:noun}` a count.

## `flz reset`

Deletes the controller's data on disk, so its next start is empty. Explained in [`running_and_testing.md`](../core/running_and_testing.md#start-from-a-clean-slate-flz-reset).

`flz reset --help` lists what it clears by default and on request, its
options and its exit codes.

The plan for a stopped controller, with nothing cleared:

```sh
$ flz reset --dry-run
```

```output
would clear ./storage/app/state/                             (0 files, 0 B)
would clear ./storage/app/process_hub/                       (0 files, 0 B)
would keep  ./storage/app/certs/                             (use --include-certs to clear)
would keep  ./storage/app/dlq/                               (use --include-dlq to clear)
would keep  ./storage/app/schedules/                         (use --include-schedules to clear)
would keep  ./storage/app/auth/                              (use --include-auth to clear)
would keep  ./storage/app/audit/                             (use --include-audit to clear)
would keep  ./storage/app/plugins/                           (use --include-plugins to clear)
would keep  socket directory                                 (use --include-sockets to clear)
would keep  ./storage/logs/                                  (not reset's to clear)
reset complete in 0 ms
```
