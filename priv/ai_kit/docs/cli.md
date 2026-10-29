# CLI

`flz` is the tool a controller is operated with: starting and stopping it,
starting relays and sending them messages, managing namespaces, tokens and
plugins, and reading state. Each of its commands is a verb of one group, such
as `flz relay start` or `flz namespace create`.

`flz` is the file `bin/flz` in the release directory, the `flezha/` directory
the [archive](../deploying/deploying_a_controller.md) unpacks to. It always runs by that
path, as `./bin/flz` from the release directory. To type only `flz`, as these
pages do, put that `bin` on your path, in each shell or in your shell profile:

```sh
$ export PATH="$PWD/bin:$PATH"
```

The release directory is an
[Elixir release](https://hexdocs.pm/mix/Mix.Tasks.Release.html): the
controller together with its own Erlang runtime, so the machine needs nothing
else installed. What is in it, and what matters to you:

| Path | What it is |
| --- | --- |
| `bin/flz` | the CLI |
| `releases/<version>/vm.args` | the VM's flags, yours to edit ([Tune the VM](../deploying/deploying_a_controller.md#tune-the-vm)) |
| `storage/app/` | the controller's durable state and its cookie, created in the directory the controller starts from |
| `storage/logs/flezha.log` | everything the controller logs, also in the directory it starts from |
| `tmp/log/` | the boot, and anything printed before the logger is up |
| `bin/flzvm` | the script every Elixir release comes with, which starts and stops the controller's VM. `flz` is a separate, compiled program that checks your command, sends it to the controller and prints the answer; it runs `flzvm` only to start the controller and for the few verbs that need no controller. You rarely run it yourself |
| `erts-<version>/`, `lib/` | the Erlang runtime and the compiled code; nothing to change |

Three things about it are worth knowing before anything else:

- **It talks to the running controller over the controller's control
  socket.** Each command becomes one request on the Unix socket the
  controller's runners attach to. The controller runs it and sends the answer
  back, and `flz` prints it and exits with the code the controller gave. So
  `flz` reaches only a controller on its own machine whose socket directory it
  finds ([how it finds it](#how-it-reaches-the-controller)).
- **Each verb takes `--json`** (or `--output json`). A verb that answers data
  answers it as JSON. A verb that acts still prints its success in text, so
  for it `--json` changes only how a failure is written.
- **Identity for a gated verb** rides `--token <t>` or the `FLEZHA_TOKEN`
  variable of the shell that runs `flz`. A token that does not verify is
  refused before any verb runs ([`authorization.md`](../core/authorization.md#how-it-works)).

## How it reaches the controller

`flz` looks for the controller's control socket, `runner_control_channel.sock`,
the way a runner does: in `FLEZHA_SOCKET_FILES_PATH` when it is set, and
otherwise in the directory the controller chooses
([the socket directory](../deploying/connecting_a_runner.md#the-socket-directory)).
A shell with the same `FLEZHA_SOCKET_FILES_PATH` as the controller, or neither,
needs nothing more. The directory is open only to the controller's user, or to
one group, so only they reach the controller this way, and a gated verb still
needs its token. Every controller listens on this socket, whatever
`FLEZHA_TRANSPORT` its runners use.

A verb that finds no socket, or no controller behind it, reports it
unreachable (exit 3). A controller that does not accept the request within five
seconds, such as one from an older release, is reported as
`cli.protocol_unsupported` (exit 2): use the `flz` of the controller's own
release.

Three verbs start a VM of their own through `bin/flzvm` instead, because they
work with no controller running: `flz controller start`, the `flz plugin
install`, `uninstall` and `sync` verbs, and `flz mcp`'s knowledge tools while
no controller can be reached. `flz runner check` needs no controller either: it
tests the runner socket
([`connecting_a_runner.md`](../deploying/connecting_a_runner.md)).

## Options and help

`flz help` lists the command groups, and `flz <group> help` lists a group's
verbs, each with one line saying what it does, and a few examples. The
[command reference](command_reference.md) lists every verb with its usage line.

`flz <group> <verb> --help`, `-h` and `flz <group> help <verb>` print the
verb's line, its usage line and each option it reads, plus the ones every verb
takes; some verbs follow these with notes and examples. Settings a verb takes
as `X<key>=<value>` or `--<key> <value>` appear only in its usage line. A verb
that reaches the controller is described by the operation it sends: its line is
the operation's own description, the one an MCP or HTTP caller reads, and an
option that carries one of the operation's arguments says what that argument
is, unless the CLI fills the option in itself: `--namespace` is `default` when
it is not written.

An option can go before or after the other arguments: `flz relay stop
--token t op1` and `flz relay stop op1 --token t` are the same command.
Most verbs ignore an option they do not know. The verbs where a misread word
costs more than a refusal stop with an error instead: `flz boot await`,
`flz relay listen` and `flz relay call`, which wait as long as their options
say; `flz reset`, which deletes data; `flz relay start` and `flz schedule add`,
which would run a relay without the setting meant; `flz mcp`; and every
`flz dlq` verb.

## Listing by the page

`flz relay list`, `flz plugin list` and `flz dlq list` print every row unless
asked for a page. `--limit <n>` prints at most `n` rows (at most 500), then how
many matched in all and, when more remain, the cursor of the next page:

```sh
$ flz relay list --limit 50 --health unhealthy
...
  Showing 50 of 212.
  Next page: --after g2gDdwpyZWxheV9saXN0...
$ flz relay list --limit 50 --health unhealthy --after g2gDdwpyZWxheV9saXN0...
```

The filters narrow the rows before they are counted. `flz relay list` takes
`--health` (health words, comma-separated; `unhealthy` names every fault),
`--factory`, `--runner` and `--search`, a piece of the relay or factory id in
any case. `flz plugin list` takes `--provider`, `--relay` (the plugins bound
to that relay) and `--search`; a paged plugin row names at most 10 of its
relays and how many there are in all. `flz dlq list` pages newest first. A
cursor belongs to the listing and the order that printed it; any other is
refused. Without these options each listing prints what it always printed.

The operations behind them, and the steps reads, take the same page as a
trailing `opts` argument (`limit`, `after`, `sort`, `filter`), and answer
`rows`, `total`, `next` and `unreachable`. A paged `Relay.list` also answers
the namespace's `summary`: its rows by health, the faulted partition groups,
and the factories whose relays wait for a runner. The
[command reference](command_reference.md) lists each flag.

## When a verb fails

A failure is written on stderr, never mixed into the answer on stdout, as a
sentence ending in its code, with the next command to run when there is one:

```output
error: namespace 'ghost' does not exist (namespace.not_found)
hint: flz namespace list
```

With `--json` the same failure is one line a program can parse:

```json
{"error": {"code": "namespace.not_found", "class": "not_found", "message": "namespace 'ghost' does not exist", "details": {"namespace": "ghost"}, "hint": "flz namespace list"}}
```

The exit code follows the error's class, so a script can tell "fix the
arguments" from "retry later" without reading the text:

| Exit | Class | Meaning |
| --- | --- | --- |
| 0 | | success |
| 1 | `internal` | a defect or an unexpected failure |
| 2 | `invalid` | the command itself is wrong |
| 3 | `unreachable` | the controller could not be reached |
| 4 | `denied` | the token or its role does not allow it |
| 5 | `not_found` | what the command names does not exist |
| 6 | `conflict` | the current state refuses it |
| 7 | `unavailable` | not possible right now; it may succeed if retried |

Codes 2 to 7 follow the order in which a command is checked: first the
command itself, then whether the controller answers, whether the token allows
it, whether what it names exists, whether the current state allows it, and
last whether it can be done right now.

The [error catalogue](../../errors.md) lists every code.

## The command groups

Each group is a set of verbs about one thing. Its name leads to its help and
examples; the [command reference](command_reference.md) lists every verb in
one line.

| Group | What it does | Explained in |
| --- | --- | --- |
| [`flz controller`](controller_commands.md#flz-controller) | Starts and stops the controller, and shows its settings and memory use. | [`deploying_a_controller.md`](../deploying/deploying_a_controller.md), [graceful shutdown](../deploying/deploying_a_controller.md#graceful-shutdown), [Start the controller](../../getting_started/up_and_running.md#start-the-controller) |
| [`flz boot`](controller_commands.md#flz-boot) | Says whether the controller has finished starting, or waits until it has. | [`deploying_a_controller.md`](../deploying/deploying_a_controller.md#check-that-it-is-up), [after a restart](../core/running_and_testing.md#after-a-controller-restart) |
| [`flz cluster`](controller_commands.md#flz-cluster) | Shows the controllers of a cluster, which one leads, and how their relays are doing. | [`clustering.md`](../deploying/clustering.md), tutorial [Run a cluster](tutorials/run_a_cluster.html) |
| [`flz runner`](controller_commands.md#flz-runner) | Lists the runners connected to a namespace, drops a runner process and readmits it, and checks that a runner on this machine can reach the controller. | [`connecting_a_runner.md`](../deploying/connecting_a_runner.md), [check that it connected](../deploying/connecting_a_runner.md#check-that-it-connected) |
| [`flz relay`](relay_commands.md#flz-relay) | Starts and stops relays, sends them input, and shows what they answer and emit. | [`running_and_testing.md`](../core/running_and_testing.md), [`workflows.md`](../core/workflows.md), [`partitioning.md`](../core/partitioning.md), [`sending_and_calling.md`](../core/sending_and_calling.md#calls), [`sessions.md`](../core/sessions.md#rewinding-a-turn), [Run the echo](../../getting_started/up_and_running.md#run-the-echo) |
| [`flz namespace`](access_commands.md#flz-namespace) | Creates and deletes namespaces, the separate areas relays and runners work in, and sets their quotas. | [`namespaces.md`](../core/namespaces.md) |
| [`flz plugin`](plugin_commands.md#flz-plugin) | Installs plugin packages, enables plugins and connects relays to them. | [`plugins.md`](../plugins/plugins.md), [`hooks.md`](../plugins/hooks.md), [`installing_plugins.md`](../plugins/installing_plugins.md), [Connect a plugin](../../getting_started/connect_a_plugin.md) |
| [`flz state`](relay_commands.md#flz-state) | Reads a relay's stored state, now or at a point in the past, and manages how long it is kept. | [`state.md`](../core/state.md#reading-a-cell-s-state), [`workflows.md`](../core/workflows.md), [`durability.md`](../core/durability.md) |
| [`flz dlq`](relay_commands.md#flz-dlq) | Lists the inputs a relay gave up on after its retries, and replays or drops them. | [`durability.md`](../core/durability.md#the-dead-letter-queue) |
| [`flz schedule`](relay_commands.md#flz-schedule) | Starts a workflow on a cron schedule. | [`workflows.md`](../core/workflows.md#one-instance-per-piece-of-work), [`relay_settings.md`](../../configuration/relay_settings.md) |
| [`flz auth`](access_commands.md#flz-auth) | Claims the first admin token, and shows who a token belongs to. | [`authorization.md`](../core/authorization.md#claim-the-admin), [a lost admin](../core/authorization.md#a-lost-admin) |
| [`flz token`](access_commands.md#flz-token) | Issues, lists and revokes tokens. | [`authorization.md`](../core/authorization.md#tokens), [runner credentials](../core/authorization.md#runner-credentials) |
| [`flz role`](access_commands.md#flz-role) | Gives roles to a principal and takes them away. | [`authorization.md`](../core/authorization.md#roles) |
| [`flz audit`](access_commands.md#flz-audit) | Reads the record of who did what. | [`authorization.md`](../core/authorization.md#the-audit-trail) |
| [`flz logs`](controller_commands.md#flz-logs) | Reads the relay history and the audit log of every controller. | [`running_and_testing.md`](../core/running_and_testing.md#the-relay-history) |
| [`flz observe`](controller_commands.md#flz-observe) | Lists the controller's sources of metrics and events, and streams their signals live. | [`hooks.md`](../plugins/hooks.md#controller-plugins-and-their-hooks) |
| [`flz reset`](controller_commands.md#flz-reset) | Deletes the controller's data on disk, so its next start is empty. | [`running_and_testing.md`](../core/running_and_testing.md#start-from-a-clean-slate-flz-reset) |
| [`flz mcp`](../core/mcp_server.md) | Serves Flezha to an AI assistant over MCP; the assistant starts it, not you. | [`mcp_server.md`](../core/mcp_server.md) |

Which role each verb needs is in
[`authorization.md`](../core/authorization.md#which-role-each-verb-needs).

## Limits and gotchas

- **`flz` reaches only a controller on its own machine.** The control socket
  is local; run `flz` on the controller's machine or in its container.
- **`--json` does not turn an action's success into JSON.**
- **Another socket directory looks like an unreachable controller.** A shell
  whose `FLEZHA_SOCKET_FILES_PATH` differs from the controller's finds no
  socket; the error names the path it looked at.
- **`ERL_FLAGS` exported in a shell profile** also reaches the VM each of the
  three verbs above starts.
