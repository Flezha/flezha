# The `flz` command line

`flz` is the tool a controller is operated with: starting and stopping it,
starting operators and sending them messages, managing namespaces, tokens and
plugins, and reading state. Each of its commands is a verb of one group, such
as `flz operator start` or `flz namespace create`.

Three things about it are worth knowing before anything else:

- **It reaches a controller by running the `flzvm` script beside it.** So it
  exists only inside a release, and talks only to the controller it sits next
  to. In Docker that is `docker exec <container> /flezha/bin/flz ...`. A
  controller embedded in an Elixir application has no `flzvm`, and `flz`
  cannot reach it.
- **Each verb takes `--json`** (or `--output json`). A verb that answers data
  answers it as JSON. A verb that acts still prints its success in text, so
  for it `--json` changes only how a failure is written.
- **Identity for a gated verb** rides `--token <t>` or the `FLEZHA_TOKEN`
  variable of the shell that runs `flz` ([`authorization.md`](authorization.md)).

## How it reaches the controller

`flz` resolves the controller's node the same way the controller did, and
reads the same cookie file: with the same `FLEZHA_STATE_DIR` and
`FLEZHA_COOKIE_FILE` as the controller, or neither, it needs nothing more. A
controller started with `RELEASE_COOKIE` set needs the same value exported
where `flz` runs. A verb that cannot reach the controller reports it
unreachable (exit 3). `flz runner check` is the exception: it tests the runner
socket and needs neither a controller nor a cookie
([`connecting_a_runner.md`](connecting_a_runner.md)).

Options are read wherever they are written: `flz operator stop --token t op1`
and `flz operator stop op1 --token t` are the same request, because a verb's
positional arguments are the words left once its options and their values are
taken out. Most verbs ignore a word they do not recognise and use their
defaults; `flz boot await` and `flz reset` refuse one, because one blocks for
as long as its option says and the other deletes data. Each group prints its
verbs with `help`, and `flz <group> <verb> --help` prints a verb's options.

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

The [error catalogue](../../errors.md) lists every code.

## The command groups

| Group | Verbs | Where it is explained |
| --- | --- | --- |
| `flz controller` | `start`, `stop`, `status`, `mem-top` | [`deploying_a_controller.md`](deploying_a_controller.md) |
| `flz boot` | `status`, `await` | [`deploying_a_controller.md`](deploying_a_controller.md#check-that-it-is-up), [`running_operators.md`](running_operators.md#after-a-controller-restart) |
| `flz cluster` | `status`: nodes, leader, operator counts and health, recovery state | [`../advanced/clustering.md`](../advanced/clustering.md) |
| `flz runner` | `list`, `check` | [`connecting_a_runner.md`](connecting_a_runner.md) |
| `flz operator` | `start`, `stop`, `list`, `info`, `state`, `send`, `call`, `listen`, `scale`, `rewind`, `replay` | [`running_operators.md`](running_operators.md), [`workflows.md`](workflows.md), [`../advanced/partitioning.md`](../advanced/partitioning.md) |
| `flz namespace` | `create`, `list`, `show`, `set-quota`, `delete` | [`namespaces.md`](namespaces.md) |
| `flz plugin` | `install`, `uninstall`, `sync`, `enable`, `bind`, `unbind`, `update`, `disable`, `list`, `help` | [`plugins.md`](plugins.md), [`../advanced/installing_plugins.md`](../advanced/installing_plugins.md) |
| `flz state` | `get`, `list`, `at`, `rotate`, `retention`, `delete`, `outbox`, `journal`, `inbox` | [`state.md`](state.md), [`workflows.md`](workflows.md) |
| `flz dlq` | inspect, replay or drop parked payloads | [`durability.md`](durability.md) |
| `flz schedule` | `add`, `list`, `remove`, `run`: steps operators started on a cron schedule | [`authorization.md`](authorization.md#which-role-each-verb-needs) |
| `flz auth` | `bootstrap`, `whoami`, `rebootstrap` | [`authorization.md`](authorization.md) |
| `flz token` | `issue`, `revoke`, `list` | [`authorization.md`](authorization.md#tokens) |
| `flz role` | `bind`, `unbind`, `list` | [`authorization.md`](authorization.md#roles) |
| `flz audit` | `list`, `read`, `tail`, `stats`, `rotate` | [`authorization.md`](authorization.md#the-audit-trail) |
| `flz observe` | `sources`, `sinks`, `tail`, `enable`, `disable` | the observability reference |
| `flz reset` | clear the controller's data on disk | [`running_operators.md`](running_operators.md#start-from-a-clean-slate-flz-reset) |

Which role each verb needs is in
[`authorization.md`](authorization.md#which-role-each-verb-needs).

## Calling an operator

`flz operator send` delivers an input and reports the delivery.
`flz operator call` delivers one and waits for the operator's answer: the
output a `response()` end of its graph produced from that input
([`../advanced/operator_calls.md`](../advanced/operator_calls.md)):

```sh
$ flz operator call echo --text hello
```

```output
hello
```

Stdout carries the answer's payload and a newline and nothing else, so a
script can use it as it is. `--timeout` is how long the operator has to
answer, in milliseconds (30 000 by default); it has no short spelling, because
`-t` is `--text`. Past it the verb fails with `call.timeout` and exits 7, while
the input stays accepted and its turn still runs. A denied call exits 4, and
an unknown operator `operator.not_found`, exit 5.

A partition group is one id for several members, so both verbs take `--key`,
the key whose member should take the input:

```sh
$ flz operator send prices --key EURUSD --text x
```

```output
Delivered to 'prices#p0'.
```

Without it, either verb fails with `call.target_unkeyed`, naming the members,
and exits 2.

## Plugins

A plugin has a name in its namespace, and operators are bound to it:

| Verb | What it does |
| --- | --- |
| `flz plugin enable <name> <provider> [--single]` | enables the plugin; with `--operator_id` it binds that operator in the same command. A controller plugin takes its hooks here, one `--hook` each, and no operator |
| `flz plugin bind <name> --operator_id <op>` | binds an operator, with its own `--hook` and `--channel` |
| `flz plugin unbind <name> --operator_id <op>` | takes the binding away; the plugin stays enabled |
| `flz plugin update <name>` | changes the plugin's options, in place when they are live; `--hook` replaces a controller plugin's hooks |
| `flz plugin disable <name>` | stops the plugin for every operator bound to it |
| `flz plugin list` | the plugins enabled in the namespace, their operators and controllers, and a controller plugin's hooks and the controllers it lags on |

A provider's own settings are passed as `X<key>=<value>` arguments to
`enable`. [`plugins.md`](plugins.md) explains them.

## Limits and gotchas

- **`flz` lives in a release.** There is no standalone `flz` that reaches a
  remote controller; run it on the controller's machine or in its container.
- **`--json` does not turn an action's success into JSON.**
- **A cookie mismatch looks like an unreachable controller**:
  `--rpc-eval : RPC failed with reason :noconnection`.
- **`ERL_FLAGS` exported in a shell profile** also enlarges the short-lived
  node each `flz` verb starts.

## See also

- [`running_operators.md`](running_operators.md): the verbs used most.
- [`authorization.md`](authorization.md): tokens and the role each verb needs.
- [`../../errors.md`](../../errors.md): every error code and its class.
- [`../../configuration.md`](../../configuration.md): the variables `flz` and the controller read.
- Tutorial: [`run_the_echo.md`](../../tutorials/echo/run_the_echo.md).
