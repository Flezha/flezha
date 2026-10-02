# CLI

`flz` is the one tool a controller is operated with, and nearly every command
in these guides is one of its verbs. Three things about it catch people out,
which is what earns it a page. It reaches a controller by running the `flzvm`
script beside it, so it exists only inside a release and only talks to the
controller it sits next to, which is why the Docker form is
`docker exec <container> /flezha/bin/flz`. Every verb takes `--json` (or
`--output json`): a verb that answers data answers it as JSON, and a verb that
acts still answers in text, so `--json` there changes only how a failure is
written. And identity for a gated verb rides `--token` or `FLEZHA_TOKEN`, which
is what [`authorization.md`](tradedesk/authorization.md#claim-the-admin) sets up.

## When a verb fails

A failure is written on stderr, never among the answer on stdout, as a sentence
ending in its code, with the next command when there is one:

```
error: namespace 'ghost' does not exist (namespace.not_found)
hint: flz namespace list
```

With `--json` the same failure is one line a program can parse:

```json
{"error": {"code": "namespace.not_found", "class": "not_found", "message": "namespace 'ghost' does not exist", "details": {"namespace": "ghost"}, "hint": "flz namespace list"}}
```

The exit code follows the class, so a script can tell "fix the arguments" from
"retry later" without reading the text:

| Exit | Class | Meaning |
|---|---|---|
| 0 | | success |
| 1 | `internal` | a defect or an unexpected failure |
| 2 | `invalid` | the command itself is wrong |
| 3 | `unreachable` | the controller could not be reached |
| 4 | `denied` | the token or its role does not allow it |
| 5 | `not_found` | what the command names does not exist |
| 6 | `conflict` | the current state refuses it |
| 7 | `unavailable` | not possible right now; it may succeed if retried |

[`errors.md`](../errors.md) lists every code.

## Calling an operator

`flz operator send` delivers an input and reports the delivery. `flz operator
call` delivers one and waits for the operator's answer, the output a
`response()` end of its graph produced from that input
([`calls.md`](../sdk/calls.md)):

```
$ flz operator call echo --text hello
hello
```

Stdout carries the answer's payload and a newline and nothing else, so a script
can use it as it is. `--timeout` is how long the operator has to answer, in
milliseconds (default 30000); it has no short spelling, because `-t` is
`--text`. Past it the verb fails with `call.timeout` and exits 7, while the
input stays accepted and its turn still runs.

A partition group is one id for several members, so both verbs take `--key`,
the key whose member should take the input:

```
$ flz operator send prices --key EURUSD --text x
Delivered to 'prices#p0'.
```

Without it, either verb fails with `call.target_unkeyed`, naming the members,
and exits 2.

## Plugins

A plugin has a name in its namespace, and operators are bound to it:

| Verb | What it does |
|---|---|
| `flz plugin enable <name> <provider> [--single]` | enables the plugin; with `--operator_id` it binds that operator in the same command |
| `flz plugin bind <name> --operator_id <op>` | binds an operator, with its own `--hook` and `--channel` |
| `flz plugin unbind <name> --operator_id <op>` | takes the binding away; the plugin stays enabled |
| `flz plugin update <name>` | changes the plugin's options, in place when they are live |
| `flz plugin disable <name>` | stops the plugin for every operator bound to it |
| `flz plugin list` | the plugins enabled in the namespace, their operators and controllers |

[`sources_and_sinks.md`](sources_and_sinks.md) walks through them.

TODO: not written yet.
