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

TODO: not written yet.
