# CLI

Part 2.

`flz` is the one tool a controller is operated with, and nearly every command
in these guides is one of its verbs. Three things about it catch people out,
which is what earns it a page. It reaches a controller by running the `flzvm`
script beside it, so it exists only inside a release and only talks to the
controller it sits next to, which is why the Docker form is
`docker exec <container> /flezha/bin/flz`. Every verb that renders takes
`--json`, and a format that is neither that nor text is refused rather than
printed anyway. And identity for a gated verb rides `--token` or
`FLEZHA_TOKEN`, which is what [`authorization.md`](authorization.md) turns on.

TODO: not written yet.
