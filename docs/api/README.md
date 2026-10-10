# The controller's consumer API

A program that is not a terminal reaches this controller through the **published
consumer surface**: `Flezha` and the `Flezha.Service.*` modules named in
[`consumers/published.txt`](consumers/published.txt).

Every function on it obeys three rules:

1. It takes a `Flezha.Service.CallerContext` — `%{source: …, principal: …}` — as
   its **last** argument.
2. It authorizes through that context before it does anything.
3. It answers **data**, not text formatted for a terminal. A failure is
   `{:error, %Flezha.Error{}}`, whose `code` is one of [the error codes](../errors.md).

The exceptions are two, and only two. A consumer verifies a token and reads the
enforcement posture *before* it holds a context, because their answers are what a
context is built from; they cannot be authorized by the thing they bootstrap.
Both are read-only. `consumers/published.txt` names them.

## How a function is governed

Every published function binds one row of `Flezha.AuthZ.Verbs`, and
`Flezha.AuthZ.Verb.run/5` applies it in one order: identity, scope, policy, act,
audit. No surface binds a row of its own: `flz`, a runner's `AdminRequest` and
the HTTP API ([`docs/plugins/http_api.md`](../plugins/http_api.md)) reach the
function through one door, `Flezha.Service.Admin`, so one request is judged once,
by the function's own row. The row names the action the
policy judges, the rule that resolves its namespace, and the audit it leaves;
`docs/topics/core/authorization.md` carries the whole table. A published function
that took a caller context and bound no row would fail
`test/flezha/authz/verbs_test.exs`, so there is no ungoverned function on this
surface: a read the policy calls public (`run_known?/2`, `parse_cron/2`, the
partition arithmetic) binds a `:public` row and is served to any caller.

The namespace a mutation is judged in is **resolved by the callee** from what is
being acted on — a dead-letter entry's own namespace, a plugin's own namespace —
never taken from the context. A consumer cannot choose the scope its own request
is judged in.

## The catalogue

Where a function binds its row it also describes itself: its arguments before the
context (`args:`, in the vocabulary plugin options use), whether it streams
(`stream: true`), and the first sentence of its `@doc`. Its row says whether it may
be reached over the network (`remote`): a runner connection and the HTTP API reach
neither an operation that is not `remote` nor one that streams. `Flezha.Service.catalogue/0` builds the
catalogue from those declarations; the door resolves an operation's name
(`"Dlq.list"`) through it and converts the JSON arguments by its rows.
`mix flezha.catalogue` writes the copy `flz` embeds, `cli/catalogue.json`, which
`flz help --json` prints and `test/flezha/api/catalogue_test.exs` holds to the code.

A provider's options are described in the same vocabulary: `Fleet.provider/2`
answers `options`, one entry per option a user can write, read from the provider's
rows by `Flezha.Plugin.ProviderBehaviour.Options.describe_row/1`, the function that
also describes an operation's arguments here. A console renders an operation's
argument and a plugin's option with one form.

## Receiving a stream on another node

Four operations stream: `Relay.listen/5`, `Observability.tail/2`, `AuditLog.tail/2` and
`Controller.stop/2`. Each hands its events to the context's `:emit`. A consumer on another
node puts a pid of its own there and makes the call with `:erpc.send_request/4`, so the call
runs in an `:erpc` worker on the controller and nothing of the consumer's runs here:

```elixir
request = :erpc.send_request(controller, Flezha.Service.Observability, :tail,
  [:flezha_runner, Map.put(context, :emit, self())])
# each signal arrives as {:flezha_emit, signal}; the reply arrives when the tail ends
```

The operation watches that pid and ends when it exits or its node disconnects, releasing its
subscription or listener, so stopping the receiving process is how a consumer lets go. The two
tails also end when the context's optional `window_ms` runs out (the audit tail, too, when the
writer closes), answering `{:ok, :caller_down | :done | :window_closed}`, and they hand on only
what `query/2` or `read/2` would answer the same caller. A listen sends `:attached` first, then
each listener event; a listen with the direction `calls` sends `{:call, request, md}` for each
call the relay takes and then one of `{:answered, answer, md}`, `{:timed_out, error, md}` or
`{:refused, error, md}`, paired by `md["flz.call_id"]`. Runner sessions arrive this way as the `runner_connected` and
`runner_disconnected` signals of the `:flezha_runner` source, whose metadata names the
`namespace` and the `runner`. A signal's `ts` is the controller's monotonic clock in
microseconds; `Controller.status/1` answers the same clock as `monotonic_us`, so a consumer ages
a signal against it rather than its own clock.

## Why a consumer's calls are recorded here

A consumer reaches this controller over `:erpc` on a live node. There is no
shared build and no compile-time link, so a rename here compiles clean on both
sides and fails only on a user's machine.

[`consumers/flezha_ui.txt`](consumers/flezha_ui.txt) therefore records every
controller function the console calls, generated by
[`scripts/consumer_surface.py`](../../scripts/consumer_surface.py) from a
consumer checkout:

```
python3 scripts/consumer_surface.py /path/to/flezha_ui --out docs/api/consumers/flezha_ui.txt
```

`test/flezha/api/consumer_surface_test.exs` resolves every line of both files
against this tree's real exports on every run. An unresolved line is reported in
one of three classes, because the remedy differs:

- **moved** — some module here exports the same name and arity; the candidates
  are named, the one sharing the longest namespace first.
- **privatised** — the name survives only as a private function. It must be
  published, not re-pointed.
- **withdrawn** — the name appears nowhere. There is nothing to point at.

The break is caused in this repository, so it is reported here, in the run that
causes it. Where `FLEZHA_UI_PATH` names a consumer checkout the manifest is
regenerated from it and any difference in the call set fails the run, so a
vendored manifest cannot quietly describe a consumer that has moved on.

## The stability this buys

A published name is retired only together with the manifest edit that stops
naming it: a rename, an arity change or a withdrawal lands in one change with its
consumers. Internal module and function names stay free to move — that freedom is
what the published surface exists to protect.
