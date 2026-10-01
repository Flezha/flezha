# Calls

For operator authors and for the applications that call operators. An **outside call** sends
one input to a running operator and waits for one answer: the output a `response()` end of the
operator's graph produced from that input, or one coded error saying why none came. The caller
is outside Flezha: `flz operator call`, application code through the published
`Flezha.Service.Operator.call/6`, or a plugin feeding the operator it is enabled on. An
**operator call** is one operator calling another from its graph with a `call(...)` end; its
outcome comes back as a [`Response`](#operator-calls-call) input, and nothing waits for it.
Both kinds of operator answer calls and make them, flow and steps. This page is the
contract: how a graph answers, where the answer goes, when a call ends, and who may make one.

## Contract

| | |
|---|---|
| The operator declares | a `response()` end on each channel whose outputs answer; tasklets are unchanged |
| The caller gives | one input and a deadline in milliseconds |
| The caller gets | the first answer that input caused, with the name of the channel that produced it, or a coded error |
| The deadline | measured by the operator's Overwatch; the input stays accepted when it passes |
| Durability | the operator's own tier: an answer leaves exactly where an output would |
| A caller that is gone | its answer is dropped and counted, never redelivered |
| Authorization | judged and audited as `operator.send` |

## Declaring the answer: `response()`

`response()` ends a channel on the answer to the call its input carried. It is a plugin end,
like a sink, and takes the sink's serializer arguments; it takes no name, because nothing
outside the operator addresses a response end by name, so the channel keeps the name it would
have without it. As after a sink, consumers may follow it on the same handle.

| SDK | Spelling |
| --- | --- |
| Rust | `.response()`, or `.response_with(serializer)` |
| Java | `.response()`, or `.response(Function<T, byte[]>)` |
| Node | `.response()`, `.response(encode)` or `.response({ codec })` |
| Python | `.response()`, with `encode=` or `codec=` |
| Ruby | `.response`, with `encode:` or `codec:` |
| PHP | `->response(?callable $encode, ?ChannelCodec $codec)` |
| Elixir | `response/1`, or `response_with/2` with a serializer |

A value is serialized as the SDK's sink serializes it: the SDK pages describe the codecs.

- **One plugin end per channel.** A `sink` and a `response()` on one channel, or two
  `response()` on one channel, are refused when the operator is declared, as
  `topology.channel_ends_twice`. Where one tasklet must both answer and record, a pass-through
  tasklet gives the second end its own channel.
- **Several response channels per graph.** An accepted path and a refused path may each end in
  `response()`; whichever answers first wins.
- **No key needed.** Under the effectively-once tier a sink's channel must be `.keyed()`, so a
  sink can drop a duplicate by its key. A response channel need not be: an answer is identified
  by its call.
- **The short form has no `response` option.** An operator that answers is written in the full
  form.

### A small example

The operator answers each call with its input. In Rust:

```rust
use flezha_runner::{tasklet, ChannelBuilder, Ctx, Tasklet, TaskletError, Operator};

#[derive(Default)]
pub struct Echo;

#[tasklet(name = "call_echo")]
impl Tasklet for Echo {
    type Input = String;
    type Output = String;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input)
    }
}

pub struct CallEcho;

impl Operator for CallEcho {
    fn factory_id() -> &'static str {
        "call_echo"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<Echo>().response();
    }
}
```

The same operator in Elixir:

```elixir
defmodule CallEcho do
  use Flezha.SDK.Operator, factory_id: "call_echo"

  @impl true
  def channels(c), do: c |> input("in") |> into(Echo) |> response()
end

defmodule Echo do
  use Flezha.SDK.Tasklet, name: "call_echo"

  @impl true
  def run(input, state, ctx) do
    emit(ctx, input)
    {:ok, state}
  end
end
```

With a runner registering it, start it and call it:

```
$ flz operator start call_echo --id echo
Started echo
$ flz operator call echo --text hello
hello
```

The answer may come from any tasklet downstream of the input, not only the one that received
it: in `input("in") |> into(Parse) |> into(Price) |> response()` the value `Price` emits
answers the call. In a steps operator the answer comes from the pass the call's input opened.

## The call token

The controller ties an input to the outputs it causes with a token in the input's metadata,
`flz.reply_to`. It mints one for every outside call, `w:` and 16 random bytes, after removing
any `flz.reply_to` the caller's metadata carried. Every other way into an operator drops the
key, and the ingest path puts back the token the input arrived with after the `:on_input`
chain has run, so no hook, plugin or sender can add, drop or change one. The token is
journaled with the input wherever the operator journals input, so a sleep, a move to another
runner and a crash re-forward deliver it again.

The runner core carries the token from the input to every value derived from it, beside the
input-log offset it already carries, and stamps it on each output of a response end. A tasklet
never sees it, and a timer firing carries none. `flz operator listen --input` shows it on the
arriving input; there it is of no use, because an answer only ever goes to the waiting caller
the controller holds for it.

## Where an answer goes

An answer leaves the runner exactly where an output of the same operator leaves, so a call never
answers before the state change its turn made is durable where the tier promises durability:

| Operator | The answer leaves |
| --- | --- |
| Off the effectively-once tier | at settle, with no durability wait |
| Under the effectively-once tier | after the effect record that carries it is synced to disk |
| A durable steps operator | when the stage that produced it commits (for a one-tasklet turn, the iteration's commit) |
| A steps operator that journals nothing | at settle |

The Overwatch takes every answer off the output path before the `:on_output` chain runs: no
plugin, no `flz operator listen` and no sink sees one. Then:

- the first answer for a waiting call is handed to its caller and ends the call;
- a later answer for the same call is dropped and counted with the reason `answered` (after the
  deadline, `unclaimed`);
- an output on a response end that nobody waits for (its input came through `flz operator
  send`, a plugin or a forward, or its caller already timed out) is dropped and counted with the
  reason `unclaimed`.

## The deadline

Every call carries a positive deadline in milliseconds, measured by the callee's Overwatch from
the moment the call reached it. With no answer by then, the caller receives `call.timeout`. The
input stays accepted: its turn still runs, and its answer, if any, is dropped as `unclaimed`. A
slow turn that costs something, such as a paid request to an outside service, still costs it
after its caller has given up.

A call to a sleeping instance is journaled, the append wakes it, and the time the wake takes
counts against the deadline.

## When a call ends early

A call is answered at once, with a code, whenever its answer can no longer come or the call
cannot be taken:

- **The Overwatch moves.** Waiting callers move with a handover to another node and are still
  answered there, or time out at their original deadline.
- **The Overwatch crashes.** Every caller waiting on it receives `call.unavailable` at once, not
  at its deadline. The journaled input still runs.
- **The input is dead-lettered.** Its caller receives `call.failed` once the dead-letter entry is
  durable (for a failed turn of a durable `per_input` `keep_progress` operator, once the turn's
  record is), carrying the entry's reference and the error text. The input is kept in the
  DLQ (`flz dlq list`) and a replay can still succeed; its answer then reaches nobody.
- **The instance ends.** When a run completes, fails or is stopped, every waiting caller
  receives `call.callee_ended` with the run's terminal status: `ok` (a steps run),
  `completed` (a flow instance's `complete`), `error` or `cancelled`. An operator caller of an instance stopped without ending that
  keeps a durable input log is the exception: its call stays journaled and is answered when the
  instance runs again, or by the caller's deadline.

### Error codes

Every refusal is in the [error model](../errors.md); the exit code is `flz`'s for the class.

| Code | Class (exit) | When |
| --- | --- | --- |
| `operator.not_found` | not_found (5) | no operator or member of that id in the namespace |
| `call.timeout` | unavailable (7) | the deadline passed with no answer; `details.deadline_ms` |
| `call.failed` | unavailable (7) | the operator dead-lettered the input; `details.dlq_ref`, `details.error` |
| `call.callee_ended` | unavailable (7) | the instance ended before it answered; `details.status` |
| `call.absorbed` | conflict (6) | the input was absorbed as a duplicate, or an `:on_input` hook filtered it out |
| `call.not_accepted` | conflict (6) | a `once` or `chained` steps instance has already taken its input; nothing is journaled |
| `call.busy` | unavailable (7) | the operator already owes `call_pending_max` answers; nothing is ingested |
| `call.unavailable` | unavailable (7) | the operator is parked or not ready, its durable append was refused, or its Overwatch went away |
| `call.target_unkeyed` | invalid (2) | a partition group was called with no key; `details.members` |
| `operator.factory_mismatch` | conflict (6) | an operator's factory call named an instance of another factory; nothing is delivered |
| `topology.channel_ends_twice` | invalid (2) | a channel declares two plugin ends (raised when the operator is declared) |
| `topology.call_*` | invalid (2) | a `call(...)` end the runtime cannot honour ([operator calls](#operator-calls-call)) |

A refusal by authorization keeps its own `authz.*` code and exits 4.

## The three callers

### `flz operator call`

```
flz operator call <operator_id> --text <payload> [--channel <ch>] [--key <k>] [--timeout <ms>] [--namespace <ns>]
```

It prints the answer's payload and a newline on stdout and exits 0. A failure is written on
stderr and exits by its class, as every verb's does ([`../guides/cli.md`](../guides/cli.md)):

```
$ flz operator call slow --text x --timeout 100
error: operator 'slow' did not answer within 100 ms; the input stays accepted and its turn still runs (call.timeout)
```

`--timeout` defaults to 30000 and has no short spelling, because `-t` is `--text`.

### `Flezha.Service.Operator.call/6`

```elixir
Flezha.Service.Operator.call(namespace, operator_id, message, channel, opts, caller_context)
```

`channel` is the entry channel, `nil` or `""` for none. `opts` carries `deadline_ms` (required,
a positive integer) and `key`. It answers `{:ok, payload, %{"channel" => channel}}`, naming the
channel that produced the answer, or `{:error, %Flezha.Error{}}`. Its own wait lasts the deadline
plus `call_grace_ms`, so the operator's `call.timeout` always reaches the caller first; a crash
of the Overwatch it waits on is `call.unavailable`.

A connected runner reaches it through the [administration door](admin.md), like every published
function. That door is for application code outside operators: a tasklet never makes a call
from `run`. One operator calls another from its graph ([operator calls](#operator-calls-call)).

### A feeding plugin

```elixir
Flezha.Plugin.Host.inject(overwatch, message, metadata, :call, deadline_ms)
```

A plugin calls the operator it is enabled on and receives what `Operator.call/6` answers. Its
wait is bounded the same way, and a host it cannot reach answers `call.unavailable`. The
`flz.reply_to` in `metadata`, if any, is dropped: the answer reaches this call and no other.

### A partition group

A partition group (`partitioned_operators.md`) is called by key: `--key EURUSD` on the CLI, or
`key: "EURUSD"` in `opts`, reaches the member whose key-group range owns the key, by the
resolution `flz state get` uses. A group called with no key is refused as
`call.target_unkeyed`, naming its members; a member named directly (`prices#p0`) is called
directly. `flz operator send` takes `--key` the same way.

### Authorization

A call through `flz operator call` or `Operator.call/6` is judged as `operator.send` in the
operator's namespace and audited like a send, naming the operator, with the error code on a
failure. A refused call delivers no input. A plugin's call on its own operator is that
operator's own input and is not judged again. See
[`../operations/authn_authz.md`](../operations/authn_authz.md).

## Operator calls: `call(...)`

One operator calls another by ending a chain of its graph in `call(...)`. Every value that
reaches the end, after the chain's transforms and key, is one request; the call waits for
nothing, and its outcome comes back later as an ordinary input, a **`Response`**, on the
operator's **Response channel**. The tasklet that made the call may read it, and so may any
other: wiring the Response channel back into the calling tasklet is fan-in, not a cycle.

```elixir
c |> input("quotes") |> into(Orders.Price) |> call("pricing", deadline_ms: 300)
c |> input("in") |> into(Route) |> key_by(& &1.session) |> call(factory: "agent", deadline_ms: 120_000)
c |> input("in") |> into(Route) |> call(factory: "triage", deadline_ms: 60_000)
```

### Where a call goes

| Target | Reaches |
| --- | --- |
| `call("pricing", ...)` | the instance `pricing`, or, when `pricing` is a partition group, the member whose key-group range owns the request's key (from a `key_by` before the end) |
| `key_by(...) \|> call(factory: "agent", ...)` | the instance of `agent` whose id is the request's key, started with the request as its first input when no instance holds that id |
| `call(factory: "triage", ...)` | a new steps instance of `triage` per call, under a generated id |

The request enters the callee on the entry channel `channel:` names, or on its first declared
entry channel. A target is always in the caller's namespace. Creating and delivering go through
the controller's one spawn-or-deliver, so two calls racing for one absent id start one
instance and deliver the other request to it. A factory call reaches only an instance of that
factory: a key naming a running instance of another factory is answered
`operator.factory_mismatch` and nothing is delivered, so a key that came from a client cannot
steer a request into an unrelated operator. With `create: false` an absent id is answered
`operator.not_found` and nothing starts; a gateway uses it for the turns of a session that
may have ended, so an expired session is refused rather than silently started again. A factory
call without a key cannot say `create: false`, and one to a flow factory is refused
`operator.factory_not_steps`, since nothing would ever end the instances it started. A key
may start a flow instance: it runs until it is stopped, and sleeps when idle
as its class's `sleep_after` says.

| SDK | Spelling |
| --- | --- |
| Rust | `.call::<Answer>(CallTarget::id("pricing"), 500)`, `CallTarget::factory("agent").create(false).channel("turn")` |
| Java | `.call(CallTarget.id("pricing"), Answer.class, 500)`, `CallTarget.factory("agent").create(false).channel("turn")` |
| Node | `.call("pricing", { deadlineMs: 500, codec })`, `.call({ factory: "agent", create: false, channel: "turn" }, { deadlineMs })` |
| Python | `.call("pricing", deadline_ms=500)`, `.call(factory="agent", deadline_ms=..., create=False, channel="turn", codec=...)` |
| Ruby | `.call("pricing", deadline_ms: 500)`, `.call(factory: "agent", deadline_ms: ..., create: false, channel: "turn", codec: ...)` |
| PHP | `->call(target: 'pricing', deadlineMs: 500)`, `->call(factory: 'agent', deadlineMs: ..., create: false, channel: 'turn', codec: ...)` |
| Elixir | `call(s, "pricing", deadline_ms: 500)`, `call(s, factory: "agent", deadline_ms: ..., create: false, channel: "turn", serialize: f, decode: g)` |

The request is written as a `sink` writes, the answer read as an `input` reads: the answer type in
Rust and Java, one named codec for both directions in Node, Python, Ruby and PHP (JSON by
default), and optional `serialize:` and `decode:` functions in Elixir (the default sink rule and
the raw binary; the Elixir `Response` carries the request as the bytes that were sent).

`call(...)` is a plugin end: a channel ends in one sink, one `response()` or one call
(`topology.channel_ends_twice`). Registration also refuses a call whose deadline is zero
(`topology.call_deadline_zero`), a factory call with `create: false` and no key
(`topology.call_without_key`), a Response channel that feeds no tasklet
(`topology.call_response_unread`: every outcome would be journaled and read by nobody; forward
with `operator_forward` when no outcome is wanted), and a call in a `:once` or `:chained`
steps class (`topology.call_outside_per_input_loop`), which never reads the later message a
`Response` is.

### The `Response`

The Response channel is an entry channel named after the calling channel with `.response`
(`gateway.response`). Each `Response` carries the request as the call end wrote it, the result
(the answer, or the error that ended the call in the SDK's one error type, a timeout being
`call.timeout`), `target` (the instance that answered, a generated one included) and `created`
(whether this call started it), so a gateway tells a new session from one that expired.

| SDK | Type |
| --- | --- |
| Rust | `flezha_runner::Response<Req, Ans>`: `request`, `result: Result<Ans, FlezhaError>`, `target`, `created` |
| Java | `Response<Req, Ans>`: `request()`, `answer()`, `error()`, `ok()`, `target()`, `created()` |
| Node | `Response<Req, Ans>`: `request`, `value` or `error`, `target`, `created` |
| Python | `Response`: `request`, `value` or `error`, `target`, `created` |
| Ruby | `Flezha::Response`: `request`, `value` or `error`, `ok?`, `target`, `created` |
| PHP | `Flezha\Response`: `request`, `value` or `error`, `ok()`, `target`, `created` |
| Elixir | `%Flezha.SDK.Response{request, result: {:ok, value} \| {:error, error}, target, created}` |

On the wire a `Response` is one JSON document the controller writes, the request and the answer
in base64: `{"call", "target", "created", "request", "value" | "error"}`, `error` being the
error model's own document. `test/fixtures/call_response_envelope.json` holds the cases every
SDK decodes identically.

### Guarantees

A call and its `Response` are ordinary inputs, and each side keeps the guarantee of its own
tier: the request is journaled in the callee's input log when the callee journals input, the
`Response` in the caller's when the caller does.

| Caller and callee | A call |
| --- | --- |
| Both keep a durable input log (an effectively-once flow operator, a durable steps one) | survives a crash of either side's runner, Overwatch or controller, runs its turn at most once to its commit, and ends with exactly one `Response`; a request still on its way when only the controller's call delivery crashes is not sent again, and that call ends in `call.timeout` at its deadline |
| Either keeps none | is best effort: a lost request or answer ends in `call.timeout` at the deadline while the caller's Overwatch lives, and a caller off the tier that loses its Overwatch loses its pending calls |

For a durable pair: the call output stays held in the caller's outbox until its `Response` is
durable in the caller's input log, so the caller's recovery dispatches every call without one
again. The request carries the call id as its `flz.msg_id`, so the callee absorbs a request sent
again while the first is retained, and each record carrying a live call is retained until the
call's deadline plus `call_retention_margin_ms` has passed. A recovered call whose `Response`
the caller's log holds is settled with nothing sent; one whose deadline is ahead is sent again
and waits; one past its deadline, within the margin, is answered `call.timeout`. **The one
corner:** a call recovered past its deadline plus the margin (the caller's side down that long)
is settled with no `Response`, because one may have been appended and truncated since, and
counted as `[:flezha, :call, :recovered_unanswered]`. There the guarantee is at most one
`Response`. A caller whose Overwatch restarted alone and gets an answer before its outbox
dispatched the call again has its Cells dispatch their held calls at once, and takes the answer
when its delivery retries. The callee holds its answer in its own outbox until the caller's
Overwatch took it, retrying until then or until the deadline plus the margin, when it drops it
as `caller_gone`; a caller no longer declared in its namespace is dropped at once. An answer
whose delivery was lost while held is dispatched again by the callee's outbox.
Clocks are assumed synchronized to well within the margin.

The deadline is the caller's alone: a wall-clock time taken when the calling invocation emitted
the request. The time an asleep callee takes to wake counts against it. When it passes, the
caller receives `call.timeout` and the callee's turn still runs; its answer is dropped and
counted.

### Order and bounds

- **Requests keep their order** from one caller to one target, so a session's turns run in the
  order they were called. `Response`s arrive in the order their appends complete, with no order
  across calls; a caller that needs one restores it from the request each `Response` carries.
- **`call_pending_max` bounds the callee**: the answers one operator owes the callers waiting on
  it, outside callers and operators alike. An operator call past it is answered `call.busy` and
  nothing is ingested.
- **An operator's own calls never count against it**, its own or a callee's. A durable caller's
  pending calls are bounded per Cell by its outbox window (`outbox_inflight_max`, 1 000
  entries): each is a held entry, so a Cell whose window is full of pending calls dispatches its
  later outputs only as calls end. That is queueing, never loss. A caller off the tier keeps no
  outbox; its calls are bounded by their deadlines.

### A steps caller

A `:per_input` steps operator calls without suspending: the request leaves when the stage that
emitted it commits, and the `Response` arrives on the Response channel as a later message, run as
a later iteration. An agent session calls a tool operator this way. An instance that ends with
calls pending abandons them; their outcomes are dropped and counted as `unclaimed`.

### When a call ends early

Every error an outside caller can receive reaches an operator caller as a `Response` carrying
it: `call.failed` when the callee dead-letters the request's input (a `:per_input`
`keep_progress` session answers a failed turn `call.failed` and goes on, the calls queued behind
it answered by their own turns), `call.callee_ended` for each call a callee took and will never
run because it ended, `call.not_accepted` from a `:once` or `:chained` instance that has taken
its input, and the controller's own codes for a creation or a delivery it refuses
(`quota.exceeded`, `operator.factory_unknown`, `operator.not_found`, `operator.factory_mismatch`,
an `authz.*` code). A rewind of the callee neither answers a call again nor takes an answer back;
a turn it interrupts before its answer left leaves its caller to the deadline.

### Sessions and quotas

A session a call creates counts against its namespace's `max_operators` from its start to its
teardown, asleep or awake; a creation past it is answered `quota.exceeded` and calls to running
sessions are still delivered. A session that ended (it completed, or expired itself on a cell
timer and `ctx.complete`) no longer holds its id: a call creates a new empty instance and says
`created: true`, or with `create: false` is answered `operator.not_found` before any turn runs.

### Authorization

A call is judged as its namespace's runner identity, as a tasklet's `ctx.spawn_steps` is:
creating the callee against `operator.start` (audited as a start naming the new instance),
delivering the request against `operator.send` (no record of its own). The runner identity needs
the `spawner` or `app` role in the namespace; a refusal ends the call with its `authz.*` code and
starts and delivers nothing. The `Response`'s way back is the caller's own call and is not
judged again.

## Limits

| Setting | Default | |
| --- | --- | --- |
| `call_pending_max` (`FLEZHA_CALL_PENDING_MAX`) | 10 000 | the answers one operator owes the callers waiting on it, outside callers and operators alike; a call past it is refused `call.busy` before anything is ingested |
| `call_grace_ms` | 1 000 | how long an outside caller waits past its deadline, so the operator's own `call.timeout` reaches it first |
| `call_retention_margin_ms` (`FLEZHA_CALL_RETENTION_MARGIN_MS`) | 300 000 | how long an input log keeps a record carrying an operator's call past the call's deadline |
| `call_worker_idle_ms` | 30 000 | how long an operator call's delivery worker (one per caller and target) waits with an empty queue before it stops |

All are in [`../configuration.md`](../configuration.md). A long deadline makes the callee keep that
much input: the `input_log_frozen_watermark` signal reports a log that grows.

## Observing calls

The controller emits `[:flezha, :call, :answered]` with the answer's latency,
`[:flezha, :call, :timeout]`, `[:flezha, :call, :answer_dropped]` with its reason
(`answered`, `unclaimed`, or `caller_gone` for an operator caller the callee's answer could not
reach by the deadline plus the margin, or that its namespace no longer declares) and `[:flezha, :call, :refused]` with its code, each
naming the namespace and the operator; an operator caller's `timeout` and
`[:flezha, :call, :recovered_unanswered]` also name the call's `target`. The observability source
`flezha_call` exports them as the histogram `flezha_call_answer_duration` and the counters
`flezha_call_timeouts_total`, `flezha_call_answers_dropped_total`, `flezha_call_refused_total`
and `flezha_call_recovered_unanswered_total`. `flz operator info` shows
the number of callers waiting on an operator as `Waiting callers`.

## Measured latency

On one machine, a durable `per_input` steps operator on the Rust runner (debug build),
measured inside the controller: 14 to 32 ms for an awake instance, 29 to 48 ms for the first
call after it slept. Through `flz operator call` both are about 300 ms, most of it the start-up
of the `flzvm` script the CLI runs.

An operator call to an asleep session (`call_gateway` to `call_agent`, the session parked after
700 ms idle), from the input reaching the gateway to its `Response`'s outcome reaching the
gateway's sink, in `test/system/operator_call_e2e_test.exs` on the same machine with debug
builds, one sample each: Rust 67 ms, Java 84 ms, Node 74 ms, Python 60 ms; Elixir, Ruby and PHP
under 102 ms (sampled at a 100 ms poll). The wake is inside that time, and counts against the
call's deadline.

## See also

- `../glossary.md`: call, call token, response end, Response, Response channel
- `tasklet_requested_spawn.md`: the spawn-or-deliver an operator call shares
- `partitioned_operators.md`: key groups and members
- [`../guides/advanced/effectively_once.md`](../guides/advanced/effectively_once.md): the tier an
  answer can wait for
- [`../errors.md`](../errors.md): every code
