# Calls

For operator authors and for the applications that call operators. An **outside call** sends
one input to a running operator and waits for one answer: the output a `response()` end of the
operator's graph produced from that input, or one coded error saying why none came. The caller
is outside Flezha: `flz operator call`, application code through the published
`Flezha.Service.Operator.call/6`, or a plugin feeding the operator it is enabled on. Both kinds
of operator answer calls, unbounded and bounded. This page is the contract: how a graph
answers, where the answer goes, when a call ends, and who may make one.

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
use flezha_runner::{tasklet, ChannelBuilder, Ctx, Tasklet, TaskletError, UnboundedOperator};

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

impl UnboundedOperator for CallEcho {
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
  use Flezha.SDK.UnboundedOperator, factory_id: "call_echo"

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
answers the call. In a bounded operator the answer comes from the pass the call's input opened.

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
| A durable bounded operator | when the stage that produced it commits (for a one-tasklet turn, the iteration's commit) |
| A bounded operator that journals nothing | at settle |

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
- **The instance ends.** When a bounded run completes, fails or is stopped, or either kind is
  stopped, every waiting caller receives `call.callee_ended` with the run's terminal status:
  `ok`, `error` or `cancelled`.

### Error codes

Every refusal is in the [error model](../errors.md); the exit code is `flz`'s for the class.

| Code | Class (exit) | When |
| --- | --- | --- |
| `operator.not_found` | not_found (5) | no operator or member of that id in the namespace |
| `call.timeout` | unavailable (7) | the deadline passed with no answer; `details.deadline_ms` |
| `call.failed` | unavailable (7) | the operator dead-lettered the input; `details.dlq_ref`, `details.error` |
| `call.callee_ended` | unavailable (7) | the instance ended before it answered; `details.status` |
| `call.absorbed` | conflict (6) | the input was absorbed as a duplicate, or an `:on_input` hook filtered it out |
| `call.not_accepted` | conflict (6) | a `once` or `chained` bounded instance has already taken its input; nothing is journaled |
| `call.busy` | unavailable (7) | the operator already owes `call_pending_max` answers; nothing is ingested |
| `call.unavailable` | unavailable (7) | the operator is parked or not ready, its durable append was refused, or its Overwatch went away |
| `call.target_unkeyed` | invalid (2) | a partition group was called with no key; `details.members` |
| `topology.channel_ends_twice` | invalid (2) | a channel declares two plugin ends (raised when the operator is declared) |

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
from `run`. Calling one operator from another belongs in the graph, and is not part of this
surface.

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

## Limits

| Setting | Default | |
| --- | --- | --- |
| `call_pending_max` (`FLEZHA_CALL_PENDING_MAX`) | 10 000 | the answers one operator owes the callers waiting on it; a call past it is refused `call.busy` before anything is ingested |
| `call_grace_ms` | 1 000 | how long an outside caller waits past its deadline, so the operator's own `call.timeout` reaches it first |

Both are in [`../configuration.md`](../configuration.md).

## Observing calls

The controller emits `[:flezha, :call, :answered]` with the answer's latency,
`[:flezha, :call, :timeout]`, `[:flezha, :call, :answer_dropped]` with its reason
(`answered` or `unclaimed`) and `[:flezha, :call, :refused]` with its code, each naming the
namespace and the operator. The observability source `flezha_call` exports them as the histogram
`flezha_call_answer_duration` and the counters `flezha_call_timeouts_total`,
`flezha_call_answers_dropped_total` and `flezha_call_refused_total`. `flz operator info` shows
the number of callers waiting on an operator as `Waiting callers`.

## Measured latency

On one machine, a durable `per_input` bounded operator on the Rust runner (debug build),
measured inside the controller: 14 to 32 ms for an awake instance, 29 to 48 ms for the first
call after it slept. Through `flz operator call` both are about 300 ms, most of it the start-up
of the `flzvm` script the CLI runs.

## See also

- `../glossary.md`: call, call token, response end
- `partitioned_operators.md`: key groups and members
- [`../guides/advanced/effectively_once.md`](../guides/advanced/effectively_once.md): the tier an
  answer can wait for
- [`../errors.md`](../errors.md): every code
