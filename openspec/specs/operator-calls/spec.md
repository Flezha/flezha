# operator-calls Specification

## Purpose

An outside caller (the CLI, the published service API, or a plugin feeding its own operator)
sends one input to an operator of either kind and receives the one output a `response()` end
produced from it, or one coded error saying why none came. This capability defines the token
that ties the input to its outputs, where an answer leaves per durability tier, the waiting
table the callee's Overwatch holds, and every way a call ends.

## Requirements
### Requirement: An outside call SHALL carry a token only the controller writes

An input an outside caller sends SHALL enter its operator through the Overwatch's call entry
point, whichever caller sent it (the CLI's `flz operator call`, the published
`Flezha.Service.Operator.call/6`, or a plugin's `Flezha.Plugin.Host.inject/5` in `:call` mode).
For an outside call the controller SHALL mint the token: that entry point SHALL remove any
`flz.reply_to` the caller's metadata carries, mint an opaque, unguessable token of at most 255
bytes and put it on the input's metadata as `flz.reply_to`. Every other entry point SHALL remove
`flz.reply_to` from the metadata it is given. The operator form of a token, which
`call-an-operator-and-get-its-response` adds (`o:` followed by the caller and the call id), SHALL
be accepted only from the controller's own call-delivery path (the delivery worker's message to
the call entry point) and from the seed option that path sets on `spawn_or_deliver`, and SHALL
NEVER be taken from an input's or a plugin's metadata, which always loses its `flz.reply_to`. The
ingest path SHALL put back, after the `:on_input` chain has run, the token the input arrived
with, so that no hook can add, remove or change one. The token SHALL be journaled with the input wherever the operator
journals input, so a sleep, a rebind's tail re-forward and a crash re-forward deliver it again.
This SHALL hold for an unbounded and a bounded operator alike.

#### Scenario: A plugin cannot forge a token

- **WHEN** a feeding plugin injects a payload in `:confirming` mode whose metadata carries `flz.reply_to`
- **THEN** the input SHALL reach the operator without `flz.reply_to`, and no output it causes SHALL be taken for an answer

#### Scenario: A plugin's call cannot choose its token

- **WHEN** a feeding plugin calls `Host.inject(overwatch, payload, %{"flz.reply_to" => "o:gateway:c1"}, :call, 5_000)`
- **THEN** the input SHALL carry only the `flz.reply_to` the call entry point minted, and its answer SHALL reach that plugin's call and no other caller or operator

#### Scenario: A hook cannot strip a token

- **WHEN** an `:on_input` hook returns the input with metadata rebuilt without `flz.reply_to`
- **THEN** the input SHALL still carry the token the call gave it when it is journaled and forwarded

#### Scenario: A sleeping bounded operator's call keeps its token

- **WHEN** an outside caller calls a durable `:per_input` bounded operator that is asleep
- **THEN** the input SHALL be journaled with its token, the append SHALL wake the instance, and the turn's answer SHALL reach the caller

### Requirement: The runner core SHALL carry the token from an input to every output it causes

The runner core SHALL carry an input's token beside the input-log offset it already carries: for
an unbounded operator on every item the input becomes and on every value an inter-tasklet emit
derives from it, and for a bounded operator on the pass the input opens. A timer firing, a
chained loop's carry and an input without a token SHALL carry none. An inter-tasklet value a
restore delivers again SHALL be given the token of the input-log record its origin offset names.
The core SHALL do this for the six SDKs that share `flz_core`, and the Elixir SDK SHALL do the
same in its re-expression; no SDK's tasklet surface SHALL see the token.

#### Scenario: A downstream tasklet answers

- **WHEN** an unbounded operator declares `input("in") |> into(Parse) |> into(Price) |> response()` and an outside caller calls it
- **THEN** the value `Price` emits for that input SHALL answer the caller, although `Price` never received the input itself

#### Scenario: A bounded turn answers from its last stage

- **WHEN** a `:per_input` bounded operator declares two tasklets in a chain ending in `response()` and is called
- **THEN** the output of the second tasklet in the pass that call's input opened SHALL answer the caller

#### Scenario: A restored edge value keeps its call

- **WHEN** an effectively-once unbounded operator's runner dies after the first tasklet's record is durable and before the second tasklet answered, and the restore delivers the recorded edge value again
- **THEN** the second tasklet's output SHALL carry the call's token and SHALL answer the caller if it is still waiting

### Requirement: A `response()` end's output SHALL be its caller's answer and SHALL never reach a plugin

Every output of a channel that ends in `response()` SHALL carry `flz.reply_to`: the token of the
input that caused it, or an empty value when that input carried none. No output of any other end
SHALL carry `flz.reply_to`. The Overwatch SHALL take every output carrying `flz.reply_to` off both
output routes before the `:on_output` chain runs, so no plugin, no listener and no sink sees an
answer. An answer whose token names a waiting caller SHALL be handed to that caller with its
payload and the name of the channel that produced it. An answer whose token is empty or names no
waiting caller SHALL be dropped and counted with the reason `unclaimed`.

#### Scenario: The answer reaches the caller and nothing else

- **WHEN** an outside caller calls an unbounded operator whose tasklet's channel ends in `response()`, and a Kafka sink plugin is enabled on the operator
- **THEN** the caller SHALL receive the answer's payload, and the Kafka producer SHALL receive nothing

#### Scenario: An output nobody asked for is dropped and counted

- **WHEN** `flz operator send` delivers an input to an operator whose graph ends in `response()`
- **THEN** the output SHALL be dropped, SHALL be counted with the reason `unclaimed`, and SHALL reach no plugin

#### Scenario: A sink output carries no token

- **WHEN** a called operator's input causes an output on a channel that ends in `sink("audit")`
- **THEN** the frame the sink plugin receives SHALL carry no `flz.reply_to` key

### Requirement: An answer SHALL leave when its tier releases outputs

An answer SHALL leave the runner exactly where an output of the same operator leaves today: at
settle for an operator off the effectively-once tier and for a bounded operator that journals
nothing; after the effect record that carries it is durable under the effectively-once tier; and
when the stage that produced it commits for a durable bounded operator. No answer SHALL leave
before the state change its invocation made is durable where the operator's tier promises
durability.

#### Scenario: An effectively-once answer follows its record's sync

- **WHEN** an effectively-once unbounded operator answers a call from an invocation that changed its Cell's state
- **THEN** the answer SHALL reach the Overwatch only in the outbox dispatch that follows the effect record's fsync

#### Scenario: A durable bounded answer follows its stage's commit

- **WHEN** a durable `:per_input` bounded operator's turn emits on a `response()` end
- **THEN** the answer SHALL reach the caller only after the stage that emitted it has committed

#### Scenario: An answer off the tier leaves at once

- **WHEN** an unbounded operator on the default tier answers a call
- **THEN** the answer SHALL leave on the frame the core ships at settle, with no durability wait

### Requirement: The first answer SHALL win

A call SHALL be answered at most once. The first answer that reaches the Overwatch for a waiting
call SHALL be handed to the caller and SHALL end the call; every later answer carrying the same
token SHALL be dropped and counted, with the reason `answered` until the call's deadline and
`unclaimed` after it. A frame sent while a tasklet runs
(a part, as `send-a-part-while-it-runs` defines it) SHALL NOT answer a call and SHALL NOT end one.

#### Scenario: Two answers for one call

- **WHEN** a called operator's input fans out to two Cells whose channels both end in `response()`, and both emit
- **THEN** the caller SHALL receive the first answer to arrive, and the second SHALL be dropped and counted as `answered`

### Requirement: A call SHALL end at its deadline with `call.timeout`

Every outside call SHALL carry a positive deadline in milliseconds, measured by the callee's
Overwatch from the moment the call reached it. When no answer has arrived by then, the caller SHALL
be answered with the error `call.timeout` (class `unavailable`), whose details carry the deadline.
The input SHALL stay accepted: its turn SHALL still run, and its answer, if any, SHALL be dropped
and counted as `unclaimed`. The time an asleep instance takes to wake SHALL count against the
deadline.

#### Scenario: A slow turn times out and still runs

- **WHEN** a caller calls a bounded operator with a deadline of 100 ms and the turn takes 2 000 ms
- **THEN** the caller SHALL receive `call.timeout` after about 100 ms, the turn SHALL commit, and its answer SHALL be dropped and counted as `unclaimed`

### Requirement: A call that cannot be answered SHALL be answered at once with a code

A call SHALL be answered at once, in the error model of `give-every-error-a-code`, whenever its
answer can no longer come or the call cannot be taken. For an unbounded and a bounded operator
alike:

- no operator or member of that id in the namespace: `operator.not_found`;
- the operator parked, not ready, or its durable append refused: `call.unavailable`, the reason in
  details;
- the input absorbed as a duplicate or filtered out by an `:on_input` hook: `call.absorbed`;
- the operator already owing `call_pending_max` answers to the callers waiting on it (its own
  outbound calls do not count): `call.busy`, nothing ingested;
- a `:once` or `:chained` bounded instance that has already been given an input:
  `call.not_accepted`, nothing ingested;
- the input dead-lettered by the callee, by any park route that parks an input: `call.failed`,
  carrying the dead-letter reference and the error text, sent once the dead-letter entry is
  durable, except for a failed turn a durable `:per_input` `keep_progress` loop parks through its
  iteration commit (`keep-a-session-after-a-failed-turn`), whose `call.failed` SHALL be sent once
  that turn's `dead_lettered` record is durable;
- the instance ending, as a bounded run completes, fails or is stopped, or as either kind is
  stopped, while callers wait: `call.callee_ended` to each, carrying the run's status.

#### Scenario: An operator that is not running

- **WHEN** `flz operator call ghost --text x` is run and no operator `ghost` exists in the namespace
- **THEN** it SHALL fail at once with `operator.not_found` and exit 5

#### Scenario: A dead-lettered input on the default tier

- **WHEN** an unbounded operator off the tier exhausts its retries on a called input and parks it
- **THEN** the caller SHALL receive `call.failed` naming the dead-letter entry before its deadline

#### Scenario: A dead-lettered input under the tier

- **WHEN** an effectively-once unbounded operator parks a called input inside its effect record
- **THEN** the caller SHALL receive `call.failed` once the dead-letter entry is durable, the token read from the input-log record the entry's origin offset names

#### Scenario: A session's failed turn answers once its record is durable

- **GIVEN** a durable `:per_input` `keep_progress` bounded operator whose turn fails on `boom`, as `keep-a-session-after-a-failed-turn` defines
- **WHEN** an outside caller calls it with `boom`
- **THEN** the caller SHALL receive `call.failed` only after the turn's `dead_lettered` record is durable, and a later call SHALL be answered by the next turn

#### Scenario: A failed turn of a session that journals nothing answers its caller

- **GIVEN** a `:per_input` `keep_progress` bounded class with persistence off, whose turn fails on `boom`
- **WHEN** an outside caller calls it with `boom`
- **THEN** the turn's park frame SHALL carry the pass's token, and the caller SHALL receive `call.failed` once the dead-letter entry is durable

#### Scenario: A single-pass instance refuses a second call

- **WHEN** a `:once` bounded instance has taken its first input and a second call reaches it
- **THEN** the call SHALL be answered `call.not_accepted` and nothing SHALL be journaled

#### Scenario: A failed bounded run answers its waiting callers

- **WHEN** a bounded run fails while two callers wait on it
- **THEN** both SHALL receive `call.callee_ended` carrying the run's terminal status `error`

#### Scenario: A full table refuses

- **WHEN** `call_pending_max` is 2, two callers wait on an operator, and a third calls it
- **THEN** the third SHALL receive `call.busy` and nothing SHALL be ingested for it

### Requirement: The waiting callers SHALL survive a handover and SHALL learn at once of a crash

The callee's Overwatch SHALL hold one row per waiting outside caller: its token, its reply
address, its deadline timer and the channel it entered on. A handover of the Overwatch to another
node SHALL carry every row with its remaining time. When the Overwatch process dies, every caller
waiting on it SHALL be answered `call.unavailable` at once, not at its deadline.

#### Scenario: A handover keeps a waiting caller

- **WHEN** an operator's Overwatch is handed over to another node while a caller waits on it
- **THEN** the caller SHALL still receive the answer the turn produces, or `call.timeout` at the original deadline

#### Scenario: A crash is reported, not waited out

- **WHEN** the Overwatch a caller waits on is killed
- **THEN** the caller SHALL receive `call.unavailable` without waiting for its deadline

### Requirement: A feeding plugin SHALL call its operator through `Host.inject/5`

`Flezha.Plugin.Host.inject/5` SHALL accept the mode `:call`, whose last argument is the deadline
in milliseconds. It SHALL answer `{:ok, payload, metadata}` with the answer's payload and metadata
carrying the answering `channel`, or `{:error, %Flezha.Error{}}` with one of the codes above; a
host it cannot reach SHALL answer `call.unavailable`. The plugin's side SHALL wait for the
Overwatch's answer for the deadline plus `call_grace_ms`, as `Flezha.Service.Operator.call/6`
does, so that a call with no answer by its deadline SHALL always end with the Overwatch's
`call.timeout` and never with the plugin's own wait ending first. A plugin calls the operator it
is enabled on, so no authorization runs for it.

#### Scenario: A plugin gets its answer

- **WHEN** a plugin enabled on a gateway operator calls `Host.inject(overwatch, envelope, %{"channel" => "http"}, :call, 5_000)` and the gateway's graph answers through `response()`
- **THEN** the call SHALL answer `{:ok, answer, %{"channel" => channel}}`, where `answer` is the answer's payload

#### Scenario: A plugin's call times out with the Overwatch's code

- **WHEN** a plugin calls `Host.inject(overwatch, envelope, %{"channel" => "http"}, :call, 100)` and the graph never answers
- **THEN** the call SHALL answer `{:error, %Flezha.Error{code: "call.timeout"}}` after about 100 ms, not `call.unavailable`

### Requirement: A partition group SHALL be called by key

A call naming a partition group SHALL carry a key, and SHALL reach the member whose key-group range
owns that key, by the one resolution `flz state get` uses. A call naming a group with no key SHALL
be answered `call.target_unkeyed`, naming the group's members. A call naming a member directly
SHALL reach that member.

#### Scenario: The owning member answers

- **WHEN** `flz operator call prices --key EURUSD --text x` is run against a group of two members
- **THEN** the member whose range owns `EURUSD` SHALL receive the input and its answer SHALL be printed

#### Scenario: A group without a key

- **WHEN** `flz operator call prices --text x` is run against a partition group
- **THEN** it SHALL fail with `call.target_unkeyed` naming `prices#p0` and `prices#p1`, and exit 2

### Requirement: Calls SHALL be observable

The controller SHALL emit `[:flezha, :call, :answered]` with the answer latency,
`[:flezha, :call, :timeout]`, `[:flezha, :call, :answer_dropped]` with its reason, and
`[:flezha, :call, :refused]` with its code, each naming the namespace and the operator, and the
observability bridge SHALL export them. `flz operator info` SHALL show how many callers wait on the
operator.

#### Scenario: A timeout is counted

- **WHEN** a call times out
- **THEN** one `[:flezha, :call, :timeout]` event SHALL be emitted naming the operator, and the exported counter SHALL move by one

