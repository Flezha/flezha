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
  stopped, while callers wait: `call.callee_ended` to each, carrying the run's status, except an
  operator caller of an instance that is stopped without ending and keeps a durable input log:
  its call stays journaled there and is answered when the instance runs again, or by the
  caller's deadline.

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

#### Scenario: A stopped durable callee keeps its operator callers' calls

- **WHEN** an effectively-once callee is stopped without ending while an outside caller and an operator caller wait on it
- **THEN** the outside caller SHALL receive `call.callee_ended` carrying `stopped`, and the operator caller SHALL receive no answer until the callee runs again or the call's deadline passes

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

### Requirement: An operator SHALL call another operator through a `call` end in its graph

An operator of either kind SHALL call an operator of either kind by ending a chain in `call(...)`;
every value that reaches the end SHALL be one call, whose request is that value. The target SHALL
be one of:

- a named instance, `call(target, deadline_ms: n)`, or a partition group whose member the record's
  key from a `key_by` before the end chooses by the member resolution outside calls use;
- the instance of a factory whose id is the record's key, `key_by(...) |> call(factory: f,
  deadline_ms: n)`, started with the request as its first input when no instance holds that id,
  unless the end says `create: false`, which SHALL answer `operator.not_found` instead;
- with no `key_by`, a new bounded instance of the factory per call, under a generated id; an
  unbounded factory there SHALL be answered `operator.factory_not_bounded`.

The request SHALL enter the callee on the entry channel the end names with `channel:`, or, when it
names none, on the callee's first declared entry channel. The target SHALL always be resolved in
the caller's namespace, and a factory call SHALL reach only
an instance of that factory: a key naming a running instance of another factory SHALL be answered
`operator.factory_mismatch` and nothing SHALL be delivered. Creating and delivering SHALL go
through the controller's one spawn-or-deliver, so two concurrent calls for one absent id start one
instance and deliver the other request to it. A call SHALL never block the invocation that made it.

#### Scenario: One stream calls another by name

- **WHEN** an unbounded `orders` operator ends a chain in `call("pricing", deadline_ms: 500)` and `pricing` is an unbounded operator whose graph ends in `response()`
- **THEN** each order SHALL reach `pricing` as an input, and each answer SHALL reach `orders` as a `Response`

#### Scenario: A session is created on its first call and reached on its second

- **WHEN** a gateway calls `key_by(& &1.session) |> call(factory: "agent", deadline_ms: 60_000)` twice with the session `anna-42`, which no instance holds
- **THEN** the first call SHALL start the bounded instance `anna-42` with the request as its first input and its `Response` SHALL say `created: true`, and the second SHALL be delivered to the same instance and say `created: false`

#### Scenario: An absent session is refused when creation is off

- **WHEN** a gateway's end says `create: false` and the key names no running instance
- **THEN** the caller SHALL receive a `Response` carrying `operator.not_found`, and no instance SHALL start

#### Scenario: A factory call without a key starts one instance per call

- **WHEN** an operator ends a chain in `call(factory: "triage", deadline_ms: 60_000)` with no `key_by` and two values reach it
- **THEN** two bounded instances SHALL start under generated ids, and each `Response` SHALL name its own instance in `target`

#### Scenario: A key cannot steer a call into another operator

- **WHEN** a gateway's factory call to `agent` carries the key `gateway`, the id of the gateway itself
- **THEN** the gateway SHALL receive a `Response` carrying `operator.factory_mismatch`, and nothing SHALL be delivered to any operator

#### Scenario: Two concurrent calls for one absent id start one instance

- **WHEN** two callers call the factory `agent` for the key `bob-7` at the same moment and no instance holds it
- **THEN** exactly one instance `bob-7` SHALL start, both requests SHALL reach it, and exactly one of the two `Response`s SHALL say `created: true`

### Requirement: An operator call's token SHALL come only from the controller's delivery

A callee SHALL take the operator form of a call token (`o:` followed by the caller's instance id,
the call id, whether the call started the callee and the call's deadline) only from the controller's own call delivery, the delivery worker's message to
its call entry point, and from the seed option that delivery sets on `spawn_or_deliver` when the
call creates the callee. It SHALL NEVER read a token from an input's or a plugin's metadata, which
every entry point strips of `flz.reply_to`. Because no path outside the controller's delivery can
place an operator token on an input, a call id that can be computed from a call-end frame's
stamps SHALL NOT let anyone answer a call.

#### Scenario: A forged operator token answers nobody

- **GIVEN** a gateway with a call pending on its session `anna-42` under the call id `c1`, and a plugin enabled on an operator whose graph ends in `response()`
- **WHEN** the plugin calls `Host.inject/5`, in `:call` mode and in `:confirming` mode, with metadata carrying `flz.reply_to: "o:gateway:c1"`
- **THEN** the input SHALL carry no `o:` token, no `Response` SHALL reach the gateway or any other operator from it, and the gateway's call SHALL end only with its session's answer or its deadline

### Requirement: The outcome of a call SHALL arrive at the caller as a `Response` input

The outcome of every call SHALL reach the caller as one input on the caller's Response channel,
named after the calling channel with the suffix `.response`, and that input SHALL be an ordinary
input: counted, run through the `:on_input` chain, journaled when the caller journals input, and
delivered to whatever the Response channel feeds, the calling tasklet included. Its payload SHALL be
one JSON document built by the controller, carrying the call's id, `target` (the callee's instance
id), `created` (whether this call started the callee), `request` (the request's bytes in base64)
and either `value` (the answer's bytes in base64) or `error` (an error document of the error model;
a timeout is the error `call.timeout`). Every SDK SHALL decode it into its `Response` type, decoding
the request with the call end's request codec (in Elixir, whose request serializer is a function,
the request's bytes) and the value with its answer codec, so that
`test/fixtures/call_response_envelope.json` decodes to the same fields in every SDK.

#### Scenario: A bounded callee's answer returns to an unbounded caller

- **WHEN** a gateway calls a `:per_input` bounded session whose turn emits `{"tokens": 812}` on its `response()` end
- **THEN** the gateway SHALL receive a `Response` whose result is ok with that value, whose request is what it sent, and whose `target` is the session's id

#### Scenario: The calling tasklet receives its own call's outcome

- **WHEN** an operator's `Account` tasklet feeds a call end whose Response channel is wired back into `Account`
- **THEN** registration SHALL succeed, and `Account` SHALL receive each `Response` as an input like the requests it receives

#### Scenario: Every SDK reads the same envelope

- **WHEN** each SDK's test suite decodes every case of `test/fixtures/call_response_envelope.json`
- **THEN** each SHALL produce the same request, result, error code, target and `created` for each case

### Requirement: Every call SHALL end with exactly one `Response` while its caller lives

The caller's Overwatch SHALL hold one row per pending call, carrying its deadline, and SHALL append
at most one `Response` per call id. The first outcome to reach the Overwatch (the callee's answer, a
refusal, a dead-letter, the callee's end, or the deadline) SHALL be appended; every later one SHALL
be dropped and counted. When the deadline passes with no outcome, the caller SHALL receive a
`Response` carrying `call.timeout`; the callee's turn SHALL still run, and its answer SHALL be
dropped and counted. The deadline SHALL be measured from the moment the calling invocation emitted
the call, and the time an asleep callee takes to wake SHALL count against it.

#### Scenario: A late answer does not make a second Response

- **WHEN** a call's deadline passes, the caller receives `call.timeout`, and the callee's answer arrives afterwards
- **THEN** the caller SHALL receive no second `Response`, and the late answer SHALL be counted as dropped

### Requirement: A call between two durable operators SHALL survive a crash of either side with exactly one `Response`

A call SHALL survive a crash of the callee's runner, of the caller's runner, and of the Overwatch
or controller of either side, when the caller and the callee both keep a durable input log (an
unbounded operator under the effectively-once tier, a durable bounded operator). The callee SHALL
run the call's turn at most once to its commit: a request sent again SHALL be absorbed by its
`flz.msg_id` while the first is retained. The caller's pending calls and their deadlines SHALL be
recovered with its state: a call output SHALL stay held in the caller's outbox until the call's
`Response` is durable in the caller's input log, so every call without a durable `Response` is
dispatched again by the caller's recovery. A recovered call whose `Response` the caller's log
retains SHALL be settled without a new `Response`; one whose deadline is ahead SHALL be sent again
and its timer re-armed; one whose deadline has passed within `call_retention_margin_ms` SHALL be
answered `call.timeout`. A recovered call past its deadline plus that margin, whose `Response` is no
longer retained, SHALL be settled with no new `Response` and counted as `recovered_unanswered`:
there the guarantee is at most one `Response`. A caller whose Overwatch restarted and holds no
row for a call whose output its outbox still holds SHALL NOT drop that call's answer: its Cells
SHALL dispatch their held calls again at once, and the answer SHALL be taken when its delivery
retries. The callee SHALL hold its answer in its own outbox until the caller's Overwatch has taken
it, and SHALL keep retrying it until then, until the call's deadline plus the margin has passed,
or until the caller is no longer declared in its namespace. An answer whose delivery was lost
while held SHALL be dispatched again by the callee's outbox and handed to the delivery again.

#### Scenario: The callee dies mid-turn

- **WHEN** a gateway calls a durable `:per_input` bounded session and the session's runner is killed while the turn runs
- **THEN** the turn SHALL run again from the journaled request on the rebound runner, commit once, and the gateway SHALL receive exactly one `Response`

#### Scenario: The caller dies while a call is pending

- **WHEN** an effectively-once gateway's runner and Overwatch are killed after its call was sent and before the answer arrived
- **THEN** the recovered gateway SHALL send the request again, the session SHALL absorb it by its message id, and the gateway SHALL receive exactly one `Response`

#### Scenario: The answer reaches a restarted caller before its re-drive

- **WHEN** a durable gateway's Overwatch restarts alone after its call was sent, and the session's answer reaches it before the gateway's outbox dispatches the call again
- **THEN** the gateway's Cells SHALL dispatch the held call again at once, the answer's delivery SHALL retry, and the gateway SHALL receive exactly one `Response` carrying the answer

#### Scenario: The caller dies after its Response was appended

- **WHEN** the caller's controller dies after a call's `Response` was durable in its input log and before its outbox cursor passed the call
- **THEN** the recovered caller SHALL find the `Response` by its message id, SHALL settle the call without a second `Response`, and SHALL send nothing again

### Requirement: A call SHALL be best effort when either side is off the durable tier

When the caller or the callee keeps no durable input log, a lost request or a lost answer SHALL end
the call with `call.timeout` at its deadline while the caller's Overwatch lives, and a caller whose
Overwatch dies SHALL lose its pending calls. No retry by the caller SHALL be implied.

#### Scenario: A lost answer becomes a timeout

- **WHEN** an unbounded callee off the tier loses its runner after taking a call and before answering
- **THEN** the caller SHALL receive `call.timeout` at the call's deadline

### Requirement: A `:per_input` bounded operator SHALL call without suspending

A bounded `:per_input` operator SHALL be able to call: the call SHALL leave when the stage that
emitted it commits, and its `Response` SHALL arrive on the Response channel as a later message, run
as a later iteration like any other. No pass SHALL wait for a `Response`. A bounded instance that
ends with calls pending SHALL abandon them: their outcomes SHALL be dropped and counted.

#### Scenario: A session calls a tool and reads the result on its next turn

- **WHEN** a `:per_input` session's turn emits a request on a call end to an unbounded `tools` operator and commits
- **THEN** the call SHALL leave at that commit, and the tool's `Response` SHALL start a later iteration of the session

### Requirement: Requests SHALL reach a target in order, and `Response`s SHALL carry no order

Requests from one caller to one target SHALL reach the target in the order the caller's Overwatch
received the call outputs, so a session's turns run in the order they were called. `Response`s
SHALL reach the caller in the order their appends complete, with no order across calls; a caller
that needs one SHALL restore it from the request each `Response` carries.

#### Scenario: Two turns for one session keep their order

- **WHEN** a gateway calls the session `anna-42` with turn 1 and then turn 2
- **THEN** the session SHALL journal turn 1 before turn 2

### Requirement: Pending calls SHALL be bounded at the callee and by the caller's outbox

`call_pending_max` SHALL bound the answers one callee operator owes to the callers waiting on it,
outside callers and operator callers alike. An operator call to a callee that already owes that
many SHALL be answered at once with a `Response` carrying `call.busy`, and nothing SHALL be
ingested for it. An operator's own pending calls SHALL NOT count against `call_pending_max`, its
own or any callee's: a durable caller's SHALL be bounded per Cell by its outbox window, whose later
outputs wait until a call ends, and every caller's by each call's deadline.

#### Scenario: A callee that owes too many answers refuses an operator caller

- **WHEN** `call_pending_max` is 1 on `pricing`, which owes one answer to a waiting caller, and `orders` calls it
- **THEN** `orders` SHALL receive a `Response` carrying `call.busy`, and `pricing` SHALL ingest nothing for that call

#### Scenario: A caller's own calls never refuse its callers

- **GIVEN** a gateway with `call_pending_max` set to 2 and three calls pending on its sessions
- **WHEN** an outside caller calls the gateway
- **THEN** the gateway SHALL ingest the request and SHALL NOT answer `call.busy`

### Requirement: A callee's failures SHALL reach an operator caller as error `Response`s

Every error answer an outside caller receives SHALL reach an operator caller as a `Response`
carrying that error: `call.failed` when the callee dead-letters the call's input, by any park
route and in either kind; `call.callee_ended` for each call queued in, or taken by, a callee that
ends; `call.not_accepted` for a `:once` or `:chained` instance that has already taken its input;
and the controller's own codes for a creation or delivery it refuses (`quota.exceeded`,
`operator.factory_unknown`, an `authz.*` code). When a `:per_input` `keep_progress` loop
dead-letters a failed turn and goes on, as `keep-a-session-after-a-failed-turn` defines, that turn's
caller SHALL receive `call.failed` and the calls queued behind it SHALL be answered by their own
turns.

#### Scenario: An unbounded callee dead-letters a call

- **WHEN** an effectively-once unbounded callee exhausts its retries on a call's input and parks it
- **THEN** the caller SHALL receive a `Response` carrying `call.failed` with the dead-letter reference, before the call's deadline

#### Scenario: A failed turn in a session that goes on

- **WHEN** a `keep_progress` `:per_input` session dead-letters a failed turn called by a gateway while a second call waits behind it
- **THEN** the first call's `Response` SHALL carry `call.failed`, and the second call SHALL be answered by the session's next turn

#### Scenario: A session ends with a call queued

- **WHEN** a session completes on its turn while another caller's request waits in its input log
- **THEN** that caller SHALL receive a `Response` carrying `call.callee_ended`, or `call.timeout` at its deadline if that answer is lost

### Requirement: A rewind SHALL neither answer a call again nor take an answer back

A rewind of a callee SHALL NOT send a second answer for a call whose answer has left, and SHALL NOT
withdraw one; its compensation undoes state only. A turn the rewind interrupts before its answer
left SHALL leave its caller to the call's deadline.

#### Scenario: A rewound session's caller keeps its answer

- **WHEN** a session is rewound to iteration 3 after iterations 4 and 5 answered two calls
- **THEN** both callers SHALL keep the `Response`s they received, and neither SHALL receive another

### Requirement: Per-key instances SHALL count against the namespace's operator quota

An instance a call creates SHALL count against the namespace's `max_operators` from its start until
its teardown, asleep or awake. A call whose creation the quota refuses SHALL be answered with a
`Response` carrying `quota.exceeded`, and no instance SHALL start.

#### Scenario: The quota refuses a new session

- **WHEN** the namespace's `max_operators` is reached and a call would create a new session
- **THEN** the caller SHALL receive `quota.exceeded`, and the calls to running sessions SHALL still be delivered

