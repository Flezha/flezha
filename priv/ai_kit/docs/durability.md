# Durability and delivery

Flezha writes down every change a tasklet makes to its state, and every step a
workflow finishes, on the controller's disk as it happens. When a runner or the
controller comes back after a stop or a crash, each relay carries on from
where it was, and nothing in the relay's code saves or loads anything. This
page says what is kept, where, what a crash can still cost, and how to run a
deployment so that a planned action loses nothing.

Two things can be lost: a relay's **state**, and its **output**. The live
copy of the state is in the cells on the relay's runner; it is copied to a
log per cell on the controller, in batches about every 5 ms. Output is routed
through the controller to its output plugins (Kafka, Postgres, HTTP, another relay).
The core rule: **a graceful stop loses no state; a hard kill can lose the last
few milliseconds of changes that were not yet on the controller's disk.**

## Durability levels

How far a relay's work is kept is its class's `durable` setting
([`relay_settings.md`](../../configuration/relay_settings.md)). A class that runs
in flow also has a duplicate rule, `exactly`, which says how often a value may
reach an output plugin:

| `durable` | Where the work is kept | After a runner or controller crash | After the controller's disk is lost | Flow, `exactly: at_least_once` (default) | Flow, `exactly: effectively_once` |
| --- | --- | --- | --- | --- | --- |
| `false` | the runner's memory only | the work starts over | lost | a value can reach an output plugin again | refused: the tier needs a disk (`topology.effectively_once_without_persistence`) |
| `local` (default) | the controller's disk | carries on; a controller that stays away past the [failure grace](#a-controller-that-fails) loses the work since the last ship, or up to everything without a backend | lost, unless a copy was shipped | a value sent just before a crash can reach an output plugin again | once at a conforming output plugin: outputs wait until the change behind them is on disk |
| `remote` | the controller's disk, and a remote copy that each commit point waits for | carries on | carries on from the copy | as for `local` | as for `local`, and nothing acknowledged is lost with the disk |

A workflow has no duplicate rule to choose: its outputs are released only when
the step that made them commits, and a durable workflow's steps write the same
atomic record the effectively-once tier writes.

**A commit point** is where a piece of work counts as done: one input for a
flow class (`ack: per_input`, or `at_least_once`), one checkpoint for a flow
class with `ack: checkpoint`, one finished step for a workflow. What losing the
controller's disk costs depends on the level and the commit point:

| `durable` | Flow, an input | Flow, a checkpoint | Steps, a stage |
| --- | --- | --- | --- |
| `false` | everything, on any crash | not declarable: the tier needs a disk | everything: the run starts over |
| `local` | the work since the last ship, at most the RPO target | the work since the last ship, at most the RPO target | the work since the last ship, at most the RPO target |
| `remote` | nothing acknowledged | nothing acknowledged; the open interval replays | nothing acknowledged |

Without a remote backend (`FLEZHA_DURABILITY_BACKEND=none`, the default) there
is no ship, so at `local` the disk is the only copy.

## What happens in each situation

| Situation | State | Output | Why |
| --- | --- | --- | --- |
| a runner stops gracefully (SIGTERM, rolling deploy) | no loss | at-least-once | the runner sends each relay's final state and waits for the controller's acknowledgement before it exits |
| one runner killed under load | the last few changes may be lost | at-least-once | no drain; the relay rebinds and restores from the controller's log, which may lag the last changes |
| every runner killed at once | the last few changes may be lost | at-least-once | the same restore, with no live copy anywhere |
| a runner runs out of memory, its container or node dies | the last few changes may be lost | at-least-once | as a kill: no drain runs |
| a controller restarts gracefully, with a live peer | no loss | at-least-once | the new instance syncs the registry from the peer; runners stay connected. A graceful stop or drain never waits for the [failure grace](#a-controller-that-fails): its relays move at once |
| a controller fails (killed, crashed, cut off) and is back within the failure grace | no loss | at-least-once | its relays stay placed on it for `FLEZHA_PROCESS_HUB_NODEDOWN_GRACE_MS` (30 s); inputs sent to them meanwhile are refused and retried by the sender; it restores them from its own disk |
| a controller fails and stays away past the failure grace | the work since the last ship, at most the RPO target; up to everything without a backend | at-least-once | its peers take its relays over from the shipped copy ([below](#a-controller-that-fails)) |
| the only controller restarts | no loss | at-least-once | the first recovery round replays the relay list from disk; `/healthz` says `recovering` until relays are bound ([`running_and_testing.md`](running_and_testing.md#after-a-controller-restart)) |
| the whole cluster starts cold | recovered from the durable registry | none in flight | disk is the only record |
| a relay moves by cold swap, the default (a drain, a node joining) | no loss | at-least-once | the relay stops, then starts on the new controller, which copies its saved state from the old one before it takes input; a drain waits until each moved relay has arrived. Input sent during the move gets no answer: a sender that waits for the answer, as every input of an effectively-once relay does, sends again and loses nothing, and input sent without waiting is lost, as in a crash. A failed controller's relays are the two rows above |
| a relay moves by hot swap (a namespace set to `hot`) | input the old copy takes after handing its state over can be lost | at-least-once | the new copy starts and takes the state live while the old one still runs, so the relay keeps answering during the move ([namespaces](namespaces.md#how-its-relays-move)) |
| a node rejoins a live cluster | synced from peers; its own copy is kept | none in flight | nothing is missing, so nothing starts |
| a Kafka broker is down, runners alive | no loss | buffered, resumes | the producer reconnects and drains |
| sustained overload: changes outpace disk syncs | no loss | at-least-once | a runner may have 16 MiB of unsynced changes in flight; past that it waits, it does not drop. A shed emits `[:flezha, :state, :credit_anomaly]` |
| a workflow's runner is killed (durable) | no loss of a committed step; the step in flight runs again | released only after its step commits | it resumes at the first step not committed ([`workflows.md`](workflows.md#crashes-and-resuming)) |
| a looping workflow's runner is killed (durable) | no loss of a committed step | released only after its step commits | it resumes after the last committed iteration; unconsumed input is still in its input log |
| input sent to a sleeping, crashed or moving durable workflow | no loss | none yet | written to the instance's input log before it is forwarded, delivered on wake |
| input sent to a sleeping flow instance | no loss | none yet | written to its input log first; the instance wakes, its cells are restored, and the message is handled once, also across a controller restart |
| the controller redeploys while an instance sleeps | no loss | none in flight | sleeping is durable; the instance stays asleep and wakes on its next input or timer |
| a workflow kept in memory (`durable: false`) is killed | everything: it re-runs from the top | at-least-once | no log; messages waiting in memory are lost |
| a failed turn of a `keep_progress` session (durable) | no loss: the turn's message is parked | the turn's outputs stay out; its undos run | the message is parked before the iteration is recorded as dead-lettered; a crash parks it at most once |
| a workflow killed in the middle of an undo (durable) | the undo resumes | none | done undos are skipped, the rest run |
| a poison message, failing every attempt | no loss | the cell continues | it is parked in the dead-letter queue before the cell moves on ([below](#the-dead-letter-queue)) |

### A controller that fails

A controller that is killed, crashes or loses its network keeps its relays for
the failure grace, `FLEZHA_PROCESS_HUB_NODEDOWN_GRACE_MS` (30 000; `0` turns it
off). Meanwhile its peers start none of them: an input sent to one is refused
as unreachable and the sender retries it, and a relay start placed on that
controller fails the same way. A fire-and-forget input sent meanwhile has
nobody to refuse it and is lost. Back within the grace, the controller restores
its relays from its own disk and nothing acknowledged is lost. With
`FLEZHA_AUTO_RESTORE_ENABLED` off, a controller that restarted rather than
reconnected ends its grace on return: its relays move as below, then back.

Past the grace, its peers take its relays over. With a durability backend each
starts from its last shipped copy, so the work since that ship is lost, at most
the RPO target. Without one, a relay starts from whatever older copy the peer
still holds, or empty: the loss can be everything. A network split that lasts
longer than the grace then follows `FLEZHA_SPLIT_BEHAVIOUR`. A graceful stop, a
drain and a rolling restart never wait for the grace.

### Why output can get ahead of state

Output and state changes travel separately, and by default the controller
sends output on before the matching state change is synced to disk. So after a
hard kill, output can outlive the state that produced it (an output plugin holds
`total=135` while the log holds `128`), and the restored relay comes back
slightly behind. A planned stop drains to zero loss; a relay that needs the
gap closed declares `exactly: effectively_once`.

**The sync window.** The controller syncs each cell's log to disk once per tick
(`:agent_fsync_interval_ms`, 5 ms by default, running only while a log has
unsynced records). Records written in the last tick can be lost on a hard
kill; a lower interval narrows the window and costs I/O. Syncing per record is
not supported for cell logs.

**A damaged record is refused, not repaired.** A log is cut short only where it
is provably a torn tail, as a crash in the middle of a write leaves, and that
repair is reported. A record whose checksum fails with complete records after
it is not a torn tail: the bytes are left as they are, the log is not served,
the refusal is reported once at `error` with a
`[:flezha, :state, :record_frame, :refused]` event naming the file, the last
good sequence number and the byte offset, and the cell's restore is refused, so
the relay stays unbound rather than running on part of its history. Recovery
is your decision, with the bytes in hand: fetch from the remote tier, take a
peer's copy, or inspect the file.

## Delivery

### At least once, by default

Output leaves as it is produced, and nothing remembers what was delivered, so
a replay after a crash can send a value again. A channel declared `keyed()`
stamps each plugin-bound output with `flz.cell`, `flz.seq`
(`<change_seq>.<emit_ordinal>`) and `flz.key` (the cell's key, or the key given
to `ctx.emit_keyed(key, value)`). The stamps come only from inputs that replay
the same way, so a replay sends identical stamps, and a consumer that upserts
on `flz.key` absorbs duplicates exactly. That holds only if `run`, `apply_to`
and `compensate` read no clock, random value or outside system: use
`ctx.now()` and `ctx.stableId()` ([`state.md`](state.md#rules-for-state-and-changes)).

Two limits: `flz.seq` is not an exact dedup handle for append-only output plugins (two
invocations that change no state can share one), and there is no `flz.epoch`
at this level. A conforming output plugin reads a keyed record without an epoch as epoch
`0` and orders by `flz.seq`; a sequence that goes backwards, from a cell that
cold-started without a baseline, is refused rather than guessed.

**Nothing is dropped by default.** `DropNothing` is each channel's default
overflow policy: a full mailbox slows the producer instead. On a channel
between tasklets the producing cell waits until its value is taken; on an
entry channel the reader stalls and the back-pressure reaches the controller;
a plugin-bound channel has no mailbox to fill, and declaring a policy there
does not compile. The Kafka producer's queue also drops nothing by default. A
class that prefers to shed declares `on_full(DropNewest)` or `DropOldest`. One
window is not closed: the controller's send buffer to a runner discards the
newest frame when the runner stops reading.

Without an input log, a restore keeps the input queued in a cell's mailbox,
since that queue is its only copy. On the effectively-once tier, and for a
class that sleeps, the restore discards it, because the input log re-forwards
it.

### Effectively once

`exactly: effectively_once` writes each input to the relay's input log before
it runs, and each invocation's state changes, outputs and consumed offset as
one record. An output leaves only once its record is on disk, stamped so a
conforming output plugin drops a repeat. A durable workflow has the same
guarantee without asking. What the tier does, what it costs, and how a late
input corrects the past are in [Effectively once](effectively_once.md).

### Workflows: released at the step's commit

A durable workflow's step does not let its output escape before the step is
durable. Each item writes one record (its state changes and its outputs); the
controller records the step as committed only once each record the step named
is on disk; and the outputs are released after that. A step that failed, was
interrupted and re-run, or was rewound releases nothing. Each released output
carries `flz.cell`, `flz.seq`, `flz.epoch`, its channel and `flz.key` when the
channel is keyed; `emit_keyed` and metadata work in a step too. A crash re-send
keeps every stamp, so a dedup output plugin absorbs it; a rewind leaves `flz.epoch` and
its re-run's outputs sort above the old ones, so an `(epoch, seq)`-guarded output plugin
overwrites. An undo's output is a newer record too. A workflow kept in memory
sends outputs straight on.

The guarantee: never lost, never released before commit, and at-least-once
with stable keys in the window between an output plugin's confirmation and the cursor's
write. Inspect a step's records with `flz state journal <relay/instance>`
and what a cell still owes with `flz state outbox <relay/tasklet/key>`
(`outbox_lag`, `held_for_stage`).

A durable workflow's input goes through the same input log: written before it
is forwarded, delivered again after a crash, and a repeated `flz.msg_id` is
absorbed, so an at-least-once sender cannot cause a duplicate turn. A rewind
never re-delivers a consumed message. Its appends use the same 16 MiB source
credit, so a sender waits until the instance commits an iteration. Inspect the
log with `flz state inbox <relay/instance>`.

### Sleeping instances

A class kept on disk can sleep when idle (`sleep_after`; never by default for a
flow class, six hours for a workflow). A sleeping flow instance writes each
input to its input log before forwarding it, one append per message, so a
flow class that never sleeps pays nothing for it. A message sent to a sleeping
instance wakes it and is delivered once, across a controller restart too. A
user's stop wakes a sleeping workflow to end `cancelled`; a sleeping flow
instance's stop removes it. The mechanism is in
[the relay lifecycle](https://github.com/flezha/flezha/blob/main/docs/internal/architecture/relay_lifecycle.md).

### Relay to relay

The `relay_forward` plugin delivers one relay's output into another's
input with no extra configuration; the guarantee follows from the source:

| Source relay | Across the hop |
| --- | --- |
| a flow relay on the effectively-once tier | effectively once |
| a durable workflow | effectively once |
| a plain flow relay | at least once, without dedup |

Forwarding into a partition group routes by `hash(flz.key)`; a message without
`flz.key` is parked in the source's dead-letter queue with
`partitioned target requires keyed emit`. Items in flight survive a controller
restart for the first two sources; a plain flow relay retries only while its
plugin worker lives ([`../../plugins/relay_forward.md`](../../plugins/relay_forward.md)).

### Timers

A timer's registration, cancellation and firing are records in the cell's own
log, so pending timers come back with the cell:

| Crash point | Outcome |
| --- | --- |
| before the registration is on disk | the timer was never owed |
| after the registration, before firing | pending; fires when due |
| after firing | it does not fire again |
| in the middle of a firing, before its record is on disk (tier) | the whole firing repeats |

Without the tier a firing is at-least-once, like an input; a firing that runs
out of attempts is parked, and its removal is durable either way, so a poison
timer parks once ([`timers.md`](timers.md)).

## The dead-letter queue

A message that fails however often it is tried is not discarded. It is tried
up to the channel's `retry_max` (3 attempts by default, with growing waits), or
parked at once on a non-retryable error, and then written to the controller's
dead-letter queue (`storage/app/dlq/<relay_class>/<tasklet_id>/`) before the
cell moves on, so the messages behind it are not held up. Some messages park
with no attempt at all: an unroutable forward into a partition group, a key
outside the member's range, and a message naming a channel that is not an entry
channel of the relay (parked after one attempt, with an error naming the
entry channels the relay has).

On the effectively-once tier the park is part of the invocation's atomic
record, so a crash cannot move the offset while losing the park.

In the tradedesk example, a trade whose price is not a number lands there. In
Java and Rust the typed `Trade` cannot be decoded, so the entry channel refuses
it (cell `_entry`); in the other SDKs `track_prices` refuses the price itself:

```sh
$ flz dlq list 'trade_watcher#p1'
```

```output
ENTRY                                                       NODE                        CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/track_prices/307bcbd6439a080f              flezha@127.0.0.1            EUR/USD         trades      1    8s        TypeError: the price is not a number: "one euro"
```

```sh
$ flz dlq show '<entry>'
```

```output
...
attempts          3
status            "parked"
payload            {"channel": "trade", "data": [{"symbol": "EUR/USD", "price": "one euro", "ord_type": "market"}]}
```

| Command | What it does |
| --- | --- |
| `flz dlq list [<relay>]` | the entries parked, with cell, channel, generation, age and error; an output a sink gave up on carries its [error](../../errors.md) object, such as `kafka.exhausted` |
| `flz dlq show <entry>` | one entry, with its payload |
| `flz dlq replay <entry>` | sends it through the ordinary input path again, as a new input; a replay that fails again re-parks with `generation` one higher |
| `flz dlq drop <entry>` | removes it, audited |

The four verbs need the `ops` role, since an entry holds the failed record
verbatim. A verb naming a relay is judged in that relay's namespace; one
naming none reads every namespace and is judged in `_system`. A partition group cannot be rescaled while its dead-letter queue
holds entries ([`partitioning.md`](partitioning.md)).

Retention bounds the queue by age and size, 30 days or 4 GiB, whichever is
stricter. A retention deletion is real loss, and loud:
`[:flezha, :dlq, :retention_dropped]`, exported as
`flezha_dlq_dropped_by_retention_total`. Each new entry also emits
`[:flezha, :dlq, :dead_lettered]`, naming the entry, which reaches controller
plugins only ([`../../plugins/hook_webhook.md`](../../plugins/hook_webhook.md)).

**In a workflow** a failing step is retried the same way, but when the
attempts run out the workflow fails and is undone instead of parking. Two cases
park: a message the entry channel cannot read, and a failed turn of a
`keep_progress` session, parked under the `_turn` slot (keyed
`_turn@<offset>`) before the iteration is recorded; `flz dlq replay` sends it
back as a new message, and a replay to a session that has ended is refused
([`workflows.md`](workflows.md#undoing-undo_all-and-keep_progress)).

## Example: a crash in the middle of an order

In the tradedesk example, `order_executor` records each step in its journal
when the step finishes, and `confirm_fill` waits ten seconds for the demo
exchange. Read the journal during that wait:

```sh
$ flz relay start order_executor --id order_1 --namespace trading --text '{"symbol": "EUR/USD", "quantity": 10}' --channel order
$ flz state journal order_1 --namespace trading
```

```output
task-journal for instance order_1:

  seq=0 task_committed task_idx=0 iter=0 epoch=0
    ran: order_1/reserve_funds/ record=0
    ...
  seq=1 task_committed task_idx=1 iter=0 epoch=0
    ran: order_1/place_order/ record=0
    ...
  seq=2 task_committed task_idx=3 iter=0 epoch=0
    ran: order_1/record_order/
    ...

  outcome: running
```

Three steps are committed and the order is still running. A runner killed now
and started again runs only `confirm_fill`, which completes the order; the
journal then ends with it and `outcome: ok`:

```output
...
  seq=3 task_committed task_idx=2 iter=0 epoch=0
    ran: order_1/confirm_fill/
    handoff: 0 item(s), 0 byte(s)
    completes the instance: result 57 byte(s)


  outcome: ok
```

The money is not reserved twice and the order is not booked twice. The PHP
example's `confirm_fill` waits on a timer set for a fixed moment, so after a
crash it finishes at the original deadline, or at once if the runner is back
later than that.

An order too big for the exchange fails at `confirm_fill`, is tried three
times, and is undone, last step first. Each undo reads its id from its step's
state, which survives a crash, and the booking stays because `record_order`
has no `compensate`:

```output
[book] EUR/USD 136 as ord-be4f7a593756acada0924d10da8d31c6
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 63 ms): the exchange fills at most 100 a time, and this is 136
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 66 ms): the exchange fills at most 100 a time, and this is 136
[undo] cancelled ord-be4f7a593756acada0924d10da8d31c6
[undo] released res-935e21608cc9ea9844b537d8c0c97517
```

## Surviving the loss of a disk: the remote tier

Everything above is local. A controller that dies with its disk takes its
relays' state, and the record that they exist, with it; moving state
between controllers is a one-time pull when a relay moves, not
replication. Unset, `FLEZHA_DURABILITY_BACKEND` is `none` and a production
controller warns on every boot; `none` set explicitly is the same as a build
without the tier, reported as `durability: local_only`.

| Backend | Where the copy goes |
| --- | --- |
| `none` | nowhere; the local disk is the only copy (the default) |
| `postgres` | a table in a PostgreSQL database (`FLEZHA_DURABILITY_PG_*`) |
| `s3` | a bucket on S3 or an S3-compatible store (`FLEZHA_DURABILITY_S3_*`) |
| `local_path` | a folder that outlives the controller's disk, such as a network mount (`FLEZHA_DURABILITY_PATH`) |

A backend selected without its settings refuses the boot, naming the missing
option ([Controller environment variables](../../configuration/environment_variables.md#state-wal-and-durability)).

With a backend, immutable copies (snapshots, log bundles, journal suffixes, the
input log, the registry, parked dead-letter records, the authorization and
schedule stores) are shipped at once after a compaction, a registry change, a
park or a drop, and otherwise every `FLEZHA_DURABILITY_SHIP_INTERVAL_MS` (30 s),
when each copy that changed since its last ship goes up. At `local` the copy is
then about one interval behind, and one still behind after
`FLEZHA_DURABILITY_RPO_TARGET_MS` (120 s) raises the RPO alarm on `/healthz` and
`flz controller status`; a `remote` class ships before each acknowledgement
instead. The manifest is written last, so a half-finished upload is invisible
to recovery. A controller that starts on an
empty disk fetches the copy and carries on from it. When a relay moves back
to a controller that kept an older copy, the copies are compared by lineage
first: an incoming copy that continued the kept one, or is unrelated to it,
replaces it whatever its length, and one the kept copy continued replaces it
only once it moved past the point the kept copy took over from. Copies of the
same lineage are compared by owner epoch, then highest sequence number (a tie
goes to the incoming one). The replacement is whole; one that does not land
whole leaves the local copy as it was. A controller a relay left ships none of
its turns until the relay binds there again, and never takes back a lineage
another controller continued. On a
controller that held nothing, that means the relay binds empty while the node it
moved from keeps its copy; while that controller still holds nothing of the
relay, each bind asks for the copy again before the runner starts. A node that
cannot be asked is not asked again.

**At `local`**, shipping is in the background and never delays an
acknowledgement; a failing backend delays nothing. What degrades is the
guarantee, loudly: a lag past `rpo_target_ms` (120 s) emits
`[:flezha, :state, :rpo_exceeded]`, shown by `flz controller status` and
`/healthz` (`degraded, reason: durability_rpo_exceeded`).

**At `remote`**, each commit point waits for its ship before it is
acknowledged: a workflow's step is acknowledged once the manifest naming it is
written; a flow cell holds its acknowledgement, the runner's credit and its
outputs until a ship of the cell and its input log lands, one ship in flight
per cell, covering every input synced meanwhile. Under `ack: checkpoint` only
the interval's close waits, so prefer it for a `remote` flow class, which
otherwise pays a remote write per synced batch. A controller restored from the
copy resumes each cell after the last input it acknowledged. A backend outage
stalls only `remote` classes, and the RPO alarm fires; `local` classes on the
same controller carry on. `flz controller status` prints each running class's
level as `durability_levels`, and each remote commit's wait is
`[:flezha, :state, :remote_commit]`. After a restart, a new stream's first
marker also waits until every cell of the stream's classes whose records on
local disk lie past its shipped copy has shipped, whether or not its writer has
started again.

## How to avoid loss

- **Stop runners gracefully, not with a kill.** Send SIGTERM and set the
  orchestrator's grace period (`terminationGracePeriodSeconds`,
  `stop_grace_period`) longer than the drain (2 s per relay, in parallel,
  with a 10 s cap on writing out state). Roll one node at a time, so a peer is
  always live.
- **The runner must receive SIGTERM itself.** Run it as PID 1 (an exec-form
  `CMD` or entrypoint, or `setpriv` / `gosu` dropping privileges by `exec`). A
  wrapper that forks the runner and kills it on SIGTERM, such as `su`
  ("Session terminated, killing shell"), turns a graceful stop into a hard kill.
- **Restart controllers correctly.** With several, bring the new one up before
  stopping the old one. A sole controller just restarts: the first recovery
  round replays the relay list once `FLEZHA_PROCESS_HUB_CLUSTER_SETTLE_MS`
  (2 000) has passed, and `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS` (45 000)
  only caps the wait on a peer that never answers. `/healthz` reads
  `recovering` until then, and until replayed relays are bound again; a
  relay whose runner has not reconnected waits unbound. That is the design,
  not a fault.
- **Make output plugins idempotent where duplicates matter.** Declare channels
  `keyed()` so a re-sent output carries the same `flz.key` and `flz.seq` to
  upsert on, or use `exactly: effectively_once`; the Kafka and Postgres plugins
  conform out of the box.
- **Write workflow steps and undos to be repeated**, or keep a fire-and-forget
  job in memory so it re-runs from a clean slate.
- **Do not treat the on-disk registry as the live truth.** It is what a cold
  start replays.
- **Set a durability backend** when the state must survive losing a disk; a
  second controller is capacity, not a backup
  ([`deploying_a_controller.md`](../deploying/deploying_a_controller.md#several-controllers-on-one-machine)).

## Limits and gotchas

- **Output is at-least-once by default.** Deduplicate downstream, or use the
  effectively-once tier.
- **A hard kill can lose the last milliseconds** of state changes and resend
  their outputs.
- **`durable: false` loses everything on a crash.**
- **The dead-letter queue has retention**: 30 days or 4 GiB.
- **The WebSocket output plugin is not exactly-once** under any setting.

## See also

- [`relay_settings.md`](../../configuration/relay_settings.md): `durable`, `exactly` and `sleep_after`.
- [`state.md`](state.md): how state is recorded and restored.
- [`workflows.md`](workflows.md): steps, undo and crash-resume.
- [`running_and_testing.md`](running_and_testing.md#after-a-controller-restart): what comes back after a restart.
- [`deploying_a_controller.md`](../deploying/deploying_a_controller.md): the variables a deployment sets, graceful shutdown.
- [`effectively_once.md`](effectively_once.md), [`timers.md`](timers.md), [`partitioning.md`](partitioning.md).
- [Controller environment variables](../../configuration/environment_variables.md#state-wal-and-durability): the durability settings.
- Tutorial: [`crash_the_desk.md`](tutorials/crash_the_desk.html) crashes the tradedesk on purpose.
