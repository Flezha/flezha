# rename-kafka-queue-overflow — implementation notes

## The exposure the decision named is not the exposure the code has

The decision priced one risk: a durable cspec row carrying the retired value
"stops matching the renamed guard, so that plugin fails to enable on the next
boot". The guard it names is
`convert_value(:queue_overflow, value) when value in […]` — and **a stored row
never reaches it.**

`convert_value/2` is called only by the generated `build/1`, which takes a
string map and has **no production caller** in `lib/`. That is already recorded:
[[KI-93]] measured it while fixing the macro, and [[KI-62]] ruled the struct
path's bypass of `convert_value/2` *not a bug* — the two paths have two
contracts. The enable path is `Extension.Table.run_init_provider/1` →
`Kafka.Producer.init_provider/1`, which reads
`coerce_atom(Map.get(opts, :queue_overflow, …))`. `coerce_atom/1` validates
nothing, and `String.to_existing_atom/1` succeeds on the retired spelling
because the observability bus's per-sink policy keeps that atom alive in four
loaded modules.

So the plugin **enables, starts and serves**. The `FunctionClauseError` waits in
`overflow/4` until the queue actually reaches `:queue_max`. Driven, not argued —
a temporary probe on the worker with `queue_max: 1`:

<!-- overflow-naming-record:begin -->
    assert :block == String.to_existing_atom("block")   # passes
    {:ok, pid} = GenServer.start(ProducerWorker, queue_overflow: :block, …)
<!-- overflow-naming-record:end -->
    assert Process.alive?(pid)                          # passes
    assert ProducerWorker.stats(pid).queue_depth == 0   # passes
    …three casts…
    PROBE exit reason: {:function_clause,
      [{Flezha.Plugin.Provider.Kafka.ProducerWorker, :overflow, [...], ...}, ...]}

`ProducerSup` is a `DynamicSupervisor` and the worker is `restart: :transient`,
so nothing cascades; what dies is the worker's in-memory queue. Guaranteed
records are not confirmed and are re-driven, plain chain records are lost, and
it recurs each time the queue fills. **The likelihood is exactly as priced** —
the retired value was the default, so a row carries it only if an operator set
the value already in force. Only the timing and the visibility differ.

## The gate needed three shapes, and none of them may say `overflow`

The gate that missed this required **adjacency**: the retired atom next to a
sibling policy. The option carries it alone, with no sibling on the line:

<!-- overflow-naming-record:begin -->
    queue_overflow: :block
<!-- overflow-naming-record:end -->

The widening keeps the match-directly model and adds, repo-wide, the policy list
**in both orders**, a `queue_overflow`-anchored shape, and `overflow(` with the
paren. The two anchors are load-bearing: the observability bus writes
`overflow: :block` in `otlp_sink.ex`, `audit_log/sink.ex` and `bus.ex`, so a
pattern matching `\boverflow\b.{0,40}:block` claims all three. Anchoring on the
option's own name is what keeps that mechanism out of scope **by construction**
where its predecessor needed an exemption.

Those three still miss prose and scenario titles that name the policy with no
option beside them. A repo-wide bare `:block` is unavailable for the same
reason, so breadth is **path-scoped** to the provider's source, its tests,
`docs/plugins/kafka_producer.md` and `openspec/specs/kafka-producer-plugin/`:
inside those, a bare `:block` is the policy, because the park is spelled
`queue_block_timeout_ms`, `blocked` and `:block_timeout` — none a bare `:block`
atom, since `\b` fails against the following `_`. Composed as two
`pattern_check` calls exactly as `check-kind-naming.sh` does; `naming_gate.py`
gains no code. One trap: the scoped half must return `[]` outside its scope, not
`False` — `walk/1` iterates the result and a bare `and` yields a bool.

Widened first, the gate reported **34 occurrences over 12 files**, every one a
live site. Twelve of those are outside the decision's list, including
`CLAUDE.md`'s dev-notes index line and `lib/mix/tasks/bench/roundtrip/engines.ex`.

## Two records that cannot be renamed, and one that blinds the gate

`docs/refactor/STATE.md` quotes the retired spelling to name this work and is
the supervisor's file, so it takes an `ALLOWED` rule erasing a **backticked span
containing** the atom — the "a quoted occurrence is a mention" reading already
beside it for `docs/known_issues.md`. Drilled: the two backticked shapes are
exempt in that file only, while bare prose, the reversed list and the clause
form are all still caught there, and all six are caught anywhere else.

The CHANGELOG entry and the test that asserts the refusal must both carry the
retired spelling. Both are wrapped in `overflow-naming-record` blocks **on
separate lines** — never the inline one-line form, which is [[KI-108]]: `walk/1`
tests `begin in line` first and `continue`s, so a line carrying both markers
latches the record open to EOF. Six files in the tree use that form today, and
`close-overflow-silent-failures.md:200` leaves 79 lines unread by every
record-tagged gate. Filed, reproduced, not fixed.

## `openspec/changes/` is gitignored, so the delta alone leaves the gate red

The archive is what syncs `openspec/specs/`. That happens after the worker's
turn, so the three `MODIFIED` requirements are edited into the live spec in the
same pass (the shape `bcb85568` committed) — re-applying a `MODIFIED` block
replaces the requirement with the same text and is idempotent. The one `ADDED`
requirement is **not** pre-synced, because adding it twice is not; it carries its
own record block so it lands green.

## `:drop_nothing` is true here, including the timeout

`overflow/4`'s surviving clauses queue the record and `process_blocked/1` admits
it when a batch drains; neither calls `record_drop/3`, so `dropped_total` does
not move and no `:dropped` event fires. The synchronous caller's
`:queue_block_timeout_ms` replies `{:error, {:queue_full, :timeout}}` — a
**refused admission**, not a discard: the record never entered the queue and the
caller is told. That is the obvious objection to the new name and it is answered
rather than left standing.

## Not renamed

The park keeps its names (`queue_block_timeout_ms`, `blocked`, `:block_timeout`,
`drop_blocked/2`) — the same ruling `block_parker` and `BLOCK_PARK_TIMEOUT` took.
`CHANGELOG.md`'s released `## 0.1.0` section and `docs/dev_notes/**` are
historical and are left alone; the predecessor left
`elixir-sdk-native-transport.md:40` standing for the same reason. Its own
`CLAUDE.md` index line still reads "`Block` is the default for every channel kind
in BOTH SDKs" — false since `bcb85568`, invisible to the gate because it is the
bare word, and **not corrected here**: out of this change's scope.
