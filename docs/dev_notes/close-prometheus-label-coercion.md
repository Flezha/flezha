# `close-prometheus-label-coercion` — the Prometheus label rule

## What the defect actually was

Two rules missing at one funnel, and only one of them raises.

`prometheus_registry.ex:192` fed every label through
`{to_string(k), to_string(v)}` — the shape `close-otlp-export-crash` had already
removed from `OtlpCodec`. KI-45 filed it *Not driven* and called it "a live crash
waiting on one new source". Both halves of that were wrong, and the corrected
entry says so: **two live `TelemetryBridge` paths carry a non-scalar today** —
`wal_writer.ex:1885` puts a 3-tuple `entity_id` on the metadata and proves it six
lines below by destructuring `{rc, sh, pk} <- state.entity_id`, and
`forward_worker.ex:178`/`:210` build `{:delivery_crashed, _}` / `{:unreachable, _}`
which `settle/2:252` hands to `Telemetry.retry/4` as `:reason`. Both map `:metric`,
so `PrometheusSink.filter/0` accepts both.

The **blast radius is five ETS tables**, not one dropped signal: `init/1` creates
all five in the registry process, so the crash discards every counter, gauge and
histogram since boot and `render/0` then raises `"Prometheus registry not started"`
through `PrometheusHttp:100`, which has no rescue. Driven, at HEAD:

```
before: flezha_dlq_dlq_usage{operative_class="c",procedure_id="p"} 1
%Protocol.UndefinedError{protocol: String.Chars,
                         value: {"order_class", "proc_a", "key-1"}}
  ...prometheus_registry.ex:192 labels_and_measurement/3
  ...prometheus_registry.ex:148 do_record/1
tables alive after: [counters: false, gauges: false, histograms: false,
                     label_index: false, truncation_counters: false]
render/0 after: {:raised, "Prometheus registry not started"}
```

And the escalation is **observed**: four such events 125 ms apart (a writer
flapping `:full`/`:open`) crash the registry four times in 376 ms, over
`Supervisor.init/2`'s default 3-in-5 s, and `Export.Supervisor` exits `:shutdown`
taking the listener, the sink and the registry with it. Export dies exactly when
back-pressure means an operator needs it.

The quiet halves, each measured and none raising: `%{l: [1, 2, 3]}` rendered the
raw bytes inside the quotes; `%{"a-b c" => "v"}` rendered
`probe_name_subject{a-b c="v"} 1`, which fails the **whole scrape**; a `%{le: …}`
key on a histogram rendered `_bucket{le="collide",le="5000"} 1`; and a non-UTF-8
binary value left the body `String.valid?/1` false.

## Measurements that corrected the change's own documents

- **"The OTLP encoded body is valid UTF-8" is not an assertion that can exist.**
  Task 2.5, design D2 and KI-45's last bullet all rest on it. A fixed64 timestamp
  makes *every* OTLP body invalid UTF-8: **0/20** clean metrics bodies and **0/20**
  clean logs bodies are `String.valid?/1`, defect present or absent. Only "the raw
  bytes are present in the body" was ever diagnostic there. The test asserts the
  *decoded* attribute plus the bytes' absence instead. On the **Prometheus** side
  the body-level assertion is sound, because that body is text.
- **Design D1's audit equivalence is right on seven shapes and wrong on two.**
  Atom, binary, integer, float, tuple, list and charlist all render identically
  under `stringify_key/1` and `metadata_text/1`. But `Atom.to_string(nil)` is
  `"nil"` while `to_string(nil)` is `""`, so a `nil` key lost its name entirely;
  and `stringify_key/1`'s bare `inspect/1` elides past `limit: 200` where
  `metadata_text/1` does not. The elision difference is the widening the OTLP rule
  already carries and is kept. The `nil` difference was **fixed, not accepted**:
  `metadata_text(nil)` renders `"nil"`, which deliberately changes the **OTLP**
  side too — an empty attribute key names nothing there either, and collides with
  a genuine `""` key.
- **`{:a, :b}` sanitizes to `_a___b_`, not design D3's `_a____b_`.** Eight
  characters in, eight out, then the leading run collapses.
- **`mix flezha.bench.observability` cannot measure task 5.3's cost.** It emits
  against **zero interested sinks** so the dispatcher exits before the registry;
  it never reaches `labels_and_measurement/3`. The funnel was benchmarked
  directly instead.

## Load-bearing decisions

- **The rule lives on `Signal`, and has four callers, not three.** `metadata_text/1`
  sits where the metadata contract already does — `Signal.new/1` validates that
  metadata is a map and constrains neither keys nor values, which is exactly the
  fact that makes the rule necessary. `OtlpCodec.kv_attribute/2`,
  `PrometheusRegistry.labels_and_measurement/3`,
  `AuditLog.Codec.stringify_map/1` and `CliManager.stringify_keys/1` call it. The
  fourth was found while checking for dangling references and folded in
  deliberately past the proposal's stated Impact (KI-59): a fourth independently-
  written copy of one decision *is* the defect this change removes; the crash is
  its symptom.
- **Two value rules survive on purpose, and both are JSON.**
  `AuditLog.Codec.stringify_value/1` and `CliManager.inspect_or_pass/1` keep their
  own answers because JSON carries typed values — collapsing either would render
  every number and boolean as a string. Neither keeps its own answer for **keys**,
  which are text in every one of these formats. (That the two disagree with each
  other — one recurses into lists and maps and passes `nil` through, the other
  inspects and renders `"nil"` — is a separate drift, left standing and reported.)
- **The extraction's control was the whole point of doing it alone.**
  `export_test.exs` was **18 passed** before the move and **18 passed** after, with
  nothing else in that step. Those 18 decode the protobuf, which is the only thing
  that can prove a move byte-identical. Every later clause — the UTF-8 one, the
  `nil` one — landed as its own step so a difference stays attributable.
- **Sanitizing happens at record time, not render time.** The label list is part
  of the ETS key (`{source, name, labels}`), so two raw sets that sanitize alike
  must merge into one key — counters add, gauges last-write-win. Sanitizing at
  render would leave two rows emitting **identical series lines**, which Prometheus
  rejects as hard as a duplicate label. It also means `check_cardinality/3` budgets
  the names an operator actually sees.
- **`le` is dropped only on a histogram.** `render_histogram_rows/3` appends its
  own `{"le", …}` per bucket; on a counter or gauge `le` is a legal label and is
  kept. The type was already in hand at the funnel, in the parameter spelled
  `_type`.
- **A leading `_` run collapses to one**, so no producer key can enter Prometheus's
  reserved `__` namespace or forge the registry's own `__truncated__` marker
  (which the registry still writes itself, after sanitizing, in the truncate
  branch).
- **`@truncation_tab` became `@registry_counters`,** carrying
  `{source, :label_coercions}` beside the bare-`source` truncation key. The two key
  shapes cannot collide, so `truncation_count/1` is untouched and
  `coercion_count/1` reads the same table. No new table, no new reporting path,
  and **no event per coercion** — the neighbour `emit_truncation_event/2` already
  emits on every over-budget emit, and copying it would double a storm on the
  recording path. The rewrite is self-evident in the scrape anyway: the sanitized
  name is what an operator reads.
- **The sanitizer is a byte pass, not two regexes.** The obvious
  `String.replace(~r/[^a-zA-Z0-9_]/, "_") |> String.replace(~r/^_+/, "_")` costs
  **~9.7 µs per label name** in compiled module code — not a script artifact, and a
  module-attribute regex is no better (10.4 µs). A single binary comprehension plus
  a leading-run trim is **~0.8 µs**, 12x faster, and agrees with the regex form on
  every input tested. This runs per label per emit.
- **And the byte pass itself only runs on names that need it.** Almost every label
  name is already legal, and rewriting one allocates a new binary to hand back the
  bytes it was given. `legal_label_name?/1` is a byte scan that allocates nothing
  and returns the original binary; only a name that fails it is rewritten. It must
  reject a leading `__` as well as the grammar, because the reserved-namespace
  collapse is part of the rule and `__foo` is grammatical but not legal here.
  Soundness is not argued, it is measured: over **45 036** inputs (36 fixed,
  40 000 random over an alphabet of letters, digits, `_`, `-`, `.`, `{`, space and
  a multi-byte char, and 5 000 arbitrary byte strings), **0 disagreements** with
  always-rewriting and **0 outputs that are not legal label names**.
- **The collision check is a list scan, not a `MapSet`.** A signal carries a handful
  of labels; allocating a set on every emit to dedup four names costs more than
  scanning the list already being built. `kept` is built head-first, so an earlier
  pair is always found and first-wins is preserved.

## What it costs, measured

`labels_and_measurement/3`'s funnel, 200 000 iterations, 4 labels per emit,
compiled arms:

All four arms in one process, so the ratios inside a run are the comparable thing
— the absolute numbers move ~10 % between runs on this box.

| arm | ns/emit | x HEAD |
|---|---|---|
| HEAD (`to_string` pair) | 1216 | 1.00 |
| value rule only (`metadata_text/1`, i.e. `String.valid?`) | 1629 | 1.34 |
| name rule, first cut (`MapSet`, rewrite every name) | 5892 | **4.85** |
| **name rule, shipping (validity fast path, list scan)** | **2166** | **1.78** |

The cost design D2 admits to is **+413 ns/emit (1.34x)**. The cost it does **not**
name — the D3 name rule — was **+4676 ns (4.85x)** in its first cut and is
**+950 ns (1.78x)** shipping: the fast path and the list scan win back **63–67 %**
of it, over three runs measuring 1.78x / 2.04x / 1.83x. Without the byte-pass
sanitizer the first cut would have been ~40 µs/emit.

So the registry's single process goes from roughly ~820 k to ~460 k records/s of
headroom rather than the ~123 k the first cut would have left. That is a real cost
and it is stated rather than asserted away, but at 1.8x on a telemetry path — not
the data plane — it no longer looks like something to file.

## Test map

- `export_test.exs` 18 → **29**. `describe "PrometheusRegistry label coercion"` is
  eight tests; `describe "Export.Supervisor under poison signals"` is one.
- **`assert_valid_exposition/1` is the net.** Over the whole rendered body: every
  metric and label name matches the grammar, no label name repeats inside one
  `{}`, and the body is `String.valid?/1`. It parses the series line properly —
  escaped `\"` inside a label value means a naive split is wrong. Every
  pre-existing Prometheus test adopts it, so validity is asserted on the ordinary
  paths and not only the pathological ones.
- **The supervisor test starts `Export.Supervisor` unnamed**, via
  `Supervisor.start_link(Export.Supervisor, [])` inside `start_supervised!` with
  `restart: :temporary`. That gives the real child list and the real restart
  intensity while the application's own instance keeps the registered name with
  zero children (`prometheus_enabled: false` in test config). A first draft used
  a manual `on_exit` `Supervisor.stop(sup, :normal)`, which exits `shutdown` and
  fails the test in teardown — hand the lifecycle to ExUnit.
- **Two negative controls, because there are two mechanisms.** Value rule reverted
  to `to_string/1`: **16/29**, 13 red — the list and UTF-8 cases on *both*
  exporters fail with no exception (`left: <<1, 2, 3>>`, `left: <<255, 0, 254>>`,
  `the exposition body is not valid UTF-8`), the tuple cases raise. Name rule
  reverted to the identity: **25/29**, 4 red, every one reported by
  `assert_valid_exposition/1` itself (`malformed label name "a-b"`,
  `malformed label name ""`) and none raising. The second control is what proves
  the sanitizer engages: one that silently matched nothing would look identical.
- **The already-valid case is asserted beside every malformed one** —
  `%{path: "/x", tenant: "t1"}` renders `{path="/x",tenant="t1"}` with
  `coercion_count/1 == 0`, and `le` on a counter survives untouched. A sanitizer
  that rewrote everything would pass the malformed cases alone.
- `test/flezha/service/observability_test.exs` drives the real
  `render_tail_signal/2`: `do_tail_loop/3` reads the **caller's own mailbox**, so
  seeding it before the call exercises the render path with no live broadcast. The
  test owns `:pubsub_sink_enabled` *and* starts the `PubSubSink` process — both are
  off in `config/test.exs`, and inheriting whatever the previous module left made
  it pass on one seed and fail on another.

## Left standing, deliberately

- **Metric and source names** (`exposition_name/2`) — design D5, filed as KI-52.
  A label name carries a runtime term and must be handled leniently; a metric name
  is a developer-chosen atom known at registration and belongs refused loudly at
  `Registry.register_source/1`, a different capability.
- **Typed OTLP `AnyValue`s** — KI-46. After this change OTLP's value path is the
  single expression `any_value_string(Signal.metadata_text(value))`; KI-46 replaces
  that one call and keeps `metadata_text/1` for the key.
- **`otlp_client.ex:147`'s `to_string` pair** — operator-supplied `:otlp_headers`
  from config, not telemetry.
- **Label-value escaping beyond `\\`, `"` and `\n`** — a raw `\r` still passes
  through; the text format mandates only those three.
- **`truncation_count/1`'s doc claims `flz observe sources --json` reads it.**
  Nothing does; the claim is stale and was not in scope to fix.
