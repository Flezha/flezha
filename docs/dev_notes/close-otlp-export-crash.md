# `close-otlp-export-crash` — the OTLP codec's attribute rule

## What the defect actually was

Two defects at the same three sites, and only one of them raises.

`kv_attribute(to_string(k), to_string(v))` fed every attribute through
`String.Chars`. `LoggerHandler.build_metadata/3` puts `logger: meta` (a map) and
`mfa: {M, F, A}` (a tuple) on **every** bridged log event, so `to_string/1`
raised `Protocol.UndefinedError` and the encode died — OTLP log export had never
worked. That is the loud half.

The quiet half is that `to_string([1, 2, 3])` is `<<1, 2, 3>>`. A list attribute
was rewritten into the binary of its elements, no exception, nothing to see. It
is the reason a `try/rescue Protocol.UndefinedError` would have been the wrong
fix: it catches the crash and preserves the corruption.

## Measurements that corrected the proposal

- **`:222` is not the resource site.** The proposal and design both call the
  three sites "resource, log-record, span". `:222` is inside
  `build_number_data_point/1` — the **metric** `NumberDataPoint` attributes.
  `default_resource/0` builds `service.name` and `service.version` from
  literals and never sees a signal's metadata, so it was never exposed.
- **A KEY can be a non-string, and crashed identically.** `Signal.new/1`
  validates that `:metadata` is a map (`validate_map!/2`) and places no
  constraint on its keys. `%{{:a, :b} => 1}` raises `String.Chars not
  implemented for Tuple` on the key; `%{[1, 2, 3] => 1}` encodes a key of
  `<<1, 2, 3>>`. Task 2.4 asked whether this was possible — it is, so keys take
  the same rule, and the spec delta was widened to say so (title, one paragraph,
  one scenario) rather than left understating the code.
- **The escalation is no longer inferred.** KI-44 read the three-restarts
  cascade off `Export.Supervisor`'s `:one_for_one` and said so. With the
  coercion reverted, `OtlpSink` is now *observed* dying inside `flush/1`
  (`otlp_sink.ex:124` → `encode_batch/1` → `kv_attribute/2`). The
  supervisor-level cascade above it is still not driven.
- **`file:` is a charlist on every bridged line, and `to_string/1` rendered it
  correctly.** The filed entry lists it with `[1, 2, 3]` as corruption; it is
  not — `to_string(~c"lib/x.ex")` is `"lib/x.ex"`. It is still an *ambiguity*
  the encoder cannot resolve, so under the new rule it reads `~c"lib/x.ex"`.
  That is a deliberate behaviour change on a value that used to read nicely.

## Load-bearing decisions

- **The coercion lives in `kv_attribute/2`, not at the three call sites.** All
  three sites changed — each lost its `to_string` pair — but the rule is stated
  once, at the funnel every attribute already went through, so a fourth site
  cannot reopen the question. `default_resource/0` passes literal binaries and
  pays one guard clause.
- **The function is `attribute_string/1`, not the design's `attribute_value/1`.**
  It renders keys too, and it returns a string; `attribute_value(key)` would
  read as a lie.
- **`encode_string/1`'s `to_string/1` fallback is deleted, not delegated.** With
  attributes coerced at the funnel, every caller of `encode_string/1` provably
  hands it a binary (literals, `Atom.to_string/1`, `exposition_name/2`,
  `attribute_string/1`). Leaving the fallback would have left the file's last
  unguarded `to_string/1` on a term of unproven type — the exact mechanism the
  change removes. The remaining single clause matches `encode_bytes/1`, which
  has been binary-only all along.
- **`default_resource/0`'s `Application.spec(:flezha, :vsn) |> to_string()`
  stays.** It converts a *known* charlist at its source and must keep producing
  `"0.1.0"`. Routing it through `attribute_string/1` would emit `~c"0.1.0"`.
  The distinction is the whole design: at the source you know what a charlist
  means; at the codec you do not.
- **`inspect/2` does not raise.** `Inspect.Opts` defaults to `safe: true`, so a
  struct with a broken `Inspect` implementation renders as `#Inspect.Error<…>`
  rather than propagating. That is what makes "encoding SHALL NOT raise" true
  and not merely likely.
- **`limit: :infinity, printable_limit: :infinity`** (design D2): `inspect/1`
  defaults to `limit: 50` and elides with `...`, and an elided attribute reads
  as a complete one.

## Test map

- `test/flezha/observability/export_test.exs`, `describe "OtlpCodec attribute
  values"` — six tests. The bridge case drives a real `LoggerHandler.log/2`
  event and captures the Signal off a `TestSinks.Counting` sink registered for
  `sources: [:logger]`, so the assertions run on the shape `build_metadata/3`
  actually produces, not a hand-made map (design D3).
- **The tests decode the protobuf.** `pb_fields/1` and friends walk the encoded
  body and read the `KeyValue` attributes back. A substring match for
  `<<1, 2, 3>>` over the whole body cannot tell a corrupted attribute from a
  coincidence — those bytes occur inside varints and inside the random 16-byte
  trace and 8-byte span ids.
- **The sink-liveness proof reuses the existing flush test.** Rather than a new
  test, the `OtlpSink time-trigger flush` signal gained
  `metadata: %{logger: %{…}, mfa: {…}}`. That test already asserted the buffer
  drains and the sink is alive; with the crash-shaped metadata it is the
  process-level regression test for the defect, and it goes red without the fix.
- **Task 3.4's negative control**, run twice (once before the sink test carried
  the metadata, once after): `attribute_string/1` reverted to
  `to_string(term)` gives **12 of 18** on `export_test.exs`, six red. The list
  case fails as `Refute with == failed, both sides are exactly equal / left:
  <<1, 2, 3>>` — no exception. Restored: 18 of 18, full suite 1836 passed.
- `Flezha.Observability.TestSinks.receive_signal/2` is `receive_until/2` moved
  out of `logger_handler_test.exs`, which was its only copy; a deadline
  replaced the recomputed-timeout recursion.

## Left standing, deliberately

- `prometheus_registry.ex:192` carries the identical `{to_string(k),
  to_string(v)}` shape — **KI-45**. Not the same fix: `PrometheusSink` filters
  to `kinds: [:metric]` so the Logger bridge never reaches it, and Prometheus
  label *names* must match `[a-zA-Z_][a-zA-Z0-9_]*`, so a coerced tuple key
  would be a well-formed string in a malformed exposition. That side needs a
  name rule before it can borrow the value rule.
- `otlp_client.ex:147` has the same two-`to_string` shape over `:otlp_headers`.
  Those are operator config, not telemetry; out of scope.
- OTLP's typed `AnyValue`s (`array_value`, `kvlist_value`) are the better wire
  and a protocol-shape change — **KI-46**.
- `scripts/check-overflow-naming.sh` is red at `0c4a695a` on KI-26's own prose,
  before this change touched anything — **KI-47**.
