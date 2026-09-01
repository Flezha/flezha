# `close-provider-value-validation` — the table refuses, and a crash is a refusal

KI-116's two mechanisms, fixed as mechanisms rather than as its eight sites.
`prod_loc` **7515 → 7515, exactly flat**; `duplicate_pairs` 13 → 12;
`lib/flezha` duplicate pairs 1 → 0; full suite 1960 passed.

## The two mechanisms

**`convert_value/2` was inert by construction.** `__before_compile__` injected
`defp convert_value(_field, value), do: {:ok, value}` at the end of every
provider, so a clause table that *looked* like a validator answered `{:ok,
anything}` for every shape its explicit clauses missed. The fix is
`@converted_opts`: a field listed there whose clauses do not match is refused; a
field no clause names still passes through. `__before_compile__` raises a
`CompileError` when a provider defines `convert_value/2` and declares nothing —
that raise is what makes this a mechanism instead of nine site patches, and it
refused all eight providers on first compile, forcing every declaration to be
made deliberately.

**A blanket refusing default was never available, and the count is why.** Nine
modules `use ProviderBehaviour`; **43 struct fields across 7 providers relied on
the catch-all** to pass a value through — `:url`, `:topic`, `:database`, `:uri`,
`:plugin_id` and their kind. Refusing by default would have refused all 43.

**`@converted_opts []` IS A VERDICT, NOT AN UNFINISHED DECLARATION.** Seven of
the eight providers declare `[]`; only `Websocket` declares fields
(`:ws_opts`, `:payload_metadata`, `:subscription_headers`). Those seven guard
every clause on the **field**, so their tables are total once the shared helpers
are, and an injected clause could never match. The injected refusal exists for
the value-guarded clauses alone. Mechanism 1 therefore splits in two: **helper
totality carries seven providers, the injected clause carries one.** A reader
who "fixes" an empty declaration by populating it has misread it —
`docs/plugins/lifecycle.md` says so in bold for that reason.

**The escape hatch was one layer below where KI-116 pointed.** The entry named
`Extension.Table.subscribe/3`; the fix belongs in **`run_init_provider/1`**,
because that is the single point invoking the callback and it is reached from
BOTH `do_subscribe/4` (enable) and `attempt_restart/3` (plugin heal) — the
second of which the entry never mentions. `invoke/4` now catches `:error`,
`:throw` and `:exit`, logs `Exception.format/3` with the stacktrace, and
`invoke_activate/4` routes through the same helper.

**A bare throw is a third escape flavour** KI-116 does not name:
`Kafka.parse_hosts/1` throws a plain string, and the sentinel-only `catch` let
it past exactly as it let a raise past. Catching only `:error` would have left
it standing.

**`attempt_restart/3` already had a function-level `rescue`**, so a *raise* on
the heal path was contained before this change — what escaped there was a throw
and an exit, and that rescue's own log named neither the callback nor the
provider. It is deleted: with the catch one layer down it was a second mechanism
for the same job, and the worse of the two.

## Gotchas that cost time

**`Identificator.string_to_pid/1` caught with ONE argument** — throws only —
while `String.to_existing_atom/1` **raises**, so the function's own documented
`{:error, "Invalid PID format"}` could never run. That is the real mechanism
behind KI-93's `BeamMsgForward` case, and it sits in `lib/flezha`, not in a
provider. The one-argument `catch` is the shape to grep for elsewhere.

**`OperativeForward` needed `@internal_opts` or every plugin heal would refuse.**
Calling `validate_opts/1` at the head of `init_provider/1` — site 5's whole fix —
refuses `:worker_pid` and `:routes`, which the provider resolves itself and
carries back on each re-prepare, and which are not `host_opts/0` keys. Derived as
`@injected_opts -- ProviderBehaviour.host_opts()` so the two lists cannot drift.
The test pinning it names the wedge in its own title; deleting it as redundant
reopens the heal refusal.

**The `:auth` keys are normalised to atoms at preparation**, so
`returned.auth[:password]` replaced `returned.auth["password"]` in
`KafkaSaslRedactionTest`. Stored string-keyed cspec rows still enable — every key
goes through `parse_enum(key, "auth key", @auth_keys)` on read. The redaction
guarantee is unchanged and still asserted.

**`parse_int/2`'s binary arm keeps its exact text**:
`provider_behaviour_test.exs` pins `"Invalid value for linger_ms: zz"` byte for
byte, and `refuse_value/2` reproduces the same bare-vs-`inspect` asymmetry so
both pinned messages survive.

## The instrument caught two things a human review would not have

**`--check` DOES NOT MEASURE `credo_warnings` OR `clippy_warnings`.** It prints
`not compared, unmeasured on this run: … in 36 area(s)` and still exits 0.
**`--lint` is the flag that measures both** (`refactor_metrics.py:567`). Run
with it, this change reported `BLOCKING lib/flezha/plugin credo_warnings: 0 → 2`
— `run_init_provider/1` at cyclomatic complexity 11 and nested depth 3, from a
`cond` wrapping the new `case`. Flattened into `settle_init/2`. **A bare
`--check` would have shipped that green**, and `credo_warnings 0` is a blocking
number in all 36 areas.

**`duplicate_pairs` 13 → 14 caught a duplication mid-change**: the same
`:recv_pid` pair written into two providers. The answer was `parse_pid/2` in the
shared layer beside `parse_int`/`parse_bool`/`parse_enum`, not an exemption.

## Test map

- `test/flezha/plugin/extension_test.exs` — mechanism 2: raise / throw / exit on
  the enable path and on the heal path, six tests.
- `test/flezha/plugin/provider_behaviour_test.exs` — mechanism 1's core, the
  `CompileError`, `parse_int/2` totality, and `OperativeForward`'s strictness
  (they live here, not in `operative_forward_test.exs`, which would have crossed
  800 lines).
- `test/flezha/plugin/kafka_test.exs` — the `:auth` closed sets, struct
  pass-through, and no-auth default.
- `test/flezha/plugin/provider/monitored_io_forward_test.exs` — new;
  `:heartbeat_interval` is converted **before** `GenServer.start_link/2`, because
  a linked child's crash is not catchable by mechanism 2 in a caller that does
  not trap.

## Not this change's

`kafka_test.exs`'s `elapsed_us < 1_000` assertion is [[KI-117]] — a wall-clock
budget 30 000× tighter than the property it defends, pre-existing at HEAD.
