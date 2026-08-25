# A subscription sub-option has one declared type

## The proposal was wrong about where the pre-write check goes

It said `PluginManager.enable_claimed/2`, "before `write_enable_to_cspec/2`".
That is one step too late. `enable_plugin/2` reads the subscriptions itself —
`has_init_subscription?/1` matched `{:on_operative_init, _}` — and an operator
who typed the name as text produced `{"on_operative_init", %{}}`, which the
match misses. Every CLI-declared init-state plugin would have slipped past a
refusal that exists because init-state plugins cannot be attached after start.
`Table.canonicalise/2` therefore runs at the head of `enable_plugin/2`; the
refusal is now `refuse_init_subscription/1` in the same `with` chain, and its
bare-atom clause (`:on_operative_init -> true`) is gone — canonicalise refuses
a non-tuple subscription outright, and the old code raised on one anyway.

## `:position_not_applicable` is spec-mandated and the delta did not change it

The first cut had `canonicalise/2` answer an undeclared name with
`{:unknown_subscription_opt, kind, name}` for every kind, which turned
`position` on a `:fanout` into that error and broke the existing test. The
`operative-extensions` spec requires `{:error, :position_not_applicable}` for
`:fanout`, `:request` and `:timer`, and this change's delta does not touch that
requirement.

The fix is NOT an `if same_name?(:position, key)` inside the generic resolver.
The unordered kinds declare `position` **refused**:

    @position_refused {:refused, :position_not_applicable}
    fanout:  [position: @position_refused],
    request: [key: :string, position: @position_refused],
    timer:   [interval_ms: :int, position: @position_refused]

so the table answers every name for every kind and `coerce_sub_opt/3` has one
generic `{:refused, reason}` clause. The next sub-option that exists for some
kinds and not others declares its own refusal in the same place. The three
explicit `Keyword.has_key?(sub_opts, :position)` branches in
`validate_kind_specific/5` are deleted — one mechanism, one site. The test
covers ALL THREE unordered kinds, not just `:fanout`: moving a refusal from
three branches to one declaration has to prove three refusals.

## A map has no order, so it is emitted in declared order

`{"on_operative_init", %{"position" => "1", "timeout_ms" => "1000", "on_error"
=> "empty"}}` iterates in Erlang map order, so the same declaration produced a
different keyword list run to run — visible in `flz operative info` and in the
durable cspec. `declared_order/2` sorts map input by its index in
`@sub_opt_types` (undeclared sorts last, and is refused a step later). A keyword
list keeps the order it arrived in, which is what makes canonical input come
back unchanged.

## Two validators are deleted, not adapted

`validate_on_error/1` and `validate_sub_opts/2` are gone. The `{:enum, ["fail",
"empty"]}` declaration and `canonicalise/2`'s shape check are the sole authority
for both, and a second copy comparing `"fail"`/`"empty"` string literals is the
exact drift this change exists to remove. `validate_interval/1` and
`validate_timeout/1` collapse onto one `validate_positive/2` — after
canonicalisation they were the same function.

What survives is what a type cannot express:

* `validate_positive/2` — `> 0`. `interval_ms` has no entry in
  `@sub_opt_defaults`, so an absent one fails the guard; that is what makes it
  required. `timeout_ms` defaults to `5_000`.
* `validate_position/1` — `>= 0`. Coercion yields an integer, and a **negative**
  one still reaches `insert_by_position/3`, whose clause starts at 0 and whose
  next stop is a `FunctionClauseError`.
* the `:request` key-conflict check, which needs the live table.

`5_000` and `"fail"` used to be written at both the validator and the dispatch
chain. `@sub_opt_defaults` + `sub_opt/2` is now the single source for both.

## `parse_int/2` has two clauses, not three

`ProviderBehaviour.parse_int/2` matches `is_integer` and `is_binary` only, so
`parse_int(:bad, :timeout_ms)` raises. `coerce_sub_opt(:int, …)` guards the
shape before calling it and answers `{:invalid_subscription_opts, {name, value}}`
for everything else — which is what the pre-existing `timeout_ms: :bad` test
asserts.

## The kind table is checked at compile time

A kind added to `Flezha.Plugin.Host.@valid_kinds` without a `@sub_opt_types`
entry would `Map.fetch!`-raise at that kind's first subscribe, in production.
`@undeclared_kinds` fails the build instead. Verified by deleting the `:timer`
row: `** (RuntimeError) @sub_opt_types declares no sub-options for [:timer]`.

## What the live drive found

Driving the real path (`CliManager.enable_plugin/4` → `PluginManager` →
Overwatch → `CliManager.operative_info/2`, against a booted controller and a
boot Rust runner) proved 6.1/6.2 and turned up two things:

* **`operative_id` is an opts flag, not a `--X` flag.** Put in `plugin_flags` it
  reaches `init_provider/1`, and a provider with a strict `@known_opts`
  (websocket) refuses the whole enable with `unknown option(s) for websocket:
  operative_id`. `enable_plugin/4` reads it from either map, which hides the
  mistake until a strict provider is used.
* **The pre-existing rollback gap is real and reachable.** That failed enable
  still left `live_interval` in the operative's cspec with its (correctly typed)
  subscriptions: the failure was inside `forward_subscribe/3`, after
  `write_enable_to_cspec/2`, and `unregister_plugin/1` clears only the in-memory
  claim. The design records it as a follow-up broader than this change; this
  change removes the *typo* route into it, not the gap.

## Where each entry point now stands

| Entry point | Type-checked by |
|---|---|
| `flz plugin enable --extension=…` | `PluginManager.enable_plugin/2`, before the cspec write |
| `flz operative start --plugins` | `Table.subscribe/3` at bind |
| installed package enable | `PluginManager.enable_plugin/2` — the installer's `safe_enable_plugin/2` rides it |
| installed package **manifest** | `Manifest.require_subscriptions/1` still checks keyword-shape only; the types are caught at enable, not at install |
| direct `%Plugin{}` construction | `Table.subscribe/3` |

`SpecDecoder.atomize_keys/1` is untouched — it serves provider `opts`, which
have their own per-provider declarations.

## `key` is a string end to end

`Ctx.Msg.key`, the `request_subs` index and the `key` sub-option all move
together. Nothing in production sends `{:flezha_request, …}` — only tests — so
the type was free to correct. `dispatch_request/3` and `reply_unknown_key/1`
needed no change: they never guarded the type.

`config/config.exs` documented the sub-option as `{"name": "fetch_init_state"}`.
It is `key`; the documented invocation never worked (it answered
`{:invalid_subscription_opts, :missing_key}`), and now answers
`{:unknown_subscription_opt, :request, "name"}`.

## Test map

| File | Covers |
|---|---|
| `test/plugin/extension_test.exs` → `describe "subscription canonicalisation"` | all five sub-options from text; idempotence; each ill-typed value; an undeclared name; an unknown extension name proved un-interned with `String.to_existing_atom/1`; `position: "0"` on both ordered kinds and `"first"` / `"-1"` refused |
| same file → `"position on every unordered kind rejected"` | the moved `:position_not_applicable` mechanism, for `:fanout`, `:request` and `:timer` |
| `test/flezha/service/cli_manager_subscription_test.exs` | the `--extension` path through `CliManager.extract_subscriptions/2` into `subscribe/3` (integer, enum, string, none); the `--plugins` spec path incl. a quoted number; a CLI-declared `:request` key reachable at dispatch. Renamed from `cli_manager_trigger_test.exs` — `--trigger` is now one corner of it |
| `test/flezha/service/plugin_manager_test.exs` | a refused enable leaves no cspec entry; an unknown name interns nothing; a stored declaration holds the coerced value |
| `test/flezha/schedule/job_test.exs` | asserts the **boundary** shape (`{"on_output", []}`) — the decoded spec is canonicalised at subscribe, not at decode |

`extract_subscriptions/2` had to become public to be tested at all; it sits
beside `trigger_to_subscription/1` and `provider_opts/1`, which already were.
