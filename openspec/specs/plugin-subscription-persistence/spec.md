# plugin-subscription-persistence Specification

## Purpose

The plugin-subscription-persistence capability guarantees that plugin subscriptions added via `flz plugin enable` survive every Overwatch (re)start path — cold start, ProcessHub redistribution, in-place child-spec-args-change restart (e.g. `HubManager.alter_child_data/2` rewriting start args after a `compute_engine_id` change on operator-pod restart), agent process restart with intact ProcessHub state, and HotSwap migration to a peer agent — without operator intervention or any external command (no second `flz plugin enable`, no `start-asset.sh` re-run, no `flz cluster restore`).

The mechanism is a single source-of-truth shift: the ProcessHub child-spec's `Builder.opts[:plugins]` list is the authoritative, durable record of which plugins are subscribed to a processor. The runtime `Overwatch.state.extensions` table is a derived projection rebuilt from the child-spec on every `init/1` via the existing `subscribe_initial_plugins/2` path. `Flezha.Service.PluginManager.enable_plugin/2` and `disable_plugin/1` mutate both stores in lockstep — runtime first, then cspec on success — using the same transient-stripped serializable plugin shape that `Flezha.Plugin.Extension.Table.serializable_plugins/1` already produces for HotSwap handover. No new serialization format and no wire-format / protocol / SDK changes are introduced; the change is internal to the agent.

A small partial-failure window exists between the runtime subscribe and the cspec update. The contract chooses brief divergence over reverting a successful subscribe: the divergence is logged at `:error` level and self-heals on the next successful `enable_plugin` / `disable_plugin` call for that processor, which canonicalises the cspec from its current state. Periodic reconcile is intentionally not introduced; operators who need symmetric guarantees can run `flz plugin disable && flz plugin enable` after observing the error log.

## Requirements

### Requirement: Plugin subscriptions added via `flz plugin enable` SHALL survive every Overwatch (re)start

When `flz plugin enable <plugin_id> ... --Xprocessor_id=<pid>` returns success, any subsequent (re)start of the owning Overwatch GenServer — through any path (cold start of the processor, ProcessHub redistribution, in-place restart triggered by `HubManager.alter_child_data/2` rewriting start args, agent process restart with intact ProcessHub state, HotSwap migration to a peer agent) — SHALL re-subscribe that plugin to the new Overwatch instance without operator intervention or any external command (no second `flz plugin enable`, no `start-asset.sh` re-run, no `flz cluster restore`).

The mechanism is a single change in source-of-truth: the ProcessHub child-spec carries the authoritative list of plugins for a processor; the runtime `Overwatch.state.extensions` table is a derived projection rebuilt from the child-spec on every `init/1`. Operations that mutate plugin subscriptions update both stores in lockstep.

- `Flezha.Service.PluginManager.enable_plugin/2` SHALL, on a successful `{:plugin_subscribe, plugin_id, plugin}` reply from the owning Overwatch, atomically update the processor's ProcessHub child-spec via `ProcessHub.Service.ProcessRegistry.update/3` so that the `(plugin_id, Plugin.strip_transient(plugin))` pair is present in the `Builder.opts[:plugins]` list embedded in the child-spec's start args.
- `Flezha.Service.PluginManager.disable_plugin/1` SHALL, BEFORE issuing `{:plugin_unsubscribe, plugin_id}` to the Overwatch, remove the matching entry from the child-spec's `Builder.opts[:plugins]` list. If the cspec update fails, the unsubscribe SHALL NOT be issued and the failure SHALL be surfaced to the caller.
- The plugin entries written into the cspec SHALL use the same transient-stripped serializable shape that `Flezha.Plugin.Extension.Table.serializable_plugins/1` already emits for HotSwap handover. No new serialization format is introduced.
- `Flezha.Processor.Overwatch.init/1` reads `processor.opts[:plugins]` and re-subscribes each entry via `subscribe_initial_plugins/2`. The contract is unchanged from today; what changes is that `processor.opts[:plugins]` reliably reflects every preceding `enable_plugin`/`disable_plugin` operation, not just the plugins declared at `flz processor start` time.

#### Scenario: Operator-pod restart preserves dynamically-enabled plugins

- **GIVEN** an agent and a Rust SDK operator both running, with a processor `p1` registered via `flz processor start p1 crypto_trades --init_values='{}'`
- **AND** two plugin instances enabled via `flz plugin enable kafka_p1_trades processor_kafka_producer --Xprocessor_id=p1 --Xtopic=p1.trades.v1` and `flz plugin enable p1_ws processor_websocket --Xprocessor_id=p1 --Xurl=wss://example.com/feed`
- **AND** `flz processor info default p1` shows both plugins listed under "Plugins:"
- **WHEN** the operator container is restarted (`docker compose restart rust` in dev, `kubectl rollout restart daemonset/<sdk-daemonset>` in prod)
- **AND** the operator's reconnect loop completes and the new `compute_engine_id` is assigned to `p1` via the existing `compute_engine_migrate` / `alter_child_data` flow
- **THEN** `flz processor info default p1` SHALL list both `kafka_p1_trades` and `p1_ws` under "Plugins:" within 10 s of the new `compute_engine_id` being assigned
- **AND** the plugins' subscription opts (topic, URL, position, etc.) SHALL match their pre-restart values
- **AND** no `flz plugin enable` command SHALL have been issued between the restart and the assertion

#### Scenario: Agent process restart with peer agents preserves dynamically-enabled plugins

- **GIVEN** two agents in the same namespace cluster, with processor `p1` distributed to agent A and plugins `kafka_p1_trades` + `p1_ws` enabled on it
- **WHEN** agent A is restarted (process exit + supervisor relaunch, OR `kubectl rollout restart` on agent A's pod)
- **AND** ProcessHub redistributes `p1` (either back to A on its return, or to peer agent B during A's downtime)
- **THEN** the receiving agent's `Overwatch.init/1` SHALL re-subscribe both plugins by reading the cspec
- **AND** `flz processor info default p1` SHALL show both plugins listed against whichever compute_engine_id currently owns the processor
- **AND** the data plane (Kafka producer outputs, WS subscriber inputs) SHALL resume within 5 s of the receiving agent reaching ready

#### Scenario: HotSwap migration preserves plugins via the cspec

- **GIVEN** a processor `p1` with two enabled plugins running on agent A
- **WHEN** ProcessHub initiates a HotSwap migration of `p1` from A to peer agent B (e.g. due to a node-leave event)
- **AND** `prepare_handover_state/1` runs on A (existing behaviour) and `alter_handover_state/2` runs on B (existing behaviour)
- **THEN** the new Overwatch on B SHALL have both plugins subscribed
- **AND** at least one of the two paths SHALL be responsible for the subscription:
  1. `init/1` on B reads `processor.opts[:plugins]` from the cspec (set by the `enable_plugin` calls on A) and subscribes them — this path is added by this requirement.
  2. `alter_handover_state/2` rehydrates from `handover_state.plugins` — existing path; unchanged.
- **AND** if both paths execute, the result SHALL be idempotent: the second one observes the plugin already subscribed and skips, OR the first runs `subscribe_initial_plugins/2` and the second's `Table.teardown_all` + `Table.rehydrate` re-establishes the same plugins. Either outcome leaves `state.extensions` containing exactly the expected set with no duplicate workers.

#### Scenario: enable + immediate disable leaves the cspec empty

- **GIVEN** a processor `p1` with no plugins enabled
- **WHEN** `flz plugin enable plug_x ...` is run successfully
- **AND** then `flz plugin disable plug_x` is run successfully
- **THEN** the cspec's `processor.opts[:plugins]` for `p1` SHALL be empty
- **AND** any subsequent `init/1` of the Overwatch SHALL NOT re-subscribe `plug_x`

### Requirement: Partial-failure window between subscribe and cspec update SHALL self-heal on next operation

When `enable_plugin/2` succeeds in subscribing the plugin to the running Overwatch but then fails to update the cspec (e.g. transient `:no_response` from `ProcessHub.Service.ProcessRegistry.update/3` after the documented retry budget is exhausted), the subscribe SHALL NOT be reverted and the failure SHALL NOT propagate as an error to the caller — but the divergence between runtime state and cspec SHALL be logged at `:error` level with a message naming the processor_id, plugin_id, and reason.

The next successful `enable_plugin` or `disable_plugin` operation for that processor SHALL canonicalise the cspec — i.e. the cspec is rebuilt from the function applied to its current state, not assumed to reflect the runtime state.

#### Scenario: Cspec write fails after subscribe; next enable_plugin canonicalises

- **GIVEN** a processor `p1` with one plugin `a` already subscribed and persisted in the cspec
- **WHEN** `enable_plugin(b, ...)` is called and the Overwatch returns `:ok` for the subscribe
- **AND** the subsequent `ProcessRegistry.update` call fails with `:no_response` after exhausting all retries
- **THEN** the call SHALL log at `:error` with a message naming `p1`, `b`, and `:no_response`
- **AND** the call SHALL return `:ok` to the caller
- **AND** the cspec at this point lists only `a` (cspec stale; runtime has `a` and `b`)
- **WHEN** a subsequent `enable_plugin(c, ...)` is called and succeeds end-to-end
- **THEN** the cspec SHALL list `a`, `b`, and `c` — i.e. the helper rebuilds the list from the cspec's current state with `c` appended; `b` is restored from the runtime via the helper's mapping function reading the live extensions, OR `b` is recovered through the next Overwatch restart's cspec read of `a` only (data loss for `b`, accepted per the failure mode)

The exact recovery semantics for `b` in the partial-failure window are documented in `design.md` Decision 2: brief divergence is acceptable, periodic reconcile is intentionally not introduced. Operators who need symmetric guarantees can run `flz plugin disable b && flz plugin enable b` after observing the error log.
