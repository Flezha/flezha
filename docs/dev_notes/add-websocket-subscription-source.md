# Websocket subscription source (`add-websocket-subscription-source`)

- **`subscription_url` + `subscribe_template` / `unsubscribe_template` on
  `Plugin.Provider.Websocket`** — poll a URL publishing
  `{"version": <int>, "items": [<string>]}`, diff it against the applied set,
  send delta frames on the socket ALREADY OPEN. Controller-only; no SDK change,
  no wire change, no new control message.
- **`{{items}}` / `{{req_id}}` are the whole protocol surface.** Item strings are
  opaque — never parsed. That is what keeps a market-data requirement from
  becoming market-data code in the framework.
- **Commit is ack-gated and retry is CONVERGENCE, not retry state.** A delta
  folds into `applied` only when an inbound JSON object echoes its `req_id` and
  does not carry `"error"` / `"success": false`. Failed/unanswered → left
  uncommitted → next cycle re-derives an identical delta under a FRESH req_id.
  `expire_stale_inflight` drops the previous cycle's ids so a very late ack
  cannot commit a set the source moved on from. `subscription_ack_mode: "none"`
  commits on send for protocols with no correlation id.
- **GOTCHA — a re-assertion carries the reserved `req_id` 0.** Connect and the
  data-liveness re-subscribe re-send what is ALREADY applied; the in-flight
  space starts at 1, so their acks can never be mistaken for a pending delta's.
- **GOTCHA — `handle_connect/3` fires on EVERY reconnect.**
  `schedule_subscription_poll/1` cancels the previous timer before arming, else
  each reconnect leaves another poll chain running and the source is polled
  twice, then four times. Pinned by "reconnecting does not leave a second poll
  chain armed". (`schedule_idle_check/1` has the same latent shape —
  pre-existing, NOT fixed here.)
- **A failed poll is NO INFORMATION, never an empty desired set** — it can never
  unsubscribe. An explicitly empty `items` array IS a real instruction and does.
- **FIXED IN PASSING (correctness edge predating the feature):** the
  data-liveness watchdog resent `post_connect`, the BOOT-TIME payload, so any
  runtime change would have been silently reverted by the mechanism meant to
  heal a stalled subscription. `subscription_frames/1` is now the ONE answer to
  "what is this socket subscribed to", read by connect and both watchdog tiers.
- **Durability rides the EXISTING cspec path.** `persist_applied` casts
  `{:plugin_opt_persist, plugin_id, key, value}` to the host Overwatch, which
  updates BOTH `Table.put_plugin_opt/4` (what `attempt_restart/3` re-runs
  `init_provider/1` from — plugin restart / handover) AND the cspec via
  `PluginManager.persist_plugin_opt/5` → `RegistryUpdate.put_plugin_opt/3`
  (controller restart). The cspec write runs in a `Task` — it is a cluster-wide
  registry round-trip with its own retry ladder and must not stall the operative.
- **GOTCHA — `Table.put_plugin_opt/4` must update `subs` entries too.**
  `build_entry/3` COPIES opts into `provider_opts`, so updating only
  `table.plugins` leaves state that survives a restart but not a dispatch.
- **`with_host_identity/2` now also merges `:source_plugin_id`** (the provider
  needs to name its own cspec row). Adding a host key BROKE `http_poll`'s
  `reject_unknown` — which is why the host list now lives once, in
  `ProviderBehaviour.host_opts/0`.
- **Strict options are GENERATED now.** A provider declaring `@known_opts` gets
  both halves from `use ProviderBehaviour`'s `__before_compile__`: the `build/1`
  refusal AND `validate_opts/1`. Both are needed for different reasons —
  `Plugin.SpecDecoder` hands a `--plugins` spec straight to `init_provider/1`
  without ever building the struct. Three providers (`http_poll`, `websocket`,
  `operative_forward`) had hand-rolled copies; `operative_forward` still has no
  `init_provider` check wired (latent, documented, NOT changed here).
- **`Flezha.Plugin.Provider.HttpFetch`** is the ONE HTTP GET (extracted from
  `PollWorker`): streamed body cancelled at a cap, conditional GET on the ETag,
  verified TLS, `autoredirect: false` (a bearer token must not be replayed to
  whatever a redirect names). Also owns `validate_url/4` / `validate_headers/3`.
- **`Websocket.Subscription`** is the policy half — struct, `from_opts/1` (every
  refusal), `render/3`, `acknowledged?/1`, `desired_set/1`, `fetch_request/2` —
  with NO knowledge of Fresh. `Websocket` owns the loop and its timers.
- **Observability rides a new optional `inspect_state/1` provider callback**, so
  `flz operative info` needs no per-provider knowledge. NOT `flz plugin info` —
  that is the installed-`.fpz` surface and has no operative to report against.
- **GOTCHA — a fake WS server for tests: Mint's GUID is
  `258EAFA5-E914-47DA-95CA-C5AB0DC85B11`.** Mistyping it fails the upgrade with
  `:invalid_nonce`, which reads like a framing bug and is not one.
- **Tests**: `websocket_subscription_source_test.exs` (50 cases: refusals,
  in-place deltas, ack gating, durability, connect/watchdog convergence — all
  driven at the `handle_info/2` settled-poll seam),
  `websocket_reconcile_live_test.exs` (a REAL `:gen_tcp` RFC 6455 server + HTTP
  origin; asserts exactly ONE TCP connection accepted across an add and a
  remove — that is what pins "no reconnect"). Six mutations were run against the
  guards to confirm the tests bite.
- **Docs**: `docs/websocket_subscription_source.md`, `docs/glossary.md`,
  `docs/plugin_lifecycle.md`, root `CHANGELOG.md`.

