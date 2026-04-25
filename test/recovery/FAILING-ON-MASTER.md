# Recovery tests — expected failures on master

This file documents the red state of the recovery-test additions landed for
`fix-processor-state-rehydration` (flezha_market openspec change). Each
entry names the test, the assertion that fails on master (prior to the
accompanying code change), and the fix that makes it pass.

## `test/recovery/store_sync_on_event_test.exs`

### "external reader sees row the moment `put/3` returns"

* **Red on master**: `:dets.insert/2` returns after writing into DETS's
  in-memory buffers. With `auto_save: 5_000` and no explicit `:dets.sync/1`,
  a sibling `:dets.open_file` in read access sees an empty table for up to
  5 s. The assertion `assert [{"cid-sync", _}]` receives `[]`.
* **Green**: `lib/flezha/recovery/store.ex` — open DETS with
  `auto_save: :infinity`, call `:dets.sync(@table)` at the tail of every
  `put/3` path.

### "delete is also sync-on-event"

* **Red on master**: same mechanism — delete is buffered, external reader
  still sees the row.
* **Green**: `:dets.sync(@table)` after `:dets.delete/2` too.

### "idle store does not advance DETS mtime (no auto_save timer)"

* **Red on master**: after 6 s of idle, the `auto_save: 5_000` timer has
  fired at least once. `File.stat!(path).mtime` has advanced.
* **Green**: `auto_save: :infinity` disables the timer; idle writes are
  impossible because there are no writes.

## `test/recovery/dets_persistence_restart_test.exs`

### "processors and plugins survive a clean stop/start cycle"

* **May pass on master**: infrastructure-level test that is local to DETS
  behaviour; as long as a clean `GenServer.stop/2` flushes (which it does
  via `:dets.close/1` in `terminate/2`), the test passes on master too.
* **Value**: regression protection at the upstream level. The
  flezha_market-level failure is covered by
  `flezha_market/docker/dev/tests/dets-volume-survives-restart.sh`, which
  exercises the additional axis — the file must live on a persistent volume
  so it survives `docker compose down`.

## `test/processor/sibling_test.exs`

Two regression tests added on top of the existing suite:

### "string handler_key is atomized so `:on_message` dispatch matches"

* **Red on master**: CLI-supplied plugin opts arrive with a string value
  for `handler_key` (`"fetch_init_state"`). The `:on_message` extension
  indexes its `request_subs` table by the subscription's `:key` atom, so
  a string handler_key never matches a subscription registered under
  `:key :fetch_init_state`. The handler returns
  `{:error, {:unknown_key, :on_message, "fetch_init_state"}}`. Observed as
  `11 tests, 2 failures` with `git stash` of `sibling.ex`.
* **Green**: `normalize_handler_key/1` converts binary → atom before
  `send(target_pid, {:flezha_request, handler_key, ...})`. After unstash:
  `11 tests, 0 failures`.

### "map request_payload is JSON-encoded before dispatch"

* **Red on master**: the transport forwards `payload` verbatim to the
  target `:on_message` subscriber, which then hands it to the Java SDK
  over the `ProcessorMessage` control frame as `byte[]`. When `opts`
  embeds `request_payload` as a decoded JSON map (the common path when
  the CLI `--plugins` blob carries structured init-state parameters),
  master Sibling passes the map directly; `send/2` accepts any term but
  the downstream `ProcessorMessage{payload: bytes}` requires binary and
  everything downstream of that point is also typed `binary()`. Observed
  as the same `2 failures` in the red run above.
* **Green**: `coerce_request_payload/1` JSON-encodes maps before sending;
  binaries pass through unchanged.
