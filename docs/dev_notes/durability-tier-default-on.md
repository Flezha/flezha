# durability-tier-default-on — implementation notes

Read before touching backend selection, the tier's artifact classes, or the
platform stores that now ride it.

## The refusal is not in `config/runtime.exs`

`config/runtime.exs` only *resolves* an unset `FLEZHA_DURABILITY_BACKEND` to
`:unset` (prod only — every other env keeps `:none`). The refusal itself is
`DurabilityTier.children/0`, which raises before the supervisor is built.

**Never move the parse into `Backend`.** `config/runtime.exs` is evaluated by
`Mix.Tasks.Loadconfig` *before* `lib/` is compiled on a cold checkout, so a call
to a project module there breaks `mix compile` and `mix deps.get` outright. The
three backend names being spelled in two places is the accepted cost.

`Backend.resolve/0` needs no `:unset` clause: its catch-all already answers
`:none`, so every reader outside `children/0` degrades to local-only rather than
crashing.

The refusal message is **printed to stderr and raised**. An application-start
failure reaches the operator as a BEAM crash report, which formats the exception
as an Erlang term and can truncate it — a refusal whose guidance is unreadable
is just a container that will not come up.

## Artifact classes are one table, read two ways

`Manifest.@classes` maps `tag => {kind, identity segments}`. `path_of/1` and
`ref_of_blob_key/1` both read it, so the key encoder and its inverse cannot
drift; `prefix/2` is the one prefix builder. Adding a class is a row here plus
an `Artifact.payload_files/1` clause (whole-payload) or a `wal_path/1` clause
(seq-addressed) — nothing in shipping, hydration or collection changes.

`Artifact.@whole_payload_kinds` deliberately stays separate: it says how a ref
is *read*, which is not the naming module's business.

## The parked set is the whole directory, on purpose

`{:dlq, {class, procedure}}` packs `entries.jsonl` plus every payload blob into
one artifact. A per-entry artifact would leave a resolved entry alive remotely
and resurrect it on hydration. Two consequences:

- **Every mutation re-ships.** `Dlq.Store.dir_changed/2` fires on park, drop,
  replay-mark and retention drop. It is the store's own durability barrier, so
  all three park call sites (stream channel, WAL writer, operative-forward) are
  covered by one hook.
- **An emptied set still commits a cut.** `Artifact.pack/1` answers `:empty`,
  and `ship_whole_payload/2` writes a manifest with `file: nil` rather than
  skipping. `Artifact.dlq_refs/1` is therefore NOT filtered by
  `has_local_state?/1` — an emptied directory has to stay in the sweep or the
  remote entry keeps naming the old blob forever.

## A takeover changes no bytes

`ship_whole_payload/2` must not short-circuit when the stored sha matches. Only
the blob *upload* is skipped (`upload_payload/4`); the entry still goes to
`commit_cut/6`, which is the one place allowed to conclude a cut is unchanged —
and it compares `owner`/`claim`, which a takeover changes and the bytes do not.
Skipping earlier makes a migrated operative's parked failures un-claimable.

## Where each store hydrates, and why it differs

- **Control-plane stores** (`{:control, {"authz" | "schedules", writer}}`) are
  per-writer replicas like the cspec, hydrated at boot by
  `Hydrator.hydrate_on_boot/0`, control stores **before** the cspec. The child
  sits ahead of `HubManager` in the application tree, which also puts it ahead
  of `AuthZ.Supervisor` and the transport — so a resurrected controller
  enforces from its first accepted verb and `RoleStore.admin_system_exists?/0`
  reads already-claimed.
- **Parked failures** hydrate on bind (`Hydrator.hydrate_operative/1`), never at
  boot. Boot-time DLQ hydration would give every node in a cluster a local copy
  of another node's parks and duplicate them in `flz dlq list`.

## The authz file list is explicit, and that is the security boundary

`Artifact.@control_stores` names the DETS files rather than scanning
`auth_dir`. `bootstrap.token` (a plaintext one-shot secret) and the
break-glass `REBOOTSTRAP` marker live in the same directory and must never
leave the node. A replacement controller does not need the token back: with
`roles.dets` restored, `Bootstrap.check_and_write/0` is a no-op.

DETS files ship while open. `:dets.sync/1` (which every store calls on each
mutation) leaves the file readable, and `AuthZ.Store.open/2` repairs or rotates
anything it cannot open — the same path a crash already takes.

## Schedules are in scope beyond the spec's literal list

The spec names DLQ and authz, then requires every other durable store to be in
scope or on a documented exclusion list whose stated content is "the audit log".
`schedules.dets` is neither re-derivable nor externally shipped, so it ships.
The exclusion list in `docs/remote_durability_tier.md` is therefore exactly the
audit log plus four non-stores (bootstrap token, rebootstrap marker, TLS
material, installed plugin artifacts).

## Test isolation: the sweep reads the developer's `app_data/`

`all_refs/1` enumerates real directories, so any test that starts a `Shipper`
picks up whatever the working copy holds. Two guards, both load-bearing:

- `DurabilityBackendStub.install/0` repoints `:authn_authz.auth_dir`,
  `:schedules.dir` and `:dlq.dir` at an empty tmp root and `uninstall/0`
  restores them. A case that wants one of those puts its own dir over it
  *after* `install/0`.
- `watchdog_test.exs` isolates `:process_hub_registry_root` alongside the state
  root; without it the RPO case alarms on the repo's own
  `app_data/process_hub/<hub>` and `refute_receive` flakes.

## Container validation (`test/scripts/durability_posture.sh`)

The prod-image half of the drill. Four things cost time to discover:

- A **named volume** for the tier root is created root-owned at mount time and
  the image runs unprivileged, so every ship fails with `EACCES` — and the
  shipper logs nothing for a failing backend, only telemetry. Use a host bind
  mount chmod 0777.
- `RELEASE_COOKIE` must be set or the release refuses to start distribution and
  every `flz` verb fails with `Cannot run --rpc-eval`.
- `bin/flzvm rpc` prints nothing on its own; pipe the expression to
  `IO.inspect/1`.
- Tokens are prefixed `flzt_`, not `flz_`.

Entity WAL recovery is not re-proved there — that is
`test/system/remote_durability_e2e_test.exs`, which destroys the state
directory of a live workflow and a live checkpoint stream against real runners.

## Known gap, out of scope

A *typo'd* backend value (`postgress`) still resolves to `:none` in prod —
silent local-only, the posture this change closes for the unset case. That is
`Backend.resolve/0`'s documented contract ("a typo must never crash the
controller into a boot loop") and was left alone.
