# improve-controller-startup-time — implementation notes

The change was proposed as three startup savings. Two of the three did not
survive re-reading the code, and the survivor turned out to be a shutdown
bug wearing a startup bug's clothes.

## The migrator item had already shipped, and the delta contradicted it

`Flezha.Boot.StatePathMigrator` already carries `@layout_marker ".layout"` /
`@layout_version "v3-namespaced"` and short-circuits on it — it landed with
`finish-platform-authn-authz`. The original spec delta additionally required
that a *missing* state root write no sentinel. `migrate/0` deliberately does
the opposite: it stamps a fresh install, so a namespace dir can never later
be read as a legacy `runner_id` dir (the two layouts are both three levels
deep and structurally indistinguishable). Shipping that delta would have
specced the opposite of the shipped behaviour. Both the tasks and the delta
were dropped; the change ended up touching no production code at all.

## `su` was killing the drain the chaos harness exists to test

The controller boot chain was `user: "0:0"` → `chown -R` → `exec su -m …`.
util-linux `su` answers SIGTERM by signalling its child and force-killing it
a couple of seconds later. Measured on an isolated pair of containers with a
workload that traps SIGTERM and drains for 8s:

| pattern | pid | outcome | `docker stop -t 30` |
|---|---|---|---|
| `exec su -m` | **7** | `DRAIN-START` → `Session terminated, killing shell... ...killed.` | **2s** |
| `harness-boot.sh` | **1** | `DRAIN-START` → `DRAIN-COMPLETE` | 9s |
| `harness-boot.sh` as root | **1** | `DRAIN-START` → `DRAIN-COMPLETE` | 8s |

The chaos controller declared a 60s drain and a 90s `stop_grace_period`;
both were unreachable. The repo already knew the failure mode — the chaos
runners were fixed with `setpriv` and carry a comment naming the exact
`Session terminated` message — but the controller, the one process with a
drain to lose, was never fixed. **Any harness service that drops privileges
must `exec setpriv`, never `su`:** setpriv replaces the process image, so
the workload keeps PID 1 and receives the stop signal directly.

## The `chown` was a symptom of a missing mountpoint, not of a missing guard

Docker provisions an empty named volume root-owned **unless the image
supplies that directory**, in which case it copies the image's contents and
ownership. `Dockerfile.toolchain` knew this for exactly one path
(`/flezha/socket_data`) and had drifted even there — every compose file
mounts `/socket_data`. Six mountpoints were missing, so three call sites
carried a runtime workaround: the chaos compose, the healing compose, and
`fix_volume_permissions()` in `test/scripts/state_persistence.sh`.

Creating the mountpoints in the image deletes all three. Proof, on a
freshly-provisioned volume: a path absent from the image comes up
`owner=0 writable=NO`; every path the image now creates comes up
`owner=1000 writable=yes`. `docker/prod/Dockerfile.controller` had applied
the same contract for years — this is that pattern, not a new one.

NB `docker/prod/docker-compose.yml` mounts `flezha_data:/flezha/app_data/data`,
a path the prod Dockerfile does not create and no code reads. Root-owned and
unused; left for its own change.

## The perf numbers did not reproduce

The proposal's headline was 1.83s of `chown` per boot over 15,411 files. The
chaos build+deps volumes hold **2,365** files today, so the same `chown`
measures **26 ms** (guarded: 3 ms). The runner-side walk is the larger one —
72,364 files on the bind mount — but on a uid-1000 host it was always a pure
no-op walk. The durable per-boot win is the `mix deps.get` round-trip
(~1.4s plus a hard hex.pm dependency), removed by hashing `mix.lock` into a
stamp inside the deps volume — the volume it describes, so dropping the
volume drops the stamp, and the stamp is written only after a successful
fetch.

## One entrypoint, not four inline blocks

The guard/drop/deps/wait logic was near-identical across four services in
two compose files, so it lives in `scripts/harness-boot.sh`, beside the prod
`scripts/entrypoint.sh`. It is driven entirely by env vars —
`FLEZHA_HARNESS_PATHS`, `FLEZHA_HARNESS_WAIT_SOCKET`, and `MIX_DEPS_PATH`
(set means "this service runs mix", which is exactly when the lock guard
applies). Because it is a plain script, every branch is testable with
`docker run` outside compose — which is how both tables above were measured.

Two traps for the next reader:

- The preflight failure message must name **both** remedies. A named-volume
  mountpoint is fixed by rebuilding the toolchain image; a bind-mounted path
  gets its ownership from the host and can only be worked around with
  `FLEZHA_CONTAINER_USER=0:0`. Naming only the rebuild sends anyone on a
  foreign-uid host down the wrong path.
- `chaos/` is gitignored (`.gitignore:48`). The chaos compose gets the same
  treatment, but those edits are local and **no spec text may depend on that
  file**.
