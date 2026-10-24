# Erlang VM arguments

The VM the node runs in is configured before the application starts, so none of
these can be a row of `lib/flezha/config/keys.ex` and `config/runtime.exs` never
sees one. They behave like the
[controller environment variables](environment_variables.md) in the way that
matters to a user (set them in the process environment or the deploy manifest,
restart to change one), and they have a page of their own because no row of
that table can express them.

## What the release sets

The launcher reads two files: the start commands take
`vm.args`, and `rpc` and `remote` take `remote.vm.args`, the short-lived node
those two commands start; no `flz` verb starts one. A gate holds these tables and the two
templates to each other, so a flag cannot ship undocumented.

<!-- vm-args:rel/vm.args.eex:begin -->

| `rel/vm.args.eex` | Why |
|---|---|
| `+sbwt short` | A scheduler with no work stops spinning for it sooner. Measured against the default over paired rounds of the regression bench: **21% less CPU** for the same run, with no throughput difference either window could resolve. It is `short` and not `none` because `none` saves barely more and costs four times as much setup latency; both numbers are in the table below. |
| `+sbwtdcpu short` | The same, for the dirty CPU schedulers. |
| `+sbwtdio short` | The same, for the dirty IO schedulers, which is where every transport NIF call that can block is scheduled. |
| `+Q 65536` | The ceiling on ports: plugin sockets, runner connections, distribution. Pinned because the default moves with whichever ERTS is underneath, 65 536 on the image's OTP 27 and 1 048 576 on OTP 29, so the ceiling is a decision rather than a property of the runtime that happened to be built in. The table is allocated up front at about 16 bytes a slot, so pinning at the image's own value costs nothing there and returns **15 MB of RSS** on the larger default. Raise it if port exhaustion ever appears. |
| `+C multi_time_warp` | Already the default on both OTP 27 and OTP 29, pinned so a future ERTS cannot move it. The keepalive roster compares a monotonic epoch, which is only well defined in this mode. Costs nothing: it is the mode the VM already chooses. |
| `+Bd` | Ctrl-C ends an attached controller at once. With the default break handler SIGINT opens the `BREAK:` menu, which pauses the whole VM: the prompt comes back while the node stays alive holding its name and its socket, and answers no `flz` verb. It is a hard stop with no drain, recovered on the next boot as a crash would be; SIGTERM and `flz controller stop` drain. |

<!-- vm-args:rel/vm.args.eex:end -->

<!-- vm-args:rel/remote.vm.args.eex:begin -->

| `rel/remote.vm.args.eex` | Why |
|---|---|
| `+S 1:1` | This node connects to the controller, evaluates there and exits. It was being given the host's full scheduler complement to do that. |
| `+SDio 1` | It opens no transport, so the dirty IO pool has nothing to serve. |
| `+Q 65536` | The same ceiling, pinned for the same reason. This node opens two ports and was allocating a table for a million of them: **15 MB of RSS**, and a tenth of the node's startup, on every start. |
| `+Bd` | Ctrl-C ends the node. With the default break handler SIGINT opens the `BREAK:` menu and the node stays up, so a call that streams could not be interrupted. |

<!-- vm-args:rel/remote.vm.args.eex:end -->

## Which spelling wins

Three environment variables claim to set VM flags and
only two of them do. Measured against an args file setting `+S 3`:

| What you set | Result |
|---|---|
| nothing | `3`, the args file is read |
| `ERL_FLAGS="+S 5"` | `5`, wins |
| `ERL_ZFLAGS="+S 5"` | `5`, wins |
| `ELIXIR_ERL_OPTIONS="+S 4"` | `3`, **ignored** |
| both `ELIXIR_ERL_OPTIONS` and `ERL_FLAGS` | `ERL_FLAGS` |

One rule explains all of it: the flags are concatenated and the last occurrence
wins, `ELIXIR_ERL_OPTIONS` is placed before the args file and `ERL_FLAGS` after
it. So `ELIXIR_ERL_OPTIONS` still adds a flag the file does not set, which is
how `rel/env.sh.eex` adds TLS distribution, and it can never override one the
file does set. **Use `ERL_FLAGS`.**

## Setting them

| Deployment | Route |
|---|---|
| A pulled image | `docker run -e ERL_FLAGS="+S 4:4"`, or the commented `ERL_FLAGS` key in `deploy/kubernetes/flezha-configmap.yaml`, which the pod already reads through `envFrom` |
| A read-only root filesystem | Mount an args file and set `RELEASE_VM_ARGS` to its path. The image's own file cannot be edited in place, and the shipped DaemonSet sets `readOnlyRootFilesystem: true` |
| Bare metal | Either, plus editing the assembled `releases/<vsn>/vm.args` directly |

Two things to know before you set one. `ERL_FLAGS` is read by every `flzvm`
invocation, so exporting it in a profile also enlarges the nodes `flzvm rpc`,
`flzvm remote` and the few `flz` verbs that run `flzvm` start. And a flag the VM does not accept refuses
the boot naming itself (`bad scheduler busy wait threshold: banana`), which
under an orchestrator is a crash loop rather than a setting quietly dropped.

## What the VM already handles

The usual advice to rescue the BEAM from
cgroups does not apply here. Measured on the shipped image on an 8-core host:

| Container | schedulers | online | dirty IO |
|---|---|---|---|
| unconstrained | 8 | 8 | 10 |
| `--cpus=2` | 8 | **2** | 10 |
| `--cpuset-cpus=0,1` | 8 | **2** | 10 |

The VM reads the quota for `schedulers_online` on its own, so a CPU limit alone
does not need a `+S`. What it does not read is the dirty IO pool, which stays at
ten threads whatever the limit.

## Flags this release does not set

Each was either measured and found
not to matter, or is a decision that belongs to a deployment rather than to a
default.

| Flag | Verdict |
|---|---|
| `+sbwt very_long` and friends | **Measured harmful.** 33% more CPU than the default, and the throughput direction was worse in 8 of 8 paired rounds on both windows. The common advice to spin harder for throughput is wrong on this workload, where the controller shares a machine with the runners it feeds |
| `+sbwt none` and friends | Measured 22% less CPU against the default, one point better than the `short` that ships, and it pays for that point with the whole latency budget: plugin setup +204%, warmup +122%, relay creation +23%, where `short` costs +43%, +28% and +3.5%. Worth setting only where CPU is the scarce resource and nothing waits on a control-path round trip |
| `+sbwtdio none` alone | Measured 4.5% less CPU, consistent across rounds, and free in wall time. Most of the saving is on the normal schedulers, not the dirty IO pool, so this is the conservative fraction of the flag above rather than an alternative to it |
| `+SDio` | Measured flat: `+SDio 4` moved CPU 1.1% and neither throughput window past its own noise. Worth setting only where the dirty IO pool's ten threads are being charged against a small CPU quota |
| `+S` | Only where the pod's CPU limit is not the number you want; the VM already follows the limit |
| `+zdbbl` | The distribution buffer bound, 1 MiB by default. Namespace hubs and WAL transfer ride distribution, so this is the flag to raise if a sending process is seen blocking on a busy dist port. No measurement here supports moving it, and it costs memory per connection |
| `+stbt` | Scheduler binding. Worth trying on a dedicated machine only; wrong anywhere a quota or cpuset can move underneath the VM |
| `+hms`, `+hmbs` | Per-process heap sizes. The cost scales with process count, and this controller runs a process per Cell |
| `-env ERL_FULLSWEEP_AFTER` | A memory-drift remedy chosen against a measured series, not a default |

The measurement protocol and the numbers behind every verdict here are in
[the benchmarking guide](https://github.com/flezha/flezha/blob/main/docs/internal/operations/benchmarking.md).
