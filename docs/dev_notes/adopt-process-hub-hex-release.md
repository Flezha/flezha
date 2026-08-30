# `process_hub` moves from a GitHub branch to the hex release (`adopt-process-hub-hex-release`)

`{:process_hub, "~> 0.7.0"}` replaces `[git: …, branch: "dev"]`. Zero behaviour
change was the goal and the suites bear it out, but the interesting part is what
the pre-implementation comparison could not see.

## 0.7.0 IS the pinned code — for `lib/`, and only for `lib/`

The pin was `9966688b` (2026-08-23, `Merge branch 'master' into dev`), whose own
`mix.exs` declares `version: "0.6.1"`. Comparing the published 0.7.0 against the
installed checkout at that commit:

* 78 `.ex` files each side, none added, none removed;
* exactly one differs, `process_hub/hub.ex`, and the difference is a **pure
  relocation** — `for_workers/1` moved ~25 lines down. `diff <(sort a) <(sort b)`
  is empty, so the two files are an identical multiset of lines and no line
  changed.

That was measured twice, independently, and it holds. But it is a claim about
the `.ex` files under `lib/` — **the package's own `mix.exs` is not in that
set**, and it differs in three places, one of them load-bearing:

    -      {:elector, "~> 0.3.3", runtime: false},
    +      {:elector, "~> 0.3.4", runtime: false},

So `mix deps.get` does not succeed on a one-line `mix.exs` edit. It refuses:

    Because "the lock" specifies "elector 0.3.3" and "process_hub >= 0.7.0"
    depends on "elector ~> 0.3.4", "the lock" is incompatible …

`mix deps.unlock elector` is required, and **the lock diff is two lines, not
one**. The generalisable trap: a file-by-file source comparison of a dependency
proves nothing about the dependency's own dependency requirements. Diff its
`mix.exs` separately or the transitive move arrives as a surprise at resolve
time.

## elector 0.3.3 → 0.3.4 is a real behaviour change, on a live path

Not a version bump. `elector_service:async_call/2` was per-node
`erpc:send_request` + `erpc:wait_response(Ref, ?ERPC_TIMEOUT)`; 0.3.4 rewrites it
onto `erpc:multicall/3,5` and keeps only the nodes that answered. Two
consequences upstream states explicitly:

* the fan-out timeout is now **per batch**, not per node;
* a node that vanishes between candidacy and reply is dropped from the responses
  instead of raising `{erpc, noconnection}` — which used to crash
  `elector_commission` whenever a node left the cluster mid-election. A
  pre/post-election hook failing on a remote node no longer aborts the election
  either.

This matters here because `elector` is not inert: process_hub calls it from
`service/declared_children.ex` and `strategy/distribution/centralized_load_balancer.ex`,
and `declared_children` is one of the two upstream surfaces the branch pin
existed for. `runtime: false` in process_hub's declaration means the OTP
application is not auto-started; it does **not** mean the code is unused.

Nothing on this side needed following: the recovery and cold-boot mechanisms sit
on `Service.Recovery.await_normal/2`, `Service.Recovery.recovery_state/1`,
`Constant.Hook.recovery_state_changed` and `is_alive/1`, and every one of those
files is byte-identical between the pin and 0.7.0.

## `alter_child_data` is ours, not process_hub's

Filed as one of three upstream surfaces the pin existed for. It is
`HubManager.alter_child_data/2` (`lib/flezha/service/hub_manager.ex`), a callback
process_hub invokes, and appears nowhere in either version of the library. The
genuinely upstream surfaces are `declared_children` and `metadata_query`, both
present in 0.7.0.

## A clean `mix compile` was not the proof it looked like

The first `mix compile --warnings-as-errors` after the swap reported
`Compiling 7 files (.ex)` — but 17 files under `lib/` call `ProcessHub`. Elixir
tracks export dependencies, so an unchanged export digest correctly recompiles
only what needs it; that is indirect evidence, not the check. `touch` does not
force it either (the compiler compares content, not mtime). Only
`mix compile --force --warnings-as-errors` re-verifies every call site: **272
files, zero warnings.**

The gate was drilled rather than trusted: injecting
`ProcessHub.definitely_not_a_function(:x)` fails the compile with
`is undefined or private`, so a removed surface would have been loud. That is
what makes the green meaningful.

## The controller image was quietly non-reproducible, and now is not

`docker/prod/Dockerfile.controller` ran `mix deps.unlock process_hub` before
`mix deps.get --only prod`, so on a cache-missed layer the published image took
the **branch tip** rather than the committed lock. Against a version requirement
that same line means "re-resolve within `~> 0.7.0`" — still not the lock. It is
deleted: the image now builds what `mix.lock` says, which is the whole point of
the change. `make refresh-process-hub` keeps its name and is the one place that
deliberately re-resolves; its comment now says which of the two acts it performs.

## Found on the way, not fixed here

`mix deps.get` prints a hex security advisory against the **already-locked**
`postgrex 0.22.3`: EEF-CVE-2026-66838 / GHSA-3gww-3f36-2388 (MEDIUM), SQL
injection via the `:comment` option in `Postgrex.stream/4`. The lock line is
untouched by this change, so it is pre-existing and predates it; it is visible
now only because a resolve ran. Worth its own entry.

`docs/dev_notes/workflow-perf-campaign.md:83` still says of process-hub commit
`bb49842` "not pushed; bump the pin after pushing" — already contradicted by
`:444` in the same file, which lists it as done. Historical note, left alone.

## Numbers

Recorded at HEAD (`18fe491d`) before the swap, and again after, on the same box:

| run | before | after |
|---|---|---|
| `mix test` | 1836 passed, 119 excluded (72.7 s) | 1836 passed, 119 excluded (58.0 s) |
| `mix test --include system` | 1915 passed, 5 skipped, 35 excluded (139.0 s) | 1915 passed, 5 skipped, 35 excluded (128.8 s) |
| `mix test --include multi_node` | not recorded | 1845 passed, 110 excluded (63.6 s) |

Zero failures in all six runs (`mix test` ran a third time after the refine
pass, which touched only comments and prose: 1836 passed, 57.8 s), and no
member of the KI-18 flake population fired in any of them.

The system tier is not optional here: process_hub is the process supervisor
under the operative lifecycle, and its recovery, migration and reconcile paths
run only under `--include system`.

`--include multi_node` is not in the change's task list and was added because
of the elector finding: it is a separate excluded tag that `--include system`
does not cover, and it is where leader election actually runs
(`schedule/hub_takeover_test.exs`, `cluster/directory_two_node_test.exs`,
`boot/controller_colocation_test.exs`). Without it the one genuine behaviour
change in this swap would have gone unexercised. It has no before-figure
because the finding surfaced after the baselines were taken; the tier is
green now, which is what the elector move needed.
