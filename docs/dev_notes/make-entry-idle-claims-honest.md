# `entry_idle` — the capability was two layers, and the matrix priced it as one

`entry_idle` was the only entry in the sixteen-capability vocabulary that no
`test/system/` module was tagged for. The gate said so on every pull request
and passed anyway. Three manifests claimed it with `"entry_idle": []`; four
deferred it.

## The finding: two layers, and the audit's M/F boundary was in the wrong place

| layer | what | where |
| --- | --- | --- |
| **1 — the fact** | one `AtomicU64` per entry-channel **name**, stamped before the frame can be declined | `flz_core/src/entry_input.rs`; `ChannelDescriptor::observe_delivery` called from `routing.rs`'s `OperativeDispatchHandle::dispatch` |
| **2 — the policy** | `last_entry_delivery` + `on_entry_idle`, the `IdleObserver`, the episode latch, the `threshold/4` tick | `sdk/rust/src/entry_idle.rs` + `runner_handle.rs`; Java `EntryIdleWatcher` + `FlezhaRunner` |

Layer 1 sits in the shared core, so **every FFI veneer already has it** and each
plumbs the ABI read — but in Node, Ruby, PHP and Python that read reaches no
application surface and its only caller in the tree is a "reads 0 for an unknown
channel" ABI test. Layer 2 exists in Rust and Java alone. Elixir has neither:
its SDK is its own core.

**So the split is 2 implement / 5 do not, not 3 / 4.** Python's claim rested on
having zero fixtures for it — a manifest entry with an empty fixture list is not
evidence of a capability, and it made a zero-fixture capability look free. That
is the shape this change removes before it is copied into three more manifests.

## One channel name is one cell — and that is a trap for a fixture

`flz_core/src/entry_input.rs` interns the observation by the channel's **bare
name**: *one name is one observed input however many classes declare it*, pinned
by `one_name_is_one_cell_and_an_unmentioned_name_is_absent`. Deliberate, and not
a defect. But in the reference test app:

```
$ grep -rhno 'input::<[^>]*>("[^"]*")' test/apps/rust_runner/src/*.rs | sed 's/.*("\(.*\)")/\1/' | sort | uniq -c | sort -rn
     25 input
     16 in
```

A fixture watching `input` would be stamped by twenty-five other operatives and
would pass or fail on whatever else the fleet was running. The watched channel
is therefore `idle_input`, which nothing else declares.

## The same rule is what lets the e2e poll without sleeping

An `IdleObserver` takes no ctx and cannot emit, and nothing else changes during
the silence — so there is nothing to observe while waiting. `Process.sleep` was
not an option: charter §9 forbids it and, measured at this commit, the `sleeps`
ratchet is **2/2 in `test` and 8/8 in `test/apps`** — zero headroom in two
blocking areas, so a sleep is a red gate, not a style note.

**The fixture therefore declares two entry channels.** `idle_input` is watched;
`idle_report` is polled. Both fan into **one** procedure, which returns
`<absent_at_boot>:<episodes>:<resumes>`. Because cells are interned by
name, a frame on `idle_report` runs the reporting procedure and **leaves
`idle_input`'s stamp untouched** — the silence survives every poll that measures
it. No new mechanism: `resolve_entry_channel` already routes by the `"channel"`
metadata key, which two e2e tests already drive.

`absent_at_boot` is read **before `run()`**, because the stamp is taken ahead of
anything that can decline a frame — by the time any procedure body executes it is
already set, so "absent before the first frame" is observable there and nowhere
else.

**The one race**: `resumed` fires on the tick, up to a tick *after* the frame that
ended the episode, so the resuming activation's own report may still read zero.
The scenario waits for the counter instead of asserting on that output. It also
has to land inside the window before the channel's renewed silence starts a
legitimate second episode.

## Arming is not declaring

`main.rs` registers the observer unconditionally, beside forty-nine declarations,
and the two are not alike: **declaring an operative is passive until traffic
arrives, whereas the first `on_entry_idle` spawns the tick thread immediately**
and it runs for the process's life. Every runner the suite launches now carries a
watcher waking four times a second. Accepted — that app uses no environment
variables at all, so gating it would be new mechanism for a marginal gain — but
the next always-on fixture should be added knowing the difference.

The delivery path itself is unchanged: `entry_observation_for` interns a cell for
every entry channel already, and `observe_delivery` stamps it on every frame
whether or not an observer exists.

## `assert {pattern} = expr, "msg"` — the message is dead

Found because the red-first drill failed with a bare `MatchError` instead of the
message written for exactly that failure. The match raises before ExUnit can use
the second argument, so **every assertion of that shape has an unreachable
message**. The working shape is two lines:

```elixir
assert episode = poll_report(...), "the silence must be reported as an idle episode"
assert {true, 1, 1, 0} = episode
```

The first fires on a `nil` timeout and names what was awaited; the second reports
the pattern mismatch.

## The gate now fails, because a print is not a reader

The gate reported an untested capability and passed — which is how this one
survived a whole programme. Landing a `SHALL` that nothing enforces would have
repeated `pin-sdk-test-toolchains`'s lesson (*a pin nothing reads stops nothing*)
with a spec instead of a pin, so the report became a failure in the same change.
It could only be flipped once the last untested capability was covered.

**Shown failing first**, on a deliberately untagged capability, then restored:
re-tagging `connect_discovery_e2e_test.exs` to another vocabulary entry turned
the gate red naming `connect_discovery`, and **only** that test (19/20).

## A confound worth knowing about

The 250 ms tick thread now runs in every reference runner the suite launches,
including the four spawned by `FlezhaTest.Handover` — a documented
load-sensitive flake (`assert m.migrated >= 1` / `left: 0`). It was seen again
here, green alone. Ruled unrelated: the entry's mechanism is controller-side and
five sightings predate the thread. But if it recurs, **eliminate the arming
first** rather than re-deriving this.

## Nothing counts deliveries, and that fell out of the ratchet

The first cut had a separate feed procedure and a `fed` counter, so the test
could see the watched frame land. `duplicate_pairs` rejected it — a second
procedure class is a second copy of each language's boilerplate, and it moved
four blocking areas at once (`sdk/rust` 215→217, `sdk/java` 92→94, `test` 78→79,
`test/apps` 140→146).

The fix was not a smaller clone but a better design: **an episode already proves
the frame landed**, because a cold start never fires. So the counter went, and
with it the second class — both channels now fan into one unnamed procedure,
which is the documented way to put one class at two graph positions. The ratchet
found a redundancy the design had not.
