# cross-sdk-perf-parity — implementation notes

The durable-path campaign's numbers were all Rust-runner numbers. This change
does not port anything — the audit found nothing to port — it closes the *proof*
gap and puts a detector on the one seam where Java can silently lag the kernel.

## The audit: which layer each campaign change sat in

- **Controller-only, so every runtime gets it free:** the outbox window refill,
  the InputLog-owned watermark, the fsync tick casting, the ack decoded once,
  the InputLog companion datasync, `send_io_confirming_many`, Overwatch sending
  the frame itself, the unsubscribed-chain early answer, the retention/`SeqIndex`
  fixes, async effectively-once ingest, sink-acknowledged delivery, the batched
  outbox reports (controller-internal, wire unchanged), the Kafka per-scheduler
  clients and in-flight window, and the Kafka `:block` default.
- **Shared kernel `flz_core`, so Rust and Java both get it:** `StagedInterval::fold`
  and `EntityItem::CloseInterval` (the batched interval records),
  `routing::in_activation`, the timer-promoter gate, the `HostOverflow` `Block`
  default. Java's declaration surface (`checkpointEvery` / `outputsAtCheckpoint`)
  landed in the same commit as the kernel change.
- **Rust host only:** `poll_pool.rs` wrapping `run_activation` in
  `in_activation`, plus the typed builder/dispatcher mirrors. Not a semantic
  gap: `install_inline_serve` was only ever called by the Rust pool, so the gate
  is neutral for Java — the JVM host's one-virtual-thread-per-activation model
  never needed the inline drain.
- **Elixir SDK:** its own core (not a poll host), inherits nothing from
  `flz_core`, defers the whole persistence family by design, and so owes nothing
  from the campaign today.

## What the proof gap actually was

Three things, all measurement: `mix bench.roundtrip` hard-wired `runtime: :rust`;
the five round-trip fixtures existed only in `test/apps/rust_runner`; and
`checkpoint_batched_outputs_e2e_test.exs` ran the Rust runner alone.

The third one is closed by `sdk-conformance-suite` rather than here: that file no
longer names a runtime at all, so `FLEZHA_E2E_RUNTIME=java mix test --include
system` *is* the Java arm. Enumerating `[:rust, :java]` inside the file — what
this change originally proposed — would have re-introduced exactly the per-file
runtime decision that change removed.

## `--runtime elixir` is refused above `--durability off`, at the task

Not at the fixture, not at the runner: `Mix.Tasks.Bench.Roundtrip.parse_runtime/2`
raises before anything starts, naming the deferred capability and the manifest.
An impossible axis should never reach a spawn — the same shape `Fleet.setup/3`
already used for `--runtime rust|java` on the native transport.

## The declaration-key detector caught one on its first run

`test/fixtures/host_class_decl_keys.txt` pins every `HostClassDecl` key; the core
and the Java builder each read it. It immediately found `procedure.max_entities`
— present in the core and the Rust builder, unspellable in Java. Closed here.
Details of both readers are in
[`sdk-conformance-suite.md`](sdk-conformance-suite.md).

## Measurement protocol

Arms interleaved *inside* each round, never one runtime's whole grid then the
next: the campaign's own null result (`kafka-producer-in-flight-window`) came
from a sweep whose apparent +50% was warm-up drift. Release binaries for all
three runners, one power state for the whole table, medians of three, zero loss
and zero duplicates asserted per run.

## Numbers

Round-trip median rec/s, `-p 2 -m 3000`, arms interleaved inside each round,
release binaries, one power state, idle box, three rounds (five for
`keyed`/`off`). Zero loss and zero duplicates on every run — the
`keyed`/`on` arm reports 6000 sent, 6000 received. `PERF-TEST.md` holds the
campaign's own Rust baselines; these are a same-session comparison.

| Tier | Rust | Java | Elixir |
|---|---|---|---|
| `identity` / `off` | 64.5k | 43.7k (-32%) | 48.9k (-24%) |
| `keyed` / `off` | 52.3k | 23.1k (-56%) | 57.2k (+9%) |
| `keyed` / `state` | 41.7k | 23.1k (-45%) | deferred |
| `keyed` / `on` | 20.9k | 15.7k (-25%) | deferred |

Run-to-run spread stays wide even idle (Rust `keyed`/`off` 40.9k–55.9k across
five rounds), so read the percentages as a direction, not a figure. The
regression bench (`mix bench -n 50 -m 200`, medians of three) puts Rust at
282k msg/s, Elixir at 184k (-35%) and Java at 112k (-61%).

**Java is 25–55% under Rust on every tier measured.** Per this change's own rule
that is named, not fixed here:

- `identity`/`off` and `keyed`/`off`, ~-35% to -55%: the suspected layer is the
  host, not the kernel or the controller — the JVM host runs one virtual thread
  per activation where the Rust pool drains inline, and a per-record path is
  where that shows. Follow-up: `java-host-dispatch-cost`.
- `keyed`/`on` (the effectively-once tier), ~-25%: the narrowest gap, which is
  what the audit predicts — that tier's cost is dominated by the controller's
  fsync and outbox path, which both runtimes share.

Elixir carries only the tier-off workloads by design, and lands within the
Rust spread on both. That is a v1 dispatch model buffering in process
mailboxes, measured on one runtime — not a claim about the others.
