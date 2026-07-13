# Flezha architecture — the durable-compute kernel

Flezha runs user-authored stateful compute across a fleet of executors, and mirrors every
state change and every emitted output into a durable store on the controller. An executor is
disposable: it holds state in memory, and the controller holds the truth. Kill an executor and
its work resumes elsewhere from the last durable point.

This directory is the **architectural map** — the model, the invariants, and the diagrams. It
tells you how the pieces compose. For the mechanism-level detail behind any box, follow the
links; the deep docs live in [`../resilience/`](../resilience/) and
[`../sdk_protocol_spec.md`](../sdk_protocol_spec.md).

| Document | What it covers |
| --- | --- |
| **This file** | Roles, the workload model, the eight kernel invariants, process topology, on-disk layout, the failure model |
| [`kernel.md`](kernel.md) | The shared durable substrate: the input log, the atomic effect record, WAL + snapshot, the outbox, the watermark, output identity, the epoch rule, flow control, health |
| [`unbounded_profile.md`](unbounded_profile.md) | Streaming: per-segment recovery, the dead-letter queue, late-input correction, checkpoint intervals |
| [`bounded_profile.md`](bounded_profile.md) | Jobs and sessions: the operation tree, the op journal, the transactional outbox, saga compensation, the durable loop, suspension, rewind |

---

## Roles

- **Controller** — the Elixir control plane (`lib/flezha/`). It is the source of truth: it
  owns the durable store, routes input, decides where each operative runs, and heals the fleet.
  It scales horizontally — several controller nodes cluster over Erlang distribution, and a
  ProcessHub hub per namespace distributes operatives across them.
- **Executor** — the SDK process (Rust or Java) where user code runs. Executors **dial the
  controller**, never the reverse. They are disposable: state lives in memory, and the
  controller replicates it durably.
- **Source adapter** — where input comes from (a websocket, a feed, an HTTP endpoint). A
  pluggable edge, not a core component.
- **Sink adapter** — where output goes (Kafka, Postgres, anything). Also a pluggable edge.

A **segment's durable state is node-local**. It is not replicated across controller nodes;
cross-node state transfer happens only through the live handover an operative migration
performs. Cross-node WAL replication is a deliberate non-goal.

## The workload model

```
Operative  ─┬─ Operation ─┬─ Segment (key "BTC/USD")   ← state lives here
            │             └─ Segment (key "ETH/USD")
            └─ Operation ─── Segment (single)
```

- **Operative** — the unit you start and stop. It has a **kind**, frozen at start:
  `unbounded` (default) or `bounded`.
- **Operation** — a typed stage inside an operative. Declares `Input`, `Output`, `State`,
  `Change`, and `Clock` types.
- **Segment** — the per-key state-bearing slice of an operation. Durability is per segment:
  one WAL, one snapshot lineage, one outbox cursor.
- **Channel** — the typed edge between stages: entry (input from outside), inter-operation, or
  plugin-bound (output to a sink).

Full vocabulary: [`../glossary.md`](../glossary.md). The SDK authoring surface:
[`../operative_architecture.md`](../operative_architecture.md).

---

## Legend

Every diagram in this directory uses the same roles and colours.

```mermaid
flowchart LR
    SO["🌐 Source<br/>pluggable adapter"]:::src
    C["🧠 Controller"]:::ctrl
    E["⚙️ Executor"]:::exec
    SI["📤 Sink<br/>pluggable adapter"]:::sink
    D[("💾 Durable store<br/>input log · WAL · journal · outbox")]:::store
    SO -->|"normal data flow"| C
    C -->|"data"| E
    C -.->|"recovery / restore"| E
    C --> D
    C --> SI
    classDef ctrl fill:#dbeafe,stroke:#1e40af,color:#111
    classDef exec fill:#dcfce7,stroke:#166534,color:#111
    classDef sink fill:#fed7aa,stroke:#9a3412,color:#111
    classDef src fill:#ede9fe,stroke:#6b21a8,color:#111
    classDef store fill:#e5e7eb,stroke:#374151,color:#111
```

A dashed arrow is a recovery or restore path. 🌐 source · 🧠 controller · ⚙️ executor ·
📤 sink · 💾 durable store.

---

## Component topology

```mermaid
flowchart TB
    SRC["🌐 Source adapter<br/>websocket · feed · HTTP"]:::src
    SINK["📤 Sink adapter<br/>Kafka · Postgres · HTTP"]:::sink

    subgraph CTRL["🧠 CONTROLLER — source of truth"]
        direction TB
        INLOG[("💾 Input log<br/>offset-addressable<br/>effectively-once tier only")]:::store
        ROUTE["Routing · mailboxes · credit"]:::ctrl
        WAL[("💾 Segment WAL<br/>+ snapshots")]:::store
        JRN[("💾 Op journal<br/>bounded step progress")]:::store
        OBX[("💾 Outbox cursor<br/>key + epoch")]:::store
        REG["Registry — where each operative lives<br/>+ executor ownership"]:::ctrl
    end

    subgraph EXEC["⚙️ EXECUTOR — user code, state in memory"]
        direction TB
        OPV["Operative instance"]:::exec
        OPA["Operation A"]:::exec
        OPB["Operation B"]:::exec
        OPC["Operation C"]:::exec
        SB1["Segment k1<br/>state in memory"]:::exec
        SB2["Segment k2<br/>state in memory"]:::exec
        OPV --> OPA
        OPA -->|"fan-out"| OPB
        OPA -->|"fan-out"| OPC
        OPB --> SB1
        OPB --> SB2
    end

    SRC -->|"1 ingest"| INLOG
    INLOG -->|"2 input + offset — under source credit"| ROUTE
    ROUTE -->|"3 primary stream"| OPV
    OPV -->|"4 state change + output — under executor credit"| ROUTE
    ROUTE -->|"5 ONE atomic record, ONE fsync:<br/>state change + outputs + consumed offset"| WAL
    ROUTE -.->|"part of the same record"| OBX
    ROUTE -->|"6 op commit — bounded only"| JRN
    OBX -->|"7 flush AFTER the fsync"| SINK
    WAL -.->|"R restore state"| OPV
    JRN -.->|"R resume step"| OPV
    REG -.->|"routes · migrates · fences the write path"| ROUTE

    classDef ctrl fill:#dbeafe,stroke:#1e40af,color:#111
    classDef exec fill:#dcfce7,stroke:#166534,color:#111
    classDef sink fill:#fed7aa,stroke:#9a3412,color:#111
    classDef src fill:#ede9fe,stroke:#6b21a8,color:#111
    classDef store fill:#e5e7eb,stroke:#374151,color:#111
```

**Read it as:** input is journaled durably (1–2) before it is forwarded, then routed to the
executor (3). The executor sends back a state change and any outputs (4), under a byte credit
it must hold. The controller writes state, outputs, and the consumed input offset as **one
atomic record under one fsync** (5) — state and emitted effect can never tear apart. Bounded
operatives additionally commit step progress to an op journal (6). Outputs reach the sink only
after that record is durable (7). On a crash, the controller restores the segment's state and
the bounded operative's step position (R).

## One kernel, two profiles

```mermaid
flowchart TB
    subgraph PROF["PROFILES — two ways to use it"]
        UNB["UNBOUNDED<br/>endless stream, no barrier, low latency<br/>change-by-change durability<br/>per-segment local recovery<br/>effectively-once tier is opt-in"]:::exec
        BND["BOUNDED<br/>operation tree with a level barrier<br/>op-boundary journal + staged outbox<br/>saga compensation<br/>durable loop and suspension"]:::exec
    end
    subgraph KERN["SHARED KERNEL — durable compute"]
        K1[("Durable input log<br/>offset-addressable, adapter-independent")]:::store
        K2[("WAL + snapshot")]:::store
        K3[("Outbox: key + epoch")]:::store
        K4["Credit-based backpressure"]:::ctrl
        K5["Single-owner write path<br/>bind is the fence"]:::ctrl
        K6["Controller as the durable front<br/>evict · rehydrate"]:::ctrl
    end
    BEXTRA[("bounded only:<br/>op journal · durable inbox · saga")]:::store

    UNB --> KERN
    BND --> KERN
    BND --> BEXTRA

    classDef ctrl fill:#dbeafe,stroke:#1e40af,color:#111
    classDef exec fill:#dcfce7,stroke:#166534,color:#111
    classDef store fill:#e5e7eb,stroke:#374151,color:#111
```

The WAL and the snapshot mechanism are **shared** — a bounded operative's segments persist
exactly like an unbounded operative's. What is bounded-specific is the op journal, the durable
inbox, and saga compensation.

---

## The eight kernel invariants

**1. Source and sink are pluggable edge adapters.** The core assumes no concrete external
system. Kafka is one optional sink; it is not a source and not a component. The internal model
— input log, WAL, outbox, watermark, credit — is adapter-independent.

**2. One atomic durable record.** Under the effectively-once tier, each processed input
produces a single CRC-protected WAL record holding `{state changes, plugin outputs, consumed
input offset, dead-letter entry, event time}`, appended and fsynced as one unit. State,
emitted effect, and progress can never tear apart. The sink flush happens only after that
record is durable. See [`kernel.md`](kernel.md#the-atomic-effect-record).

**3. The durable input log is opt-in.** Default is at-least-once: no input-log file, no
outbox, no gating, byte-identical to a build that never heard of the tier. With
`effectively_once()` declared, every source is normalised into one offset-addressable durable
log and each segment recovers from its own consumed offset.

**4. Output identity, and the epoch rule.** Replay-stable output identity is
`(flz.segment, flz.seq, flz.key)`; the durable outbox identity adds `flz.epoch`. A crash
retry, a re-drive, or an executor takeover **keeps** the epoch — sink-side dedup absorbs the
repeat. Only a deliberate correction, an operator replay, or a bounded rewind **bumps** it, so
the corrected output passes dedup and overwrites the stale row by natural key. See
[`kernel.md`](kernel.md#the-epoch-rule).

**5. One owner writes a segment's WAL.** Executor identity is two layers — a durable stable
identity owned by the SDK, and a controller-minted session token per connection. Takeover is
newest-wins, and it **unbinds the old session before binding the new one**. The session token
rides every write and the WAL refuses one from a superseded session, so the owner pays no
per-write cost and a stale writer can never realign the log onto its own lineage. See
[`kernel.md`](kernel.md#ownership-and-fencing).

**6. Flow control parks, it does not drop.** Executor→controller byte credit is minted on WAL
**fsync** — durable drain, not append — so the un-fsynced buffer is hard-bounded by the window.
A sender out of credit parks. Under the tier a second credit loop gates source ingestion on the
minimum watermark across the operative's segments, so the slowest branch back-pressures the
source instead of growing the input log to disk-full. The controller→executor direction is
deliberately **not** credit: it drops the newest frame past 8 MiB, because parking there would
freeze a controller serving thousands of operatives behind one dead executor. See
[`kernel.md`](kernel.md#flow-control-the-three-loops).

**7. The minimum watermark is the retention signal.** The controller tracks `min(consumed
offset)` across an operative's segments and truncates the input log below it, subject to the
correction hold window. A permanently-failing payload must never freeze it: after bounded
retries the segment parks the payload durably to the dead-letter queue and advances its offset.
Data is never lost and never silently dropped. See
[`unbounded_profile.md`](unbounded_profile.md#dead-letter-queue).

**8. Health is producing-liveness, not channel-liveness.** `/healthz` and convergence answer
"is work flowing" — is the WAL sequence advancing — with an *is it being fed?* precondition, so
an idle segment (no input, flat is correct) is distinguished from a stuck one (fed but not
advancing). An empty intended set is never vacuously healthy: durable state on disk for an
operative absent from the live intent degrades health. See
[`kernel.md`](kernel.md#health-and-liveness).

---

## Process topology

The controller is one flat `:one_for_one` supervisor. Order matters and is load-bearing: the
signal handler installs OS handlers before anything can observe a `SIGTERM`; cluster formation
precedes the transport, so a hub is never created on a not-yet-clustered node; and the
convergence loop starts last, after the state subsystem it heals.

```mermaid
flowchart TB
    subgraph APP["Flezha.Supervisor — :one_for_one"]
        direction TB
        A1["Shutdown.SignalHandler<br/><i>first: SIGTERM handlers before anything else</i>"]:::ctrl
        A2["Service.EventManager · BootLoader · Service.Identificator"]:::ctrl
        A3["Convergence.Snapshot<br/><i>the published health ETS table</i>"]:::ctrl
        A4["Service.HubManager<br/><i>one ProcessHub hub per namespace</i>"]:::ctrl
        A5["Observability.Supervisor"]:::ctrl
        A6["Shutdown.Coordinator"]:::ctrl
        A7["Executor.FleetSupervisor · Executor.FleetManager"]:::ctrl
        A8["Cluster.Orchestrator<br/><i>before the transport, by design</i>"]:::ctrl
        A9["AuthZ.Supervisor"]:::ctrl
        A10["Transport.Server — one per transport (UDS, QUIC)"]:::ctrl
        A11["Service.PluginManager · Kafka.ProducerSup · Postgres.SinkSup"]:::ctrl
        A12["State.Supervisor · State.FsyncBatcher<br/>State.Retention · State.Watchdog · Dlq.Store"]:::store
        A13["Convergence.Loop<br/><i>the single self-healer</i>"]:::ctrl
        A14["Healthz.Server<br/><i>opt-in</i>"]:::ctrl
        A1 --> A2 --> A3 --> A4 --> A5 --> A6 --> A7 --> A8 --> A9 --> A10 --> A11 --> A12 --> A13 --> A14
    end

    subgraph PEROP["per operative instance"]
        OW["Operative.Overwatch<br/>a ProcessHub child — binds an executor,<br/>owns plugins, ingests input, journals it"]:::exec
    end
    subgraph PEREX["per executor session — a child of FleetSupervisor"]
        EX1["ControlUnit · ControlChannel"]:::exec
        EX2["StreamChannel × N — primary (0x01)"]:::exec
        EX3["StreamChannel × N — secondary (0x02)"]:::exec
    end
    subgraph PERSEG["per segment"]
        WW["State.WalWriter — owns one WAL file,<br/>the outbox cursor, and the owner token"]:::store
    end

    A4 -.->|"places"| OW
    A7 -.->|"accepts"| EX1
    A12 -.->|"starts on demand"| WW

    classDef ctrl fill:#dbeafe,stroke:#1e40af,color:#111
    classDef exec fill:#dcfce7,stroke:#166534,color:#111
    classDef store fill:#e5e7eb,stroke:#374151,color:#111
```

`Flezha.Operative.Overwatch` is the per-operative-instance GenServer and the pivot of the whole
control plane: it binds an executor, prepares and activates plugins, journals every inbound
input, and receives every output on its way to a sink. `Flezha.State.WalWriter` is the
per-segment durable writer. `Flezha.Convergence.Loop` is the **only** self-healer; it runs
`Intent → Probe → Diff → Planner → Heal` every 5 s and on declared events.

Placement uses two independent hashes that must not be conflated: ProcessHub's consistent
hashing picks which **controller node** hosts the Overwatch, and a rendezvous hash over the
operative id picks which **executor** it binds to.

---

## Connect, bind, declare

An executor dials in, presents a **stable identity** that survives its own restarts, and the
controller mints a per-connection **session token**. Everything the controller keys on — stream
routing, registry names, the WAL owner — is the session token; the stable identity is what makes
a reconnect a *binding swap* rather than a new executor.

```mermaid
sequenceDiagram
    participant EX as ⚙️ Executor
    participant CTRL as 🧠 Controller
    participant OW as 🧠 Overwatch (per operative)

    EX->>CTRL: dial + connect_control_channel<br/>{namespace, stable_id, factories}
    Note over CTRL: mint a session token for this connection<br/>if a prior session held this stable_id — unbind it FIRST, then bind the new one
    CTRL->>EX: AssignExecutorId
    CTRL->>EX: ConnectSocket {sockets_count}
    EX->>CTRL: open data streams — primary 0x01 in, secondary 0x02 out

    Note over OW: convergence sees an unbound operative and binds it
    OW->>EX: Ping (proves the reply path is intact)
    OW->>EX: AddOperative {operative_id, factory_id, kind, init_values, resume_segment_count}
    EX-->>OW: AddOperativeResponse {metadata}
    Note over OW: decode_declarations — the ONE seam where the SDK declares capability:<br/>effectively_once · ack_granularity · correction_window_ms · compensable_ops · leaf_ops
    OW->>EX: RestoreInitState per persisted segment (snapshot + WAL tail)
    Note over OW,EX: only now does input flow
```

Those five metadata keys are the complete set of SDK→controller declarations. An executor that
declares nothing — a Java executor, or a plain unbounded class — sends an empty map and the
controller keeps every default. Wire detail:
[`../sdk_protocol_spec.md`](../sdk_protocol_spec.md).

---

## On-disk layout

Everything durable lives under `app_data/`.

```mermaid
flowchart TB
    ROOT["app_data/"]:::store
    ST["state/"]:::store
    DLQ["dlq/"]:::store
    PH["process_hub/"]:::store
    AUD["audit/ · auth/ · certs/"]:::store

    CLS["&lt;operative_class&gt;/"]:::store
    IL["input.log<br/><i>durable input log — effectively-once tier</i>"]:::store
    DECL["effectively_once · checkpoint_ack<br/><i>declaration sidecars</i>"]:::store
    OPID["&lt;operation_id&gt;/"]:::store
    SEG["&lt;segment_key&gt;.wal<br/>&lt;segment_key&gt;.snap.&lt;seq&gt;<br/>&lt;segment_key&gt;.outbox.dedup"]:::store

    INST["&lt;instance_id&gt;/&lt;instance_id&gt;/"]:::store
    JRN["op.journal<br/><i>bounded step progress</i>"]:::store
    INB["inbox.wal<br/><i>bounded durable inbox</i>"]:::store

    DLQE["&lt;class&gt;/&lt;operation&gt;/entries.jsonl + &lt;id&gt;.bin"]:::store
    PHR["&lt;hub_id&gt;/registry.dets + cluster.healthy"]:::store

    ROOT --> ST
    ROOT --> DLQ
    ROOT --> PH
    ROOT --> AUD
    ST --> CLS
    CLS --> IL
    CLS --> DECL
    CLS --> OPID
    OPID --> SEG
    ST --> INST
    INST --> JRN
    INST --> INB
    DLQ --> DLQE
    PH --> PHR

    classDef store fill:#e5e7eb,stroke:#374151,color:#111
```

The path triple `(operative_class, operation_id, segment_key)` uniquely identifies a segment's
state. It does **not** include the executor id — an executor is an addressing concern, not a
durability one, so a restarted executor pod inherits the state that was already on disk.

The op journal and the durable inbox ride the *same* WalWriter substrate as a segment, under
two reserved segment keys (`_op_journal`, `_op_inbox`), which is why they sit at
`<instance_id>/<instance_id>/` and are skipped by the segment scan. Root is configurable
(`:state_root`, default `app_data/state`).

---

## The failure model

Each failure class on the left is closed by exactly one mechanism on the right.

```mermaid
flowchart LR
    subgraph RISK["What could go wrong"]
        direction TB
        R1["executor crashes — in-memory state gone"]:::risk
        R2["controller crashes — buffered changes not yet fsynced"]:::risk
        R3["output leaves before its state is durable"]:::risk
        R4["the same input is processed twice after a replay"]:::risk
        R5["the append buffer grows without bound under load"]:::risk
        R6["a poison payload freezes the watermark, or is silently dropped"]:::risk
        R7["a fresh controller zeroes a live segment with an empty restore"]:::risk
        R8["a superseded executor realigns the WAL onto a stale lineage"]:::risk
        R9["/healthz is green while the data plane is dead"]:::risk
        R10["a late input lands after the window it belongs to already closed"]:::risk
    end
    subgraph PROT["What closes it"]
        direction TB
        P1["WAL + snapshot — state mirrored to the controller"]:::prot
        P2["fsync batching + credit — the un-fsynced buffer is window-bounded"]:::prot
        P3["effectively-once tier — the flush is gated on the fsync"]:::prot
        P4["deterministic key + dedup at the sink"]:::prot
        P5["credit-grant flow control — park, never drop"]:::prot
        P6["dead-letter queue — retry, park durably, advance"]:::prot
        P7["baseline pull — CompactSegment instead of an empty push"]:::prot
        P8["single-owner write path — the WAL refuses a superseded session"]:::prot
        P9["producing-liveness — fed AND advancing, plus intent-mismatch"]:::prot
        P10["late-input correction — rewind, replay in event-time order, bump the epoch"]:::prot
    end
    R1 --> P1
    R2 --> P2
    R3 --> P3
    R4 --> P4
    R5 --> P5
    R6 --> P6
    R7 --> P7
    R8 --> P8
    R9 --> P9
    R10 --> P10

    classDef risk fill:#fee2e2,stroke:#991b1b,color:#111
    classDef prot fill:#dcfce7,stroke:#166534,color:#111
```

R3 and R4 are **permitted** in the default at-least-once mode — deduplication downstream is
the user's concern there. The effectively-once tier is what closes them.

### Durability guarantee summary

| Scenario | State loss | Output |
| --- | --- | --- |
| Graceful executor stop (SIGTERM / `disconnect()`) | none — the SDK ships final state | at-least-once |
| Executor SIGKILL, default tier | best-effort — up to the async WAL lag | at-least-once |
| Executor SIGKILL, effectively-once tier | none past the last fsync | effectively-once into a conforming sink |
| Controller graceful restart with a live peer | none — peer sync | at-least-once |
| Controller recovery-mode restart, sole node | none — DETS replay | at-least-once |
| Whole-cluster cold boot | recovered from DETS + the on-disk WAL | n/a |
| Bounded operative SIGKILL, durable | none past the last committed op | at-least-once |
| Bounded operative SIGKILL, persistence opted out | full — re-runs from the top | at-least-once |

The full scenario matrix and the operator playbook are in
[`../resilience/data_durability.md`](../resilience/data_durability.md).

---

## Where to go next

- The durable substrate every profile shares → [`kernel.md`](kernel.md)
- Streaming workloads → [`unbounded_profile.md`](unbounded_profile.md)
- Jobs, sagas, and agent sessions → [`bounded_profile.md`](bounded_profile.md)
- Fault-tolerance mechanisms in depth → [`../resilience/overview.md`](../resilience/overview.md)
- The wire protocol → [`../sdk_protocol_spec.md`](../sdk_protocol_spec.md)
- Authoring an operative → [`../operative_architecture.md`](../operative_architecture.md)
