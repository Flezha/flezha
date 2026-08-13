# Flezha architecture — the durable-compute kernel

Flezha runs user-authored stateful compute across a fleet of runners, and mirrors every
state change and every emitted output into a durable store on the controller. A runner is
disposable: it holds state in memory, and the controller holds the truth. Kill a runner and
its work resumes elsewhere from the last durable point.

This directory is the **architectural map** — the model, the invariants, and the diagrams. It
tells you how the pieces compose. For the mechanism-level detail behind any box, follow the
links; the deep docs live in [`../resilience/`](../resilience/) and
[`../sdk_protocol_spec.md`](../sdk_protocol_spec.md).

| Document | What it covers |
| --- | --- |
| **This file** | Roles, the workload model, the eight kernel invariants, process topology, on-disk layout, the failure model |
| [`kernel.md`](kernel.md) | The shared durable substrate: the input log, the atomic effect record, WAL + snapshot, the outbox, the watermark, output identity, the epoch rule, flow control, health |
| [`stream_profile.md`](stream_profile.md) | Streaming: per-entity recovery, the dead-letter queue, late-input correction, checkpoint intervals |
| [`workflow_profile.md`](workflow_profile.md) | Jobs and sessions: the procedure tree, the proc journal, the transactional outbox, saga compensation, the durable loop, suspension, rewind |
| [`runner.md`](runner.md) | Inside a runner: the shared kernel, the poll boundary, per-entity exclusivity, and what each host SDK owns |

---

## Roles

- **Controller** — the Elixir control plane (`lib/flezha/`). It is the source of truth: it
  owns the durable store, routes input, decides where each operative runs, and heals the fleet.
  It scales horizontally — several controller nodes cluster over Erlang distribution, and a
  ProcessHub hub per namespace distributes operatives across them.
- **Runner** — the SDK process (Rust or Java) where user code runs. Runners **dial the
  controller**, never the reverse. They are disposable: state lives in memory, and the
  controller replicates it durably. Inside a runner there is one more split: transport,
  ordering, durability, retries, timers and the workflow driver live in a shared Rust kernel
  (`flz_core`), and the host SDK brings the authoring surface and the threads that run user
  code. See [`runner.md`](runner.md).
- **Source adapter** — where input comes from (a websocket, a feed, an HTTP endpoint). A
  pluggable edge, not a core component.
- **Sink adapter** — where output goes (Kafka, Postgres, anything). Also a pluggable edge.

An **entity's durable state is node-local**. It is not replicated across controller nodes;
cross-node state transfer happens only through the live handover an operative migration
performs. Cross-node WAL replication is a deliberate non-goal.

## The workload model

Three levels of containment, and one edge type that joins them:

```mermaid
flowchart LR
    SRC["🌐 Source"]:::src
    subgraph CTRL["🧠 Controller"]
        direction TB
        ROUTE["routing + placement"]
    end
    subgraph RUN["⚙️ Runner"]
        direction TB
        subgraph OPV["Operative — the unit you start and stop"]
            direction LR
            subgraph P1["Procedure &quot;parse&quot;"]
                direction TB
                E1(["Entity — key BTC/USD"]):::leaf
                E2(["Entity — key ETH/USD"]):::leaf
            end
            subgraph P2["Procedure &quot;aggregate&quot;"]
                direction TB
                E3(["Entity — single"]):::leaf
            end
        end
    end
    SINK["📤 Sink"]:::sink
    SRC -->|"entry channel"| ROUTE
    ROUTE --> P1
    P1 -->|"inter-procedure channel"| P2
    P2 -->|"plugin-bound channel"| SINK
    E1 -.->|"one WAL · one snapshot lineage<br/>one outbox cursor"| D[("💾 Durable store")]:::store
    E2 -.-> D
    E3 -.-> D
    classDef ctrl fill:#dbeafe,stroke:#1e40af,color:#111
    classDef sink fill:#fed7aa,stroke:#9a3412,color:#111
    classDef src fill:#ede9fe,stroke:#6b21a8,color:#111
    classDef store fill:#e5e7eb,stroke:#374151,color:#111
    classDef leaf fill:#dcfce7,stroke:#166534,color:#111
```

- **Operative** — the unit you start and stop. It has a **kind**, frozen at start:
  `stream` (default) or `workflow`.
- **Procedure** — a typed stage inside an operative. Declares `Input`, `Output`, `State`,
  `Change`, and `Clock` types.
- **Entity** — the per-key state-bearing slice of a procedure. Durability is per entity:
  one WAL, one snapshot lineage, one outbox cursor.
- **Channel** — the typed edge between stages: entry (input from outside), inter-procedure, or
  plugin-bound (output to a sink).

### The three entity modes

A procedure resolves to exactly one of three modes, chosen by what it declares. The mode
decides how many entities exist and when they are created — nothing else about the procedure
changes.

```mermaid
flowchart TB
    subgraph S["Single — declares nothing"]
        direction LR
        SI["input"] --> SE(["Entity (unkeyed)"]):::leaf
    end
    subgraph T["Static — declares entity_keys() + entities_for()"]
        direction LR
        TI["input"] --> TF{{"entities_for(input)<br/>fan-out"}}
        TF --> TA(["Entity &quot;eu&quot;"]):::leaf
        TF --> TB(["Entity &quot;us&quot;"]):::leaf
        TC(["Entity &quot;apac&quot;"]):::leaf
    end
    subgraph DY["Dynamic — declares entity_key()"]
        direction LR
        DI["input"] --> DK{{"entity_key(input)<br/>routes to exactly one"}}
        DK --> DA(["Entity &quot;BTC&quot;"]):::leaf
        DK -->|"created on first sight"| DB(["Entity &quot;SOL&quot;"]):::leaf
        DB -.->|"idle past entity_idle_ttl()<br/>evicted, recreates cold"| DX["∅"]:::gone
    end
    classDef leaf fill:#dcfce7,stroke:#166534,color:#111
    classDef gone fill:#e5e7eb,stroke:#374151,color:#111,stroke-dasharray:4 3
```

- **Single** — one unkeyed entity. The default when a procedure declares no entity opt-in.
- **Static** — a fixed keyset from `entity_keys()`; every entity exists from start. One input
  may fan out to **several** entities, which is why the selector is plural (`entities_for`).
- **Dynamic** — a stream keyset. `entity_key(input)` routes each input to exactly one
  entity, created lazily on first sight. Eviction is off unless `entity_idle_ttl()` is
  declared; when it is, an idle entity is dropped destructively and a later key of the same
  name cold-starts fresh. Workflow operatives refuse dynamic mode.

Full vocabulary: [`../glossary.md`](../glossary.md). The SDK authoring surface:
[`../operative_architecture.md`](../operative_architecture.md).

---

## Legend

Every diagram in this directory uses the same roles and colours.

```mermaid
flowchart LR
    SO["🌐 Source<br/>pluggable adapter"]:::src
    C["🧠 Controller"]:::ctrl
    E["⚙️ Runner"]:::exec
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

A dashed arrow is a recovery or restore path. 🌐 source · 🧠 controller · ⚙️ runner ·
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
        WAL[("💾 Entity WAL<br/>+ snapshots")]:::store
        JRN[("💾 Proc journal<br/>workflow step progress")]:::store
        OBX[("💾 Outbox cursor<br/>key + epoch")]:::store
        REG["Registry — where each operative lives<br/>+ runner ownership"]:::ctrl
    end

    subgraph EXEC["⚙️ RUNNER — user code, state in memory"]
        direction TB
        OPV["Operative instance"]:::exec
        OPA["Procedure A"]:::exec
        OPB["Procedure B"]:::exec
        OPC["Procedure C"]:::exec
        SB1["Entity k1<br/>state in memory"]:::exec
        SB2["Entity k2<br/>state in memory"]:::exec
        OPV --> OPA
        OPA -->|"fan-out"| OPB
        OPA -->|"fan-out"| OPC
        OPB --> SB1
        OPB --> SB2
    end

    SRC -->|"1 ingest"| INLOG
    INLOG -->|"2 input + offset — under source credit"| ROUTE
    ROUTE -->|"3 primary stream"| OPV
    OPV -->|"4 state change + output — under runner credit"| ROUTE
    ROUTE -->|"5 ONE atomic record, ONE fsync:<br/>state change + outputs + consumed offset"| WAL
    ROUTE -.->|"part of the same record"| OBX
    ROUTE -->|"6 proc commit — workflow only"| JRN
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
runner (3). The runner sends back a state change and any outputs (4), under a byte credit
it must hold. The controller writes state, outputs, and the consumed input offset as **one
atomic record under one fsync** (5) — state and emitted effect can never tear apart. Workflow
operatives additionally commit step progress to a proc journal (6). Outputs reach the sink only
after that record is durable (7). On a crash, the controller restores the entity's state and
the workflow operative's step position (R).

## One kernel, two profiles

```mermaid
flowchart TB
    subgraph PROF["PROFILES — two ways to use it"]
        UNB["STREAM<br/>endless stream, no barrier, low latency<br/>change-by-change durability<br/>per-entity local recovery<br/>effectively-once tier is opt-in"]:::exec
        BND["WORKFLOW<br/>procedure tree with a level barrier<br/>proc-boundary journal + staged outbox<br/>saga compensation<br/>durable loop and suspension"]:::exec
    end
    subgraph KERN["SHARED KERNEL — durable compute"]
        K1[("Durable input log<br/>offset-addressable, adapter-independent")]:::store
        K2[("WAL + snapshot")]:::store
        K3[("Outbox: key + epoch")]:::store
        K4["Credit-based backpressure"]:::ctrl
        K5["Single-owner write path<br/>bind is the fence"]:::ctrl
        K6["Controller as the durable front<br/>evict · rehydrate"]:::ctrl
    end
    BEXTRA[("workflow only:<br/>proc journal · durable inbox · saga")]:::store

    UNB --> KERN
    BND --> KERN
    BND --> BEXTRA

    classDef ctrl fill:#dbeafe,stroke:#1e40af,color:#111
    classDef exec fill:#dcfce7,stroke:#166534,color:#111
    classDef store fill:#e5e7eb,stroke:#374151,color:#111
```

The WAL and the snapshot mechanism are **shared** — a workflow operative's entities persist
exactly like a stream operative's. What is workflow-specific is the proc journal, the durable
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
log and each entity recovers from its own consumed offset.

**4. Output identity, and the epoch rule.** Replay-stable output identity is
`(flz.entity, flz.seq, flz.key)`; the durable outbox identity adds `flz.epoch`. A crash
retry, a re-drive, or a runner takeover **keeps** the epoch — sink-side dedup absorbs the
repeat. Only a deliberate correction, an operator replay, or a workflow rewind **bumps** it, so
the corrected output passes dedup and overwrites the stale row by natural key. See
[`kernel.md`](kernel.md#the-epoch-rule).

**5. One owner writes an entity's WAL.** Runner identity is two layers — a durable stable
identity owned by the SDK, and a controller-minted session token per connection. Takeover is
newest-wins, and it **unbinds the old session before binding the new one**. The session token
rides every write and the WAL refuses one from a superseded session, so the owner pays no
per-write cost and a stale writer can never realign the log onto its own lineage. See
[`kernel.md`](kernel.md#ownership-and-fencing).

**6. Flow control parks, it does not drop.** Runner→controller byte credit is minted on WAL
**fsync** — durable drain, not append — so the un-fsynced buffer is hard-bounded by the window.
A sender out of credit parks. Under the tier a second credit loop gates source ingestion on the
minimum watermark across the operative's entities, so the slowest branch back-pressures the
source instead of growing the input log to disk-full. The controller→runner direction is
deliberately **not** credit: it drops the newest frame past 8 MiB, because parking there would
freeze a controller serving thousands of operatives behind one dead runner. See
[`kernel.md`](kernel.md#flow-control-the-three-loops).

**7. The minimum watermark is the retention signal.** The controller tracks `min(consumed
offset)` across an operative's entities and truncates the input log below it, subject to the
correction hold window. A permanently-failing payload must never freeze it: after workflow
retries the entity parks the payload durably to the dead-letter queue and advances its offset.
Data is never lost and never silently dropped. See
[`stream_profile.md`](stream_profile.md#dead-letter-queue).

**8. Health is producing-liveness, not channel-liveness.** `/healthz` and convergence answer
"is work flowing" — is the WAL sequence advancing — with an *is it being fed?* precondition, so
an idle entity (no input, flat is correct) is distinguished from a stuck one (fed but not
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
        A7["Runner.FleetSupervisor · Runner.FleetManager"]:::ctrl
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
        OW["Operative.Overwatch<br/>a ProcessHub child — binds a runner,<br/>owns plugins, ingests input, journals it"]:::exec
    end
    subgraph PEREX["per runner session — a child of FleetSupervisor"]
        EX1["ControlUnit · ControlChannel"]:::exec
        EX2["StreamChannel × N — primary (0x01)"]:::exec
        EX3["StreamChannel × N — secondary (0x02)"]:::exec
    end
    subgraph PERSEG["per entity"]
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
control plane: it binds a runner, prepares and activates plugins, journals every inbound
input, and receives every output on its way to a sink. `Flezha.State.WalWriter` is the
per-entity durable writer. `Flezha.Convergence.Loop` is the **only** self-healer; it runs
`Intent → Probe → Diff → Planner → Heal` every 5 s and on declared events.

Placement uses two independent hashes that must not be conflated: ProcessHub's consistent
hashing picks which **controller node** hosts the Overwatch, and a rendezvous hash over the
operative id picks which **runner** it binds to.

---

## Connect, bind, declare

A runner dials in, presents a **stable identity** that survives its own restarts, and the
controller mints a per-connection **session token**. Everything the controller keys on — stream
routing, registry names, the WAL owner — is the session token; the stable identity is what makes
a reconnect a *binding swap* rather than a new runner.

```mermaid
sequenceDiagram
    participant EX as ⚙️ Runner
    participant CTRL as 🧠 Controller
    participant OW as 🧠 Overwatch (per operative)

    EX->>CTRL: dial + connect_control_channel<br/>{namespace, stable_id, factories}
    Note over CTRL: mint a session token for this connection<br/>if a prior session held this stable_id — unbind it FIRST, then bind the new one
    CTRL->>EX: AssignRunnerId
    CTRL->>EX: ConnectSocket {sockets_count}
    EX->>CTRL: open data streams — primary 0x01 in, secondary 0x02 out

    Note over OW: convergence sees an unbound operative and binds it
    OW->>EX: Ping (proves the reply path is intact)
    OW->>EX: AddOperative {operative_id, factory_id, kind, init_values, resume_entity_count}
    EX-->>OW: AddOperativeResponse {metadata}
    Note over OW: decode_declarations — the ONE seam where the SDK declares capability:<br/>effectively_once · ack_granularity · correction_window_ms<br/>compensable_procs · leaf_procs · compact_after_bytes
    OW->>EX: RestoreInitState per persisted entity (snapshot + WAL tail)
    Note over OW,EX: only now does input flow
```

Those six metadata keys are the complete set of SDK→controller declarations, and the shared
runner core emits them for every host language — a Java class and a Rust class declaring the
same thing send the same map. A class that declares nothing — a plain stream class — sends
an empty map and the controller keeps every default. Wire detail:
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
    OPID["&lt;procedure_id&gt;/"]:::store
    SEG["&lt;entity_key&gt;.wal<br/>&lt;entity_key&gt;.snap.&lt;seq&gt;<br/>&lt;entity_key&gt;.outbox.dedup"]:::store

    INST["&lt;instance_id&gt;/&lt;instance_id&gt;/"]:::store
    JRN["proc.journal<br/><i>workflow step progress</i>"]:::store
    INB["inbox.wal<br/><i>workflow durable inbox</i>"]:::store

    DLQE["&lt;class&gt;/&lt;procedure&gt;/entries.jsonl + &lt;id&gt;.bin"]:::store
    PHR["&lt;hub_id&gt;/registry.dets"]:::store

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

The path triple `(operative_class, procedure_id, entity_key)` uniquely identifies an entity's
state. It does **not** include the runner id — a runner is an addressing concern, not a
durability one, so a restarted runner pod inherits the state that was already on disk.

The proc journal and the durable inbox ride the *same* WalWriter substrate as an entity, under
two reserved entity keys (`_proc_journal`, `_proc_inbox`), which is why they sit at
`<instance_id>/<instance_id>/` and are skipped by the entity scan. Root is configurable
(`:state_root`, default `app_data/state`).

---

## The failure model

Each failure class on the left is closed by exactly one mechanism on the right.

```mermaid
flowchart LR
    subgraph RISK["What could go wrong"]
        direction TB
        R1["runner crashes — in-memory state gone"]:::risk
        R2["controller crashes — buffered changes not yet fsynced"]:::risk
        R3["output leaves before its state is durable"]:::risk
        R4["the same input is processed twice after a replay"]:::risk
        R5["the append buffer grows without bound under load"]:::risk
        R6["a poison payload freezes the watermark, or is silently dropped"]:::risk
        R7["a fresh controller zeroes a live entity with an empty restore"]:::risk
        R8["a superseded runner realigns the WAL onto a stale lineage"]:::risk
        R9["/healthz is green while the data plane is dead"]:::risk
        R10["a late input lands after the window it belongs to already closed"]:::risk
    end
    subgraph PROT["What closes it"]
        direction TB
        P1["WAL + snapshot — state mirrored to the controller"]:::prot
        P2["fsync batching + credit — the un-fsynced buffer is window-workflow"]:::prot
        P3["effectively-once tier — the flush is gated on the fsync"]:::prot
        P4["deterministic key + dedup at the sink"]:::prot
        P5["credit-grant flow control — park, never drop"]:::prot
        P6["dead-letter queue — retry, park durably, advance"]:::prot
        P7["baseline pull — CompactEntity instead of an empty push"]:::prot
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
| Graceful runner stop (SIGTERM / `disconnect()`) | none — the SDK ships final state | at-least-once |
| Runner SIGKILL, default tier | best-effort — up to the async WAL lag | at-least-once |
| Runner SIGKILL, effectively-once tier | none past the last fsync | effectively-once into a conforming sink |
| Controller graceful restart with a live peer | none — peer sync | at-least-once |
| Controller recovery-mode restart, sole node | none — DETS replay | at-least-once |
| Whole-cluster cold boot | recovered from DETS + the on-disk WAL | n/a |
| Workflow operative SIGKILL, durable | none past the last committed proc | at-least-once |
| Workflow operative SIGKILL, persistence opted out | full — re-runs from the top | at-least-once |

The full scenario matrix and the operator playbook are in
[`../resilience/data_durability.md`](../resilience/data_durability.md).

---

## Where to go next

- The durable substrate every profile shares → [`kernel.md`](kernel.md)
- Streaming workloads → [`stream_profile.md`](stream_profile.md)
- Jobs, sagas, and agent sessions → [`workflow_profile.md`](workflow_profile.md)
- Inside a runner, and writing a new SDK → [`runner.md`](runner.md)
- Fault-tolerance mechanisms in depth → [`../resilience/overview.md`](../resilience/overview.md)
- The wire protocol → [`../sdk_protocol_spec.md`](../sdk_protocol_spec.md)
- Authoring an operative → [`../operative_architecture.md`](../operative_architecture.md)
