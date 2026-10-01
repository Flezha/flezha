# operator-inside-an-operator Specification

## Purpose
An edge into an operator class: a flow class is inlined into the outer graph, and a steps class
runs as one stepped scope per edge key inside the outer instance, with its own state, journal,
input log, sleep and undo, and its exit output continuing on the outer edge.

## Requirements
### Requirement: An edge SHALL be able to end in an operator class

In every SDK, `into` SHALL accept an operator class as well as a tasklet. The inner class
SHALL run by its own `run` setting. An inner class whose run is `flow` SHALL be inlined at
registration: its tasklets and channels join the outer graph with its names prefixed, no
scope is created, and the outer graph SHALL be judged as one graph by the existing
topology rules. An inner class whose run is `steps` SHALL run as a stepped scope inside the
outer instance. A tasklet before or after the edge SHALL run exactly as it would with a
tasklet there.

#### Scenario: A flow class is inlined

- **WHEN** a flow class `Cleanup` with three tasklets is the target of `into` in another flow class
- **THEN** the outer registration SHALL declare the three tasklets under prefixed names, `flz operator info` SHALL list them as tasklets of the outer instance, and no journal SHALL exist for them

#### Scenario: A steps class becomes a scope

- **WHEN** a steps class `Session` with `each: per_input` is the target of `into` after `key_by` in a flow class `Gateway`
- **THEN** each key's first message SHALL create one inner instance of `Session` with its own journal and pass sheet, and `Gateway`'s tasklets before and after the edge SHALL show no pass in their telemetry

### Requirement: A stepped scope SHALL be identified by instance, inner class and key

The task journal and the pass sheet SHALL be keyed by the scope `(instance, inner class,
key)`. A whole-operator steps run SHALL be the scope with the inner class absent and the
key absent, read by the same code path. `TaskCommit`, `IterCommit`, the park and the
restore messages SHALL carry the scope. The journal SHALL live under the outer instance's
state directory in a sub-directory per inner class and key.

#### Scenario: One code path for both

- **WHEN** a top-level steps instance and an inner steps instance each commit a stage
- **THEN** both commits SHALL be appended by the same journal writer, the first under the absent scope and the second under its inner class and key, and the two journals SHALL be inspectable with `flz operator journal <id>` and `flz operator journal <outer>/<inner>/<key>`

#### Scenario: A crash resumes the inner pass

- **WHEN** the outer class is durable and the runner dies while an inner instance's pass is between two stages
- **THEN** the rebind SHALL restore the inner scope from its journal and its input log and SHALL resume at the stage after the last committed one, and no outer tasklet SHALL re-run

### Requirement: Keyed and unkeyed edges SHALL create inner instances as cells are created

A keyed edge into a steps class SHALL create one inner instance per key on the key's first
message, capped by the edge's `max_cells`, and SHALL count inner instances against the
namespace's operator quota. An unkeyed edge SHALL create one inner instance that runs one
pass at a time, and the outer flow SHALL wait behind its mailbox under the channel's
overflow policy. A partitioned outer operator SHALL hold its inner instances per member.

#### Scenario: One instance per key, capped

- **WHEN** an edge with `max_cells: 2` feeds a steps class and three keys arrive
- **THEN** two inner instances SHALL exist, the third key's message SHALL be dropped and counted in `inputs_dropped_capped_total`, and the namespace quota SHALL count two

#### Scenario: An unkeyed inner instance serialises

- **WHEN** an unkeyed edge feeds a `per_input` steps class and ten messages arrive at once
- **THEN** the inner instance SHALL open ten passes one after another, and the messages SHALL wait in its mailbox in arrival order

### Requirement: Messages SHALL enter and leave an inner instance by its loop shape

A message on the edge SHALL enter the inner class's entry channel and open or feed a pass
by the inner class's `each`: `per_input` one pass per message, `chained` one pass whose
terminal output re-feeds the inner chain until it converges, `once` one pass for the
scope's life with a later message refused as to a finished instance. When the outer class
is durable, the message SHALL be journaled to the inner scope's input log before it is
forwarded, once: a replay of the outer input that sent it SHALL append nothing new. The
inner graph's exit output SHALL continue on the outer edge after the inner class, released
at stage commit; sinks inside the inner graph SHALL stay sinks; a `complete(result)` inside
the inner instance SHALL end its scope and carry the result out on the exit. When the outer
instance journals its input (on the effectively-once tier, or one that sleeps) and the
inner class keeps its work, the exit values SHALL ride the commit that makes them final
into the outer instance's input log, so a crash before the next outer tasklet took them
delivers them after resume; inside any other outer instance they SHALL be as durable as
that instance's own input, at most once.

#### Scenario: A chained loop inside a stream

- **WHEN** a flow class feeds a `chained` steps class whose chain hands off until a batch comes back empty
- **THEN** the inner instance SHALL iterate until convergence, and only then SHALL its exit output reach the next outer tasklet

#### Scenario: An answer outlives a crash before the outer graph took it

- **WHEN** a session inside a gateway that journals its input committed its turn, and the runner dies while the gateway's next tasklet holds the session's answer
- **THEN** after resume the answer SHALL reach the gateway's sink once

#### Scenario: A replayed outer input runs no second inner pass

- **WHEN** the outer instance's input is replayed after the inner scope's log took the message it sent
- **THEN** the scope's log SHALL hold that message once and the scope SHALL run one pass for it

#### Scenario: A once scope refuses a second message

- **WHEN** a `once` steps class inside a flow class receives a second message for a key whose pass has ended
- **THEN** the message SHALL be refused as to a finished instance and SHALL be visible in the DLQ naming the scope

### Requirement: An inner instance SHALL sleep and wake on its own

An inner steps instance SHALL sleep by its own class's `sleep_after`: idle at a committed
boundary for that long, it SHALL park its cells and its journal, drop its pass sheet and be
evicted from the outer instance's memory, and the next message for its key SHALL restore
it before delivery with the input tail it is owed; a message that landed while it parked
SHALL restore it at once. Its wake deadline, and a finished `once` scope's end, SHALL
outlive a restart of the outer instance. The outer instance SHALL not sleep on an inner's
account, and an inner SHALL not stay awake on the outer's. `flz operator info` SHALL list
sleeping inner scopes with their count.

#### Scenario: A session sleeps inside the gateway

- **WHEN** a `Session` inner instance with `sleep_after: 2 seconds` goes idle inside a `Gateway` that keeps receiving other keys
- **THEN** within a few seconds `flz operator info gateway` SHALL count it as sleeping, its cells SHALL be gone from the runner's memory, and the next message for its key SHALL be answered with its state intact

### Requirement: Undo and rewind SHALL be scoped to the inner instance

A rollback of an inner instance SHALL compensate the inner class's tasklets only, in
reverse order per cell, and SHALL never compensate a tasklet of the outer graph; outputs
already released on the outer edge SHALL not be retracted. `flz operator rewind` SHALL
take the scope and rewind that inner instance's journal alone.

#### Scenario: A failed inner pass leaves the outer graph alone

- **WHEN** an inner `undo_all` instance fails at its second stage after the outer tasklet before it emitted
- **THEN** the inner first stage's compensation SHALL fire, the outer tasklet SHALL not be compensated, and the outer tasklet after the edge SHALL receive nothing from that pass

### Requirement: A steps class SHALL NOT contain a steps class

Registration SHALL refuse a class whose run is `steps` declaring an edge into a class
whose run is `steps`, with `topology.nested_steps` naming both classes. A steps class MAY
contain a flow class, and a flow class MAY contain any number of steps classes, including
the same class at several graph positions.

#### Scenario: Nested steps are refused

- **WHEN** a steps class declares `into(OtherStepsClass)`
- **THEN** registration SHALL be refused with `topology.nested_steps` naming both classes

#### Scenario: The same steps class at two positions

- **WHEN** a flow class declares `into(Session)` on two different edges
- **THEN** each edge SHALL have its own family of scopes, and `flz operator info` SHALL list them under the two prefixed names

