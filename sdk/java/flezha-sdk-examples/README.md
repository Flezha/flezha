# Flezha Java SDK: worked examples

These are compiled sources, not snippets. Every example is built by the Maven
reactor and most are covered by a test, so an example that drifts away from the
SDK breaks the build rather than quietly misleading its reader.

```bash
mvn -f sdk/java/pom.xml test -pl flezha-sdk-examples -am
```

Each example is a package of ordinary top-level classes, laid out as a real
project would be rather than as nested classes in a single file. Open one and
begin with its `package-info.java`, which states what the example demonstrates,
which mechanism it relies on, and which file to read first. The classes carry
explanatory comments, so reading an example top to bottom is intended to teach
the mechanism rather than leave it to be inferred.

## The ladder

Six examples, in order. Each assumes the preceding one and introduces exactly one
new idea, so reading them out of order will leave gaps.

**1. [`echo/`](src/main/java/com/flezha/examples/echo)** is the minimal complete
application, and establishes the three-part structure every Flezha application
has: a procedure holding the code invoked per message, an operative declaring the
graph, and a main starting a runner. It declares no state, which keeps the whole
example short enough to hold in mind at once.

**2. [`state/`](src/main/java/com/flezha/examples/state)** makes the procedure
durable, and introduces the rule that catches almost everyone: a procedure never
assigns to its own fields. State is persisted as an ordered log of changes and
reconstructed by replaying that log, so a direct assignment is absent from the
log and absent from the reconstructed object. The failure is silent, because a
direct assignment behaves correctly for as long as the process survives.

**3. [`pipeline/`](src/main/java/com/flezha/examples/pipeline)** chains two
procedures, demonstrating that a procedure's output is an ordinary channel and
that a chain is the normal shape rather than a special case. Each stage keeps its
own state and is restarted independently, and neither stage knows its position in
the chain.

**4. [`keyed/`](src/main/java/com/flezha/examples/keyed)** partitions one stream
into many independent entities, each with its own state, processed in parallel.
It also covers the transforms available on the wiring itself, which reshape or
discard values before any user code is invoked, and the ordering constraint
between them.

**5. [`merged/`](src/main/java/com/flezha/examples/merged)** feeds one procedure
from two entry channels, with sensor readings on one and a live configuration
threshold on the other. It shows how two channels are reconciled into the single
input type a procedure declares.

**6. [`local/`](src/main/java/com/flezha/examples/local)** executes a complete
graph inside the calling JVM with no controller, no socket and no container. This
is both the fastest way to try an idea and the basis for a test that exercises
real wiring. Nothing is durable in this mode, so it establishes that a graph
behaves as intended, not that it recovers correctly.

After these six you can build, wire and test a real application. The remainder
address specific problems.

## Specific problems

**[`timers/`](src/main/java/com/flezha/examples/timers)** closes a session after
a period of inactivity, using timers. It introduces `onTimer` as a second entry
point into user code alongside `run`, and the cancel-then-rearm pattern that
keeps one live deadline per entity.

**[`effectivelyonce/`](src/main/java/com/flezha/examples/effectivelyonce)**
prevents a retry writing the same result twice, which matters when the sink is a
database row, a payment, or anything else that cannot absorb a duplicate. Two
mechanisms are required together, and the example explains why neither alone is
sufficient.

**[`corrections/`](src/main/java/com/flezha/examples/corrections)** repairs an
already-published result when an input arrives after its interval closed. Rather
than discarding the message or misattributing it, the runtime replays the
affected entity with the message in its correct position and re-emits.

**[`workflow/`](src/main/java/com/flezha/examples/workflow)** covers ordered
steps that must not be re-executed as a unit after a partial failure: reserve,
charge, dispatch. Completed steps are journaled, a crash resumes at the next
step, and an exhausted retry ladder unwinds what was already done.

**[`agent/`](src/main/java/com/flezha/examples/agent)** is a long-lived agent
that holds its conversation in durable state and resumes mid-thread after a
restart, waking on a message or on a timer. This is the shape to copy for
anything intended to run for weeks. It is also the one example where the Kryo
type list must be written by hand, and it explains when that is necessary.

**[`spawn/`](src/main/java/com/flezha/examples/spawn)** shows a stream procedure
requesting a workflow worker from the controller, so a detector can delegate
substantial work without becoming a dispatcher itself.

## Looking things up

The examples show how the pieces fit together. For the behaviour of a specific
type or method, read the generated javadoc: every concept is documented on the
type it concerns, so the explanation is on the class you are already looking at.

```bash
mvn -f sdk/java/pom.xml -pl flezha-sdk-api javadoc:javadoc
```

The output is at `../flezha-sdk-api/target/site/apidocs/index.html`. The plugin
skips regeneration when its output appears current, so delete that directory
first if a change does not appear.
