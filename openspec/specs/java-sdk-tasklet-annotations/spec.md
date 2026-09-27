# java-sdk-tasklet-annotations Specification

## Purpose

The java-sdk-tasklet-annotations capability is the Java realization of the SDK authoring contract's declarations. `@Tasklet.Spec`, nested in the `Tasklet` interface, carries a tasklet's name and scalar settings, and `@UnboundedOperator` / `@BoundedOperator` on a tasklet class declare the single-tasklet short form. All three are retained at runtime and read when a class is registered; nothing is generated, so the class the author writes is the class every call names. Multi-tasklet topology stays explicitly declared in a `channels()` method, with no engine, protocol, or WAL change.
## Requirements
### Requirement: `@UnboundedOperator` and `@BoundedOperator` carry the short form

The Java SDK SHALL provide annotations `@UnboundedOperator` and `@BoundedOperator` in `com.flezha.sdk.authoring`, retained at runtime, applied to a class that implements `Tasklet`, declaring a whole single-tasklet operator of the kind the annotation names. Each SHALL require a non-empty `id`, SHALL accept an optional `sink`, an optional `onFull`, and an optional `input` defaulting to the empty string, which registration SHALL lower to the entry channel name `input`, and SHALL NOT accept a `kind` member. `@BoundedOperator` SHALL additionally accept `loopMode()`, a `LoopMode` defaulting to `ONCE`. No other member SHALL exist on either annotation: the output type comes from the tasklet's `Output`, keyed emits are the body's `ctx.emitKeyed`, and cell keys and mailbox capacity are `@Tasklet.Spec` members.

`FlezhaRunner.registerFactory` and `unregisterFactory` SHALL accept such a class and build, at registration, an operator whose `factoryId()` is the `id`, whose topology wires the entry channel into the class and, when a `sink` is written, the class to that plugin sink, whose bounded `loopMode()` is the annotation's, and whose every other operator method is at its interface default. With no `sink`, the topology SHALL declare the entry channel and the tasklet and no plugin channel. The tasklet's name SHALL be the `id`. The class SHALL need no `@Tasklet.Spec`; one written there carries the tasklet's settings, and its `name` SHALL equal the `id`, or registration SHALL fail with an error beginning `ShortFormNameMismatch` naming both. Nothing SHALL be generated and nothing SHALL register itself.

Registration SHALL refuse, each with an error naming the class: an empty or absent `id`, naming the required member; a `sink` on a class whose `Output` is `Void`, with a message beginning `SinkWithoutOutput`; and an annotated class that also implements `UnboundedOperator` or `BoundedOperator`, with a message beginning `ShortFormWithTopology`.

#### Scenario: An annotated class is a whole unbounded operator

- **WHEN** a class `Echo implements Tasklet<String, Void, String>` carries only `@UnboundedOperator(id = "echo", sink = "out")`, and the runner is given `registerFactory(Echo.class)`
- **THEN** the runner declares an unbounded operator `echo` with entry channel `input` and sink `out`, backed by the tasklet `echo`

#### Scenario: An annotated class is a whole bounded operator

- **WHEN** a class `Place` is annotated `@BoundedOperator(id = "place", sink = "placed", input = "order")`, is registered, an instance is started, and one message is sent on `order`
- **THEN** `run` receives the message on `order`, what it emits reaches `placed`, and the instance completes as a `once` bounded operator

#### Scenario: A short-form bounded operator declares its loop mode

- **WHEN** a class is annotated `@BoundedOperator(id = "chat", sink = "replies", loopMode = LoopMode.PER_INPUT)`, is registered, and two messages reach its entry channel
- **THEN** the operator's `loopMode()` returns `PER_INPUT`, `run` runs once per message, and the instance does not complete after the first

#### Scenario: A short form with no sink is declared

- **WHEN** a class is annotated `@UnboundedOperator(id = "counter", input = "ticks")` and registered
- **THEN** the operator declares `ticks` into the tasklet and no plugin channel

#### Scenario: The author registers the class they wrote

- **WHEN** `registerFactory(Echo.class)` is called for an annotated class
- **THEN** the operator is declared, and no class other than `Echo` exists for it

#### Scenario: Nothing registers itself

- **WHEN** an application declares annotated short-form classes and never calls `registerFactory` for them
- **THEN** its runner declares no operator for them

#### Scenario: The entry channel keeps its default name

- **WHEN** a class annotated `@UnboundedOperator(id = "echo", sink = "out")` declares no `input` element
- **THEN** the operator's entry channel SHALL be named `input`

#### Scenario: The annotation names the entry channel

- **WHEN** a class is annotated `@UnboundedOperator(id = "echo", sink = "out", input = "prices")`
- **THEN** the operator's entry channel SHALL be named `prices`

#### Scenario: An annotation with no id is refused

- **WHEN** a class `Upcase` is annotated `@UnboundedOperator(sink = "out")` with no `id`
- **THEN** registration SHALL fail naming the required `id`, and SHALL NOT derive a factory id from the class name

#### Scenario: An annotated class implementing an operator interface is refused

- **WHEN** a class annotated `@UnboundedOperator` also implements `UnboundedOperator`
- **THEN** registration SHALL fail with a message beginning `ShortFormWithTopology`

#### Scenario: A short form takes settings from @Tasklet.Spec

- **WHEN** a class carries `@UnboundedOperator(id = "counter", input = "ticks")` and `@Tasklet.Spec(name = "counter", maxCells = 10)`
- **THEN** the tasklet is named `counter` and its cells are capped at 10

#### Scenario: A short form whose tasklet name differs from its id is refused

- **WHEN** a class carries `@UnboundedOperator(id = "counter")` and `@Tasklet.Spec(name = "tally")`
- **THEN** registration SHALL fail with a message beginning `ShortFormNameMismatch` naming `counter` and `tally`

#### Scenario: A sink on a Void output is refused

- **WHEN** a class `Drain implements Tasklet<String, Void, Void>` is annotated `@UnboundedOperator(id = "drain", sink = "out")`
- **THEN** registration SHALL fail with a message beginning `SinkWithoutOutput`

### Requirement: `@Tasklet.Spec` declares a tasklet's name and settings

The Java SDK SHALL provide a class annotation `@Tasklet.Spec`, nested in the `com.flezha.sdk.tasklet.Tasklet` interface, retained at runtime and read at registration, which every class implementing `Tasklet` SHALL carry, except a short-form class, whose tasklet takes the operator's `id` as its name. It SHALL have a required `name`, the tasklet's durable name under which its state is saved, and SHALL NOT derive a name from the class name. Its optional members SHALL be `keys` (static cell keys), `capacity` (mailbox capacity), `maxCells`, `idleTtlSeconds` (the idle TTL in seconds, 0 for no eviction), `maxTimersPerCell`, `compactAfterBytes`, `schemaVersion`, `minReadableSchemaVersion` and `logicalClock`, each with the meaning and default the method of the same purpose has today. Reading a member SHALL behave exactly as the corresponding method override would.

Registration SHALL fail with an error beginning `TaskletSpecMissing` and naming the class when a tasklet class that is not a short form does not carry the annotation, and SHALL fail naming the member and its value when `name` is empty or a count or duration member is negative.

#### Scenario: The annotation names the tasklet

- **WHEN** a tasklet class carries `@Tasklet.Spec(name = "quiet_symbol")`
- **THEN** the tasklet SHALL be named `quiet_symbol`, and its state SHALL be saved under that name

#### Scenario: A renamed class keeps its state

- **WHEN** a deployed tasklet class `QuietSymbol` with `@Tasklet.Spec(name = "quiet_symbol")` is renamed `QuietPair`, keeping the annotation
- **THEN** the tasklet SHALL still be named `quiet_symbol`, and its saved state SHALL be restored

#### Scenario: Settings take effect from the annotation

- **WHEN** a tasklet carries `@Tasklet.Spec(name = "quiet_symbol", maxCells = 1_000, idleTtlSeconds = 3_600)`
- **THEN** the tasklet's cells SHALL be capped at 1 000 and evicted after one hour idle, as the matching method overrides would do

#### Scenario: A tasklet without the annotation is refused

- **WHEN** a class implementing `Tasklet` without `@Tasklet.Spec` is named in an operator's wiring
- **THEN** registration SHALL fail with an error beginning `TaskletSpecMissing` naming the class

#### Scenario: A negative member is refused

- **WHEN** a tasklet carries `@Tasklet.Spec(name = "x", idleTtlSeconds = -1)`
- **THEN** registration SHALL fail naming `idleTtlSeconds` and the value

### Requirement: The Java SDK generates no tasklet or operator class

The Java SDK SHALL ship no annotation processor. No tasklet, handle, change or operator class SHALL be generated from an author's class, and every call that names a tasklet or an operator (`into`, `registerFactory`, `unregisterFactory`, `TaskletTestHarness.of`) SHALL take the class the author wrote.

#### Scenario: Only the author's classes exist

- **WHEN** a project containing tasklets and short-form operators is compiled with the Java SDK
- **THEN** the build output SHALL contain no class the author did not write, and no `annotationProcessorPaths` entry SHALL be needed

