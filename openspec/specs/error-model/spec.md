# error-model Specification

## Purpose
TBD - created by archiving change give-every-error-a-code. Update Purpose after archive.
## Requirements
### Requirement: Every caller-visible failure SHALL carry one error shape

Every failure Flezha reports to a caller SHALL carry five fields: a `code`, a `class`, a `message`, `details` and a `hint`. This holds for the `flz` CLI, the published consumer API, the SDK administration door, spawn, a runner connect, commit and message replies, the FFI and every SDK. `code` SHALL be a string spelled `<family>.<snake_case>`, stable across releases, and SHALL never be an Elixir term rendered as text. `class` SHALL be one of `internal`, `invalid`, `unreachable`, `denied`, `not_found`, `conflict` and `unavailable`. `message` SHALL be one sentence in plain words, naming the thing it is about when the source knows it. `details` SHALL be an object whose values are strings, numbers, booleans, lists or objects, and SHALL never be empty of a value its message names. `hint` SHALL be the next command to run when one exists, and absent otherwise.

#### Scenario: A refusal on the consumer API is the shape, not a term

- **WHEN** a caller deletes a namespace `ghost` that does not exist through `Flezha.Service.Namespace.delete/2`
- **THEN** the answer SHALL be `{:error, %Flezha.Error{code: "namespace.not_found", class: :not_found}}`, with `details` naming `ghost` and a message that names it

#### Scenario: No surface leaks an Elixir term

- **WHEN** any catalogued refusal is rendered on any surface
- **THEN** its `code` and `message` SHALL contain no Elixir term syntax such as a leading `:`, `{`, `%{` or a node name

### Requirement: One catalogue SHALL define every code

The file `flz_transport/errors.json` SHALL be the only definition of the codes. For each code it SHALL give the class, a message template whose `{placeholders}` name keys of `details`, the detail keys, an optional hint, and the internal reasons that map to it. It SHALL also list, by name, the reasons that never reach a caller. The Elixir controller SHALL read it at compile time and the Rust crates SHALL read it at build time. No code, class, message or hint SHALL be written anywhere else in the source.

#### Scenario: One definition

- **WHEN** the source tree is searched for the text of a catalogued message template
- **THEN** it SHALL be found in `flz_transport/errors.json` and the generated `docs/errors.md` only

#### Scenario: The catalogue is internally consistent

- **WHEN** the catalogue test runs
- **THEN** every class SHALL be one of the seven failure classes, every placeholder SHALL be a declared detail key, every site an entry names SHALL be a real verb action or a declared wire site, and no reason at one site SHALL map to two codes

### Requirement: A new caller-visible reason SHALL NOT ship without a code

A test SHALL parse every source file under `lib/`. Each `{:error, atom}` and `{:error, {atom, ...}}` literal it finds SHALL be mapped by the catalogue or named on its internal list. It SHALL refuse every `{:error, <text>}` literal, plain or interpolated. At runtime, a reason that reaches a caller with no catalogue entry SHALL become the code `internal.uncatalogued`, of class `internal`, carrying the raw reason as text in `details.reason`, and SHALL emit `[:flezha, :error, :uncatalogued]`. An operation called through `Flezha.Service.Admin` by a networked caller (HTTP, MCP, a runner's admin request) SHALL answer that code with `details.reason` naming the controller's log, which SHALL record the raw reason. The test suite SHALL fail when that event fires. In Rust, the code of every error type SHALL come from an exhaustive match, and a test SHALL check each code it can return against the catalogue.

#### Scenario: An uncatalogued literal fails the build's tests

- **WHEN** a developer adds `{:error, :brand_new_reason}` to a module under `lib/` and neither catalogues nor lists it
- **THEN** the catalogue test SHALL fail naming the reason and the file

#### Scenario: A text reason is refused

- **WHEN** a module under `lib/` returns `{:error, "something went wrong"}`
- **THEN** the catalogue test SHALL fail naming the file and line

#### Scenario: A missed reason is caught at runtime

- **WHEN** a reason with no catalogue entry reaches the conversion during the test suite
- **THEN** the caller SHALL receive `internal.uncatalogued` and the suite SHALL fail

### Requirement: The catalogue SHALL be published as a generated page

`docs/errors.md` SHALL be generated from the catalogue and list every code with its class, exit code, message and hint, grouped by family. It SHALL also give the class table and the JSON schema. A gate SHALL fail when the committed page differs from a fresh generation.

#### Scenario: The page follows the catalogue

- **WHEN** a code is added to the catalogue and the page is not regenerated
- **THEN** the docs gate SHALL fail naming `docs/errors.md`

### Requirement: The class SHALL decide the exit code

The class SHALL map to exactly one process exit code: `internal` 1, `invalid` 2, `unreachable` 3, `denied` 4, `not_found` 5, `conflict` 6, `unavailable` 7. Success is 0. Every `flz` verb SHALL exit with the code of its failure's class, whichever side, `flz` or the controller, raised the failure.

#### Scenario: Classes are told apart by exit code

- **WHEN** `flz namespace delete ghost` refuses because `ghost` does not exist, and `flz namespace delete` is run with no token under enforced authorization
- **THEN** the first SHALL exit 5 and the second 4

#### Scenario: A malformed command line is invalid

- **WHEN** `flz relay start echo --nope` is run
- **THEN** it SHALL exit 2 with the code `usage.unknown_option` and reach no controller

### Requirement: Text and JSON renderings SHALL be fixed

The text rendering SHALL be the line `error: <message> (<code>)` followed, when a hint exists, by the line `hint: <hint>`. The JSON rendering SHALL be one object `{"error": {"code", "class", "message", "details", "hint"}}`, where `details` is always an object and `hint` is a string or `null`. It SHALL be written as one line. The Elixir and Rust renderers SHALL produce byte-identical output for the same code and details, which a shared fixture checks in both suites.

#### Scenario: The two renderers agree

- **WHEN** the shared rendering fixture runs in the Elixir suite and in the Rust suite
- **THEN** every case SHALL render to the same text and the same JSON in both

### Requirement: One error type and one renderer SHALL exist per language

The Elixir controller SHALL hold the model in `Flezha.Error` alone: the struct, the mapping from an internal reason to a code, and the rendering. The Rust crates SHALL hold it in `flz_transport::errors` alone. No other module SHALL turn a caller-visible reason into text. Each SDK SHALL expose exactly one error type for anything Flezha refuses, carrying the five fields.

#### Scenario: No second printer

- **WHEN** `lib/flezha/cli/` (which also holds the door, `Flezha.Service.Admin`) and `cli/src/` are searched for a function that turns a reason into caller-facing text
- **THEN** none SHALL exist outside `Flezha.Error` and `flz_transport::errors`

#### Scenario: One catchable type per SDK

- **WHEN** an SDK's administration call, spawn, connect or registration is refused
- **THEN** the SDK SHALL raise or return its one error type, and its `code` SHALL equal the code a `flz` user sees for the same refusal

