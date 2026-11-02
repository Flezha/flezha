# api-mcp Specification

## Purpose
What an AI assistant gets from Flezha over MCP: `flz mcp`, a stdio server that answers through the controller the way `flz` does and works before any controller runs; the HTTP API's `/mcp` endpoint, stateless and tools only, with one tool per catalogue entry the token's role grants, the relay tools beside them, and a Flezha failure answered as a tool error with its code and hint; the four knowledge tools (rules and docs map, an SDK's echo example, an error's meaning, a guide page) that answer from the installed release on every Flezha MCP server; and the generated `priv/ai_kit/` they read, held to its sources.
## Requirements
### Requirement: `flz mcp` SHALL serve MCP over stdio with no controller needed to start

`flz mcp` SHALL run an MCP server on stdin and stdout for as long as its client keeps it
open. It SHALL answer `initialize`, `ping`, `tools/list` and `tools/call` whether or not a
controller runs, and SHALL write nothing but protocol messages to stdout. It SHALL serve
through the same MCP core as the HTTP API's `POST /mcp`, so that a tool, its schema and its
error mapping are the same on both. It SHALL answer `ping` while a call runs, SHALL kill a
cancelled call's child and send that call no answer, and SHALL leave no child running when
SIGINT or SIGTERM stops it.

#### Scenario: Started with no controller

- **WHEN** `flz mcp` is started while no controller runs and a client sends `initialize`
  then `tools/list`
- **THEN** it SHALL answer both, list the knowledge tools, and print nothing to stdout that
  is not a JSON-RPC message

#### Scenario: An operation called while no controller runs

- **WHEN** `relay_list` is called and no controller runs
- **THEN** the result SHALL be a tool error carrying the code `controller.unreachable` and
  its hint

#### Scenario: A message is data, never code

- **WHEN** an argument of a message carries `#{...}`
- **THEN** it SHALL reach the operation as that text, and nothing in it SHALL run on the
  controller

#### Scenario: A ping while a call hangs

- **WHEN** a `tools/call` hangs on its controller and the client sends `ping`
- **THEN** the `ping` SHALL be answered without waiting for the call

#### Scenario: A cancelled call

- **WHEN** the client sends `notifications/cancelled` for a running `tools/call`
- **THEN** the call's child SHALL be killed and the call SHALL get no answer

#### Scenario: Stopped by its client

- **WHEN** `flz mcp` receives SIGTERM while a call's child runs
- **THEN** the child SHALL be killed and reaped before `flz mcp` exits

#### Scenario: A controller started later is found

- **WHEN** a controller is started after `flz mcp` and `relay_list` is called again
- **THEN** the call SHALL answer the controller's relays without `flz mcp` being
  restarted

### Requirement: `flz mcp` SHALL expose exactly the operations the HTTP API exposes, as the caller's token

The operation tools of `flz mcp` SHALL be the remote, non-streaming catalogue entries. Each
call SHALL be made through the controller's door as the principal of the token `flz` reads
(`--token` or `FLEZHA_TOKEN`). An operation that is not remote SHALL be neither listed nor
callable, whatever the token.

#### Scenario: A dangerous verb is not a tool

- **WHEN** `tools/list` is read with an `admin@_system` token
- **THEN** no tool SHALL stop the controller, issue or revoke a token, or reset state

#### Scenario: A role is enforced

- **WHEN** a `viewer` token calls `relay_start`
- **THEN** the result SHALL be a tool error with an `authz` code and its hint, and the
  refusal SHALL be audited

### Requirement: The knowledge tools SHALL answer from the installed release

Every MCP server of Flezha SHALL list five tools to every caller: `flezha_guide`,
`flezha_sdk_example`, `flezha_error`, `flezha_docs` and `flezha_new_project`. They SHALL read no
controller state and SHALL be marked read-only. `flezha_guide` SHALL answer the working rules and
the docs map; `flezha_sdk_example` the echo example of one of elixir, go, java, node, php,
python, ruby and rust; `flezha_error` the class, message and hint of an error code from the error
catalogue; `flezha_docs` one guide page the map lists; `flezha_new_project` the files of one
template the release carries (each with its path, content and executable bit, its `.gitignore`
included) and the template's run commands, for a language and an example defaulting to
`starter`. An unknown value SHALL answer a tool error whose hint names where the valid values are.
The server's `instructions` SHALL carry the working rules, which SHALL tell an assistant to start
a new project from `flezha_new_project`.

#### Scenario: The SDK example for a language

- **WHEN** `flezha_sdk_example` is called with `language: "python"`
- **THEN** the answer SHALL hold every tracked file of `sdk/python/examples/echo` but
  `.gitignore`, each under its path

#### Scenario: An error code is explained

- **WHEN** `flezha_error` is called with `code: "dlq.not_found"`
- **THEN** the answer SHALL carry that code's class, message and hint as the error
  catalogue holds them

#### Scenario: An unknown page

- **WHEN** `flezha_docs` is called with a page the map does not list
- **THEN** the result SHALL be a tool error whose hint names `flezha_guide` as the list of
  pages

#### Scenario: The rules reach the client

- **WHEN** a client initializes
- **THEN** the `instructions` SHALL hold the text of `priv/ai_kit/rules.md`

#### Scenario: A new project for a language

- **WHEN** `flezha_new_project` is called with `language: "java"`
- **THEN** the answer SHALL hold every file the release's `flezha-java-starter.tar.gz` holds,
  with the same contents and executable bits, and the starter's run commands

#### Scenario: An unknown template

- **WHEN** `flezha_new_project` is called with `example: "nope"`
- **THEN** the result SHALL be a tool error coded `template.unknown` whose hint lists every
  valid template as `<language>/<example>`

### Requirement: The rules SHALL name no SDK function

`priv/ai_kit/rules.md` SHALL state the working rules (read the SDK example before writing
SDK code, try local mode first, act on `code` and `hint`, prefer the operation tools, never
ask for an admin token or stop or reset unasked) and where the docs are. It SHALL name no
SDK function or class, so that it has nothing to go stale.

#### Scenario: No SDK name in the rules

- **WHEN** every code span of the rules is read
- **THEN** each SHALL be a tool name, `flz` or one of its options, a field of an error, an
  error code, or a page the map lists

### Requirement: Every factual part of the knowledge SHALL be generated and held to its source

`mix flezha.ai_kit` SHALL write `priv/ai_kit/sdk/<language>.md` for each SDK
from that SDK's `examples/echo` tracked sources, and `priv/ai_kit/llms.txt` with the guide
pages it links. The suite SHALL fail when an SDK file or a page differs from its source, or
when the map links a page that is missing or a page is there that the map does not link.
Error meanings and operations SHALL be read from the code at call time, not copied.

#### Scenario: An SDK example changes

- **WHEN** a file under `sdk/python/examples/echo` changes and the knowledge is not
  regenerated
- **THEN** the check SHALL name `sdk/python.md`

#### Scenario: The release carries the knowledge

- **WHEN** a release is built
- **THEN** `flezha_sdk_example` and `flezha_docs` SHALL answer from it with the source tree
  absent

### Requirement: The MCP endpoint SHALL speak Streamable HTTP, stateless and tools only

`POST /mcp` SHALL accept JSON-RPC 2.0 and answer `application/json`. It SHALL implement
`server/discover`, `ping`, `tools/list` and `tools/call` for the pinned revision, answer
`initialize` and accept notifications for a client of the earlier handshake revision,
declare only the `tools` capability, mint no session, and answer `405` to `GET /mcp` and
`DELETE /mcp`. A request naming a revision it does not support SHALL be answered `400`
listing the supported ones. An unknown method SHALL be answered with JSON-RPC error
`-32601`.

#### Scenario: A client discovers the server

- **WHEN** a client sends `server/discover` naming the pinned revision
- **THEN** the answer SHALL list the supported revisions, the `tools` capability and the
  server named `flezha`

#### Scenario: A client of the handshake revision initializes

- **WHEN** a client sends `initialize`
- **THEN** the answer SHALL carry the supported handshake revision, the `tools` capability
  and `serverInfo` named `flezha`, and no session id

#### Scenario: A server stream is asked for

- **WHEN** a client sends `GET /mcp`
- **THEN** the answer SHALL be `405`

### Requirement: Each tool SHALL be generated from its catalogue entry

Every remote, non-streaming catalogue entry the token's role grants SHALL be one tool:

- its name SHALL be the entry name in snake case with `.` replaced by `_`;
- its description SHALL be the entry's description;
- its input schema SHALL be generated from the entry's argument rows, with the required rows
  required and no other property allowed;
- its `readOnlyHint` SHALL be true only for an operation whose row writes no audit record.

A tool SHALL be listed only when the token's role grants its row's action. Beside the
operation tools, the five knowledge tools of the `api-mcp` capability SHALL be
listed for every token, and the server's `instructions` SHALL carry the working rules.

#### Scenario: The schema follows the rows

- **WHEN** `tools/list` is read and the entry `Dlq.list` declares one optional string argument
  `relay_class`
- **THEN** the tool `dlq_list` SHALL have an input schema with one optional string property
  `relay_class` and `additionalProperties: false`

#### Scenario: A role sees only what it may call

- **WHEN** a namespace `viewer` token sends `tools/list`
- **THEN** no tool whose action the viewer role does not grant SHALL be listed

#### Scenario: The knowledge tools are served over HTTP

- **WHEN** a namespace `viewer` token sends `tools/list` to `POST /mcp`
- **THEN** `flezha_guide`, `flezha_sdk_example`, `flezha_error`, `flezha_docs` and
  `flezha_new_project` SHALL be listed

### Requirement: A Flezha failure SHALL reach the model as a tool error with its code and hint

A `tools/call` whose operation fails SHALL answer a result with `isError: true`, whose text
content is the error object as JSON and whose structured content is `{"error": {...}}`
carrying `code`, `class`, `message`, `details` and `hint`. A protocol fault (an unknown tool,
arguments that are not an object) SHALL be answered as a JSON-RPC error instead. A success
SHALL answer the door's JSON as text content and as structured content, wrapped as
`{"result": ...}` when it is not a JSON object.

#### Scenario: A dead letter that does not exist

- **WHEN** `tools/call` of `dlq_get` names an entry that does not exist
- **THEN** the result SHALL carry `isError: true` and the code `dlq.not_found`

#### Scenario: An unknown tool

- **WHEN** `tools/call` names `controller_stop`
- **THEN** the answer SHALL be a JSON-RPC error, not a tool result

### Requirement: Relay tools SHALL be listed beside the catalogue tools

`tools/list` on `POST /mcp` SHALL include the relay tools of the `relay-calls` capability for
the token's namespace, after the catalogue and knowledge tools, and `tools/call` on one SHALL go
through the door as `Relay.call_channel`.

#### Scenario: One list for both

- **WHEN** an `app` role token of namespace `desk` lists tools and factory `orders` describes
  `place`
- **THEN** the list holds the catalogue tools, the knowledge tools and `orders_place`

