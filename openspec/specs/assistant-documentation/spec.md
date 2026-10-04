# assistant-documentation Specification

## Purpose
What the generated documentation gives an AI assistant: a small `llms.txt` map it reads whole,
links from it to each page's full Markdown copy, and the operation catalogue.

## Requirements
### Requirement: `llms.txt` SHALL list what a user of Flezha needs and nothing internal

`mix docs` SHALL write `doc/llms.txt` with a short description of Flezha, a "Start here"
list, every page of the user-facing guide groups in sidebar order, and the published API
modules. It SHALL NOT list an internal module, a Mix task, or a page of the Architecture or
Contributing groups.

#### Scenario: No internal module is listed

- **WHEN** `mix docs` has run
- **THEN** `doc/llms.txt` SHALL NOT contain `Mix.Tasks.Bench` or `Flezha.AuthN.Cert`, and
  SHALL contain `Flezha.Service.Operator`

#### Scenario: The start list points at the essentials

- **WHEN** `doc/llms.txt` is read
- **THEN** its "Start here" list SHALL link what Flezha is, local mode, the CLI page, the
  error reference and `catalogue.json`

### Requirement: Every listed page SHALL be summarized and linked to its full Markdown copy

Each page line in `doc/llms.txt` SHALL hold the page title, a link to the page's Markdown
copy in `doc/`, and the first sentence of the page's introduction. Every linked file SHALL
exist, so an assistant can open the full page from the map.

#### Scenario: A page line leads to the full page

- **WHEN** `mix docs` has run
- **THEN** `doc/llms.txt` SHALL contain a line linking `what_is_flezha.md` with the sentence
  "Flezha is a durable, distributed runtime for stateless and stateful data processing.",
  and `doc/what_is_flezha.md` SHALL hold that page's full Markdown

#### Scenario: A link without a copy fails the build

- **WHEN** a listed page has no Markdown copy in `doc/`
- **THEN** the docs step SHALL fail naming that page

### Requirement: `llms.txt` SHALL stay small enough to read whole

The docs step SHALL fail when `doc/llms.txt` is larger than 32 KB, naming its size.

#### Scenario: An oversized map fails the build

- **WHEN** the map built from a fixture is larger than 32 KB
- **THEN** the docs step SHALL fail and name the size

### Requirement: The operation catalogue SHALL ship with the docs

`mix docs` SHALL copy `cli/catalogue.json` to `doc/catalogue.json`.

#### Scenario: The catalogue is next to the docs

- **WHEN** `mix docs` has run
- **THEN** `doc/catalogue.json` SHALL equal `cli/catalogue.json`

