# docs-content Specification

## Purpose

What the documentation holds and how it is laid out. A page's folder says whether it is published,
links reach only pages the repository carries, a repeated fact is explained on one owner page, every
SDK carries a README that is its package page, and the published API docs carry no test module. The
guides describe a relay by its settings and never by a kind, keep bounded and unbounded for input
data, show only settings that are leaves of the settings tree, and run each part's example from its
pages in every language the part is written for. For AI assistants the generated docs ship a small
`llms.txt` map linking each page's full Markdown copy, and the operation catalogue.

## Requirements

### Requirement: A page's folder SHALL say whether it is published

Every published page and asset SHALL live under `docs/public/`, at the same sub-path the page
has in the site's layout, a published page being an extra of either ExDoc configuration in
`mix.exs`; every other tracked page under `docs/` SHALL live under `docs/internal/`. `docs/README.md` SHALL be a short pointer to both: the published site's first
page and `docs/internal/README.md`, the one index of internal pages. A test SHALL fail when a
Markdown file under `docs/public/` is not an extra, or an extra lies outside `docs/public/`.

#### Scenario: A page added to the public folder but not to the site fails

- **WHEN** a Markdown file is added under `docs/public/` and no `mix.exs` extra names it
- **THEN** the layout test SHALL fail naming the file

#### Scenario: An extra outside the public folder fails

- **WHEN** a `mix.exs` extra names a page under `docs/internal/`
- **THEN** the layout test SHALL fail naming the extra

#### Scenario: Moving pages keeps the published URLs

- **WHEN** `mix docs` builds the site after the pages moved under `docs/public/`
- **THEN** every published page SHALL keep the HTML file name it had, and the AI kit SHALL keep
  every file name it had

### Requirement: A link SHALL reach only a page the repository carries, by a path its reader can follow

A published page SHALL link an internal page only by its absolute GitHub URL, because the
published site carries no internal page; a relative link between two published pages, or
between two internal pages, SHALL stay relative. No tracked document and no code comment or
printed message SHALL cite a documentation page that exists only in the untracked
`local_dev/docs/` tree, under its old `docs/` path or any other. The citation
gate SHALL judge a relative Markdown link by resolving it from the citing file, whether it
starts with `..` or with a folder that is not tracked at the repository's top, like any other
path inside the repository.

#### Scenario: A dead relative link climbing out of its folder fails the gate

- **WHEN** a page under `docs/internal/sdk/` links a `../` path that resolves inside the
  repository to a file the repository does not track
  <!-- docs-citation-record:begin -->(for example `../internals/runner_identity.md`)<!-- docs-citation-record:end -->
- **THEN** the citation gate SHALL exit non-zero naming the page, the line and the path

#### Scenario: A comment naming a local-only page is gone

- **WHEN** the tracked sources of the core and of every SDK are searched for the old paths of
  the pages that exist only in `local_dev/docs/`
  <!-- docs-citation-record:begin -->(`docs/operations/observability.md`, `docs/quickstart.md`, `docs/runbooks/`)<!-- docs-citation-record:end -->
- **THEN** no match SHALL remain outside this specification's own recorded-citation lines

#### Scenario: A public page reaching an internal page

- **WHEN** a published page refers a reader to the glossary
- **THEN** the link SHALL be `https://github.com/flezha/flezha/blob/main/docs/internal/glossary.md`
  with an optional anchor, and the path in it SHALL exist in the tree

### Requirement: A repeated fact SHALL be explained on one owner page

Each of these facts SHALL be explained on one page, and every other page that needs it SHALL
link that page instead of restating it: local mode and testing a tasklet
(`docs/public/topics/core/running_and_testing.md`), a relay's API, answering and calling
(`docs/public/topics/core/sending_and_calling.md`), cell timers
(`docs/public/topics/core/timers.md`), installing a plugin
(`docs/public/topics/plugins/installing_plugins.md`), adding an SDK to a project (the "Add the
SDK to a project" section of `docs/public/getting_started/up_and_running.md`; each SDK's README
keeps its own install line, being that package's registry page), environment variables and application
settings (the generated tables of `docs/public/configuration/`), every `flz` verb (the rendered
help and `docs/public/topics/cli/command_reference.md`), and the list of pages (the sidebar for
published pages, `docs/internal/README.md` for internal ones).

#### Scenario: An SDK README points at local mode instead of teaching it

- **WHEN** a reader opens any SDK's `README.md` looking for local mode
- **THEN** it SHALL find one line naming the SDK's local-mode entry point and a link to the
  "Local mode" section of `running_and_testing.md`, not a second explanation

#### Scenario: A page that needs an SDK package links the one install section

- **WHEN** a published page other than `up_and_running.md` names how to get an SDK package
- **THEN** it SHALL link `up_and_running.md#add-the-sdk-to-a-project` and SHALL NOT restate the
  install line

#### Scenario: The test harnesses are listed once and truly

- **WHEN** a reader looks up which SDKs ship a test harness
- **THEN** `running_and_testing.md` SHALL list each harness that exists in the SDK sources,
  with its name, and `docs/public/topics/core/state.md` SHALL link that list

### Requirement: Every SDK SHALL carry a README that is its package page

Each SDK repository SHALL carry a `README.md` that states how to install the
SDK, the language versions it supports, how the native library reaches the host, and links to
the public pages for local mode, testing and the relay API. The SDK guides under
`docs/internal/sdk/` SHALL NOT repeat the install, supported-versions, local-mode or testing
text.

#### Scenario: PHP and Ruby have a package page

- **WHEN** a reader opens `sdk/php/README.md` or `sdk/ruby/README.md`
- **THEN** the file SHALL exist and state the install command and the supported language
  versions

#### Scenario: An example README links only what exists

- **WHEN** the relative links of every `sdk/*/examples/*/README.md` are resolved
- **THEN** each SHALL reach a file that exists, and none SHALL name the guides tree removed on
  2026-10-03
  <!-- docs-citation-record:begin -->(`docs/guides/`)<!-- docs-citation-record:end -->

### Requirement: The published API docs SHALL carry no test module

`mix docs` SHALL publish no module compiled from `test/`, so a reader browsing the API sees the
controller's modules and none of its test scaffolding.

#### Scenario: Test helpers are left out

- **WHEN** `mix docs` has run
- **THEN** no page SHALL exist for a module whose source is under `test/helpers` or
  `test/fixtures`

### Requirement: The guides SHALL name no relay kind

The tutorial and topic pages SHALL describe how a relay runs by its settings, in flow or in
steps. No page under `docs/public/tutorials/` or `docs/public/topics/`, and neither `docs/internal/glossary.md` nor
`docs/public/getting_started/what_is_flezha.md`, SHALL describe a relay as bounded or unbounded,
or as a kind.

#### Scenario: A grep over the guides finds no kind word about a relay

- **WHEN** the tutorial and topic pages, the glossary and `what_is_flezha.md` are searched for `bounded`, `unbounded` and `relay kind`
- **THEN** every match SHALL be one of the input-data or mailbox senses of the next requirement, and no match SHALL describe a relay

### Requirement: Bounded and unbounded SHALL describe input data only

Where a page uses the words bounded or unbounded, they SHALL describe input data in the
sense a stream processor uses them, a finite input such as a file against an endless input
such as a live feed, or a mailbox's capacity. The glossary SHALL carry one entry saying so.

#### Scenario: The words appear only next to data or a mailbox

- **WHEN** a match of `bounded` or `unbounded` is read in context in a guide page or the glossary
- **THEN** the noun it qualifies SHALL be an input, data, a file, a feed, a log or a mailbox, never a relay, a class, a run or a kind

#### Scenario: The glossary defines the input sense

- **WHEN** the glossary is read
- **THEN** it SHALL carry an entry "Bounded / unbounded (input)" stating that the words describe a finite and an endless input and are not a property of a relay

### Requirement: Every class setting a guide shows SHALL be a leaf of the settings tree

Every relay setting shown in a tutorial or topic page's code, in any language tab, SHALL be
one the settled settings tree
defines, written in the form that SDK's reference page documents. No such page SHALL show a
retired setting, a retired builder call or a retired environment variable.

#### Scenario: The workflow example declares steps in every tab

- **WHEN** the "As a workflow" section of `docs/public/configuration/relay_settings.md` and the Part 2 tutorial page that places orders are read
- **THEN** each of their language tabs SHALL declare the order executor's run as steps, with its loop shape and failure mode, in that SDK's settled form, and none SHALL name a retired class or flag

#### Scenario: The durability page shows the ladder once

- **WHEN** `docs/public/topics/core/durability.md` is read
- **THEN** it SHALL carry one table naming the three durability levels and the flow run's duplicate rule beside them, and SHALL carry no retired persistence, tier or sync setting

#### Scenario: A grep over the guides finds no retired setting

- **WHEN** the tutorial and topic pages are searched for the retired setting names, builder calls and environment variables the merge change lists as removed
- **THEN** there SHALL be no match

### Requirement: Part 3 SHALL build the session in steps and reach it from the gateway with a call

The Part 3 tutorial under `docs/public/tutorials/agentdesk/` SHALL build the session as a relay
that runs in steps, one pass per turn, started and driven from the CLI, and SHALL let the
reader watch an idle session sleep. It SHALL run the gateway in flow, SHALL have the gateway
call the user's session, and SHALL keep each user's daily token budget on the gateway in state
keyed by user, fed by the token count the call's Response carries.
`docs/public/topics/core/sessions.md` and `docs/public/topics/core/sending_and_calling.md` SHALL teach
the session and `call` with excerpts of the same example.

#### Scenario: The tutorial shows a session sleeping

- **WHEN** the steps of the Part 3 tutorial page that starts a session are followed
- **THEN** the reader SHALL see a session answer a turn from the CLI, go idle, be shown as sleeping by the CLI, and answer the next turn with its state intact

#### Scenario: The gateway runs in flow

- **WHEN** the Part 3 tutorial page that serves HTTP is read
- **THEN** its gateway SHALL declare no run setting, so it runs in flow, and SHALL answer a request for a session's events with that session's stream, in the Java and Elixir tabs

#### Scenario: The gateway calls the user's session and meters its answers

- **WHEN** the Part 3 tutorial page that calls the session is read
- **THEN** the gateway's graph SHALL call the user's session with a deadline, count the tokens each Response carries in a tasklet keyed by user, and answer 429 once the user is over the day's budget, in the Java and Elixir tabs

### Requirement: The glossary SHALL define run, durable and steps

`docs/internal/glossary.md` SHALL define how a relay runs (flow or steps), where its work is
kept (memory, the controller's disk, the remote copy), and what steps add (pass, stage,
journal, undo, rewind, the loop shapes).

#### Scenario: The three entries exist

- **WHEN** the glossary is read
- **THEN** it SHALL carry entries for the run setting, the durable setting and steps, each naming the setting that turns it on

### Requirement: Each part's example SHALL run from its pages in every language the part is written for

Following a part's tutorial pages in order SHALL produce, with the commands and the code the
cloned example carries, the working example the part's opening page promises, in each
SDK language for Parts 1 and 2, and in Java and Elixir for Part 3, whose other
language tabs say the page is not yet written for them.

#### Scenario: Part 1 runs

- **WHEN** the tutorial pages of Part 1 are followed in order in one SDK language
- **THEN** the echo relay SHALL read the input file and write the output file the pages show

#### Scenario: Part 2 runs

- **WHEN** the tutorial pages of Part 2 are followed in order in one SDK language
- **THEN** the watcher SHALL write buy signals, an order executor SHALL be started per buy and by hand, SHALL end filled or refused, and the crash-and-recover steps SHALL show the recovery the page describes

#### Scenario: Part 3 runs

- **WHEN** the tutorial pages of Part 3 are followed in order in one SDK language
- **THEN** a session SHALL answer turns from the CLI and sleep when idle, the gateway SHALL answer HTTP and stream an answer to a browser, the session it calls SHALL answer, and the budget SHALL refuse a user over it

#### Scenario: Every language

- **WHEN** the three scenarios above are run
- **THEN** Parts 1 and 2 SHALL have been run in Elixir, Java, Node, PHP, Python, Ruby and Rust, Part 3 in Elixir and Java, and the dev note SHALL record each run

### Requirement: `llms.txt` SHALL list what a user of Flezha needs and nothing internal

`mix docs` SHALL write `_build/.doc/llms.txt` with a short description of Flezha, a "Start here"
list, every page of the user-facing guide groups in sidebar order, and the published API
modules. It SHALL NOT list an internal module, a Mix task, or a page of the Architecture or
Contributing groups.

#### Scenario: No internal module is listed

- **WHEN** `mix docs` has run
- **THEN** `_build/.doc/llms.txt` SHALL NOT contain `Mix.Tasks.Bench` or `Flezha.AuthN.Cert`, and
  SHALL contain `Flezha.Service.Relay`

#### Scenario: The start list points at the essentials

- **WHEN** `_build/.doc/llms.txt` is read
- **THEN** its "Start here" list SHALL link what Flezha is, local mode, the CLI page, the
  error reference and `catalogue.json`

### Requirement: Every listed page SHALL be summarized and linked to its full Markdown copy

Each page line in `_build/.doc/llms.txt` SHALL hold the page title, a link to the page's Markdown
copy in `_build/.doc/`, and the first sentence of the page's introduction. Every linked file SHALL
exist, so an assistant can open the full page from the map.

#### Scenario: A page line leads to the full page

- **WHEN** `mix docs` has run
- **THEN** `_build/.doc/llms.txt` SHALL contain a line linking `what_is_flezha.md` with the sentence
  "Flezha is a durable, distributed runtime for stateless and stateful data processing.",
  and `_build/.doc/what_is_flezha.md` SHALL hold that page's full Markdown

#### Scenario: A link without a copy fails the build

- **WHEN** a listed page has no Markdown copy in `_build/.doc/`
- **THEN** the docs step SHALL fail naming that page

### Requirement: `llms.txt` SHALL stay small enough to read whole

The docs step SHALL fail when `_build/.doc/llms.txt` is larger than 32 KB, naming its size.

#### Scenario: An oversized map fails the build

- **WHEN** the map built from a fixture is larger than 32 KB
- **THEN** the docs step SHALL fail and name the size

### Requirement: The operation catalogue SHALL ship with the docs

`mix docs` SHALL copy `crates/cli/catalogue.json` to `_build/.doc/catalogue.json`.

#### Scenario: The catalogue is next to the docs

- **WHEN** `mix docs` has run
- **THEN** `_build/.doc/catalogue.json` SHALL equal `crates/cli/catalogue.json`
