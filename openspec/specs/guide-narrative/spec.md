# guide-narrative Specification

## Purpose
What the tutorials, the topic pages and the glossary teach about relays: one relay described by its settings, never by a kind; the words bounded and unbounded kept for input data; and each part's example run from its pages in the languages it is written for.
## Requirements
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
- **THEN** each of their seven language tabs SHALL declare the order executor's run as steps, with its loop shape and failure mode, in that SDK's settled form, and none SHALL name a retired class or flag

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
cloned example carries, the working example the part's opening page promises, in each of the
seven SDK languages for Parts 1 and 2, and in Java and Elixir for Part 3, whose other five
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
