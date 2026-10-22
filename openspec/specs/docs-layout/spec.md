# docs-layout Specification

## Purpose
TBD - created by archiving change refactor-s2-docs-split. Update Purpose after archive.
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
(`docs/public/topics/plugins/installing_plugins.md`), environment variables and application
settings (the generated tables of `docs/public/configuration/`), every `flz` verb (the rendered
help and `docs/public/topics/cli/command_reference.md`), and the list of pages (the sidebar for
published pages, `docs/internal/README.md` for internal ones).

#### Scenario: An SDK README points at local mode instead of teaching it

- **WHEN** a reader opens any SDK's `README.md` looking for local mode
- **THEN** it SHALL find one line naming the SDK's local-mode entry point and a link to the
  "Local mode" section of `running_and_testing.md`, not a second explanation

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

