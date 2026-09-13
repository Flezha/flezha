# file-source-provider Specification

## Purpose
TBD - created by archiving change add-a-file-plugin. Update Purpose after archive.
## Requirements
### Requirement: A file source SHALL inject one entry frame per line through the confirming ingest

A built-in provider `operative_file_source` SHALL be attachable as a plugin with options
`{path, channel | channel_in, start_at? (beginning | end, default beginning), poll_interval_ms?
(default 1000), max_line_bytes? (default 1 MiB), read_chunk_bytes? (default 64 KiB),
commit_interval_ms? (default 1000), failure_threshold? (default 3), idle_after_ms? (default 0,
derived), plugin_id?}`. It SHALL declare that it feeds the operative, so `channel` resolves as
the entry channel by the rule in `operative-extensions`; `channel_in` SHALL be the explicit
spelling of the same name, one of the two SHALL be required, and `channel_out` SHALL be refused
as an option this provider does not offer.

A record SHALL be the bytes of one line up to and excluding its newline. Each record SHALL be
injected on the entry channel through the operative's confirming ingest, and the reader SHALL
NOT read past one chunk ahead of the last confirmed line, so a payload the operative did not
take is offered again and the controller's memory is bounded by one chunk and one line. A
trailing fragment with no newline SHALL NOT be delivered until a newline follows it. An empty
line SHALL be delivered as an empty string. A line that is not valid UTF-8 SHALL stop the
reader at that offset.

`init_provider/1` SHALL perform no reading: the worker opens the file at activation.

#### Scenario: Three lines become three frames in order

- **GIVEN** a file holding `a\nb\nc\n`
- **WHEN** the plugin is enabled with `channel_in: "events"`
- **THEN** exactly three frames `a`, `b`, `c` SHALL be injected on `events`, in that order

#### Scenario: A fragment waits for its newline

- **GIVEN** a file holding `a\nb`
- **WHEN** the plugin has delivered `a`
- **THEN** no second frame SHALL be injected until a newline is appended, and `b` SHALL be
  injected once it is

#### Scenario: A refused ingest is re-offered

- **WHEN** the operative refuses an injected line
- **THEN** the reader SHALL offer the same line again on its next attempt and SHALL NOT advance
  past it

#### Scenario: The chain-side key is refused

- **WHEN** a plugin is attached with `channel_out`
- **THEN** the attach SHALL fail, naming `channel_out` as an option this provider does not offer

#### Scenario: Neither channel spelling is refused

- **WHEN** a plugin is attached with neither `channel` nor `channel_in`
- **THEN** the attach SHALL fail naming the two spellings

### Requirement: A file source SHALL follow a growing file and resume at its committed offset

The reader SHALL poll the file every `poll_interval_ms` for new bytes. It SHALL commit the byte
offset of the end of the last confirmed line through the plugin's persisted option seam, after a
batch of confirmations or `commit_interval_ms`, whichever comes first, and SHALL read the
committed value back from an internal `offset` option on every restart path: a plugin restart,
a live move to another controller, and a controller restart. A committed offset SHALL win over
`start_at`; with no committed offset, `beginning` SHALL start at byte 0 and `end` at the file's
current size.

Delivery SHALL be at-least-once: a crash after a confirmation and before its commit re-delivers
the lines confirmed since the last commit, and SHALL NOT skip one.

A file whose size is below the committed offset, or whose inode has changed, SHALL be treated as
truncated or replaced: the reader SHALL reopen at byte 0, commit 0, and log a warning naming the
path and the old offset. A missing file SHALL NOT be treated as truncation: the reader SHALL
keep polling and report unhealthy until it appears.

#### Scenario: Appended lines are delivered

- **GIVEN** a plugin that has delivered every line of its file
- **WHEN** `d\n` is appended
- **THEN** `d` SHALL be injected within one `poll_interval_ms`

#### Scenario: A plugin restart resumes after the last committed line

- **GIVEN** a plugin that has confirmed and committed three lines
- **WHEN** the plugin is restarted and a fourth line is appended
- **THEN** only the fourth line SHALL be injected

#### Scenario: A controller restart resumes after the last committed line

- **GIVEN** a plugin that has confirmed and committed three lines
- **WHEN** the controller restarts and the operative is rebuilt from its declared manifest
- **THEN** the reader SHALL start at the committed offset and inject nothing until a new line
  is appended

#### Scenario: A crash before the commit re-delivers, never skips

- **GIVEN** a plugin that has confirmed five lines and committed through the third
- **WHEN** the worker crashes and restarts
- **THEN** the fourth and fifth lines SHALL be injected again and the sixth SHALL NOT be
  skipped

#### Scenario: `start_at: end` ignores existing content

- **GIVEN** a file holding three lines and no committed offset
- **WHEN** the plugin is enabled with `start_at: "end"` and a fourth line is appended
- **THEN** only the fourth line SHALL be injected

#### Scenario: A truncated file restarts at zero

- **GIVEN** a plugin with a committed offset of 300
- **WHEN** the file is truncated to 0 and `x\n` is written
- **THEN** `x` SHALL be injected, the committed offset SHALL be 2, and a warning SHALL name the
  path and `300`

### Requirement: A file source SHALL bound memory and self-report health

A line longer than `max_line_bytes` SHALL stop the reader at that line's offset and report
`{:unhealthy, {:line_too_long, offset}}`; the committed offset SHALL NOT advance past it. A
missing file SHALL report `{:unhealthy, {:file_missing, path}}`. `failure_threshold`
consecutive read failures SHALL report `{:unhealthy, {:read_failing, error}}`. A reader whose
last settled poll is older than the idle window (`idle_after_ms`, or three `poll_interval_ms`
when 0) SHALL report `{:unhealthy, {:poll_idle, age_ms}}`. A worker that does not answer the
probe inside its budget SHALL report `{:fail_open, :probe_timeout}`, per the provider health
contract. A file that does not grow SHALL read healthy.

#### Scenario: An oversized line stops the reader

- **GIVEN** `max_line_bytes: 16`
- **WHEN** the file holds a 20-byte line at offset 40
- **THEN** no frame SHALL be injected for it, health SHALL report
  `{:unhealthy, {:line_too_long, 40}}`, and the committed offset SHALL be at most 40

#### Scenario: A missing file is unhealthy and recovers when it appears

- **WHEN** the file is absent at activation
- **THEN** health SHALL report `{:unhealthy, {:file_missing, path}}`, and once the file is
  created with `a\n` the plugin SHALL inject `a` and report healthy

#### Scenario: A quiet file is healthy

- **GIVEN** a file that has not grown for many poll intervals
- **WHEN** health is queried
- **THEN** it SHALL report `:healthy`

#### Scenario: A reader that stopped polling is unhealthy

- **GIVEN** a reader whose last settled poll is older than the idle window
- **WHEN** health is queried
- **THEN** it SHALL report `{:unhealthy, {:poll_idle, age_ms}}`

