# hook-webhook-plugin Specification

## Purpose
The first-party `hook_webhook` controller plugin sends each hook event it is enabled for as one JSON POST to a URL, best effort, with bounded retries and its headers never logged.
## Requirements
### Requirement: `hook_webhook` SHALL POST each hook event as JSON

The first-party controller provider `hook_webhook` SHALL implement all five controller hooks
and SHALL send each event it receives as one HTTP `POST` to its `url`, with the event as a
JSON object in the body, `content-type: application/json`, and the configured `headers`. It
SHALL keep one connection open between requests and SHALL send events in the order it
received them.

#### Scenario: An event reaches the URL

- **GIVEN** a local HTTP test server and `hook_webhook` enabled with its URL and
  `--hook on_relay_started`
- **WHEN** a relay starts in the plugin's namespace
- **THEN** the server SHALL receive one POST whose JSON body holds `hook`
  `on_relay_started`, the relay, and a non-empty `text`

#### Scenario: Slack-style receivers

- **WHEN** any event is sent
- **THEN** its body SHALL hold a top-level string field `text`

### Requirement: `hook_webhook` SHALL validate its options without I/O

`url` SHALL be required, `http` or `https`, with a host and no credentials, and SHALL be
refused otherwise with `plugin.url_scheme_unsupported`, `plugin.url_without_host` or
`plugin.url_has_credentials`. `headers` SHALL be a JSON object of string values, refused with
`plugin.headers_invalid` otherwise. `timeout_ms` (default 5000) and `retries` (default 2) SHALL
be bounded integers. Validation SHALL open no connection.

#### Scenario: A URL with credentials

- **WHEN** `hook_webhook` is enabled with `--Xurl=https://user:pw@hooks.test/x`
- **THEN** the enable SHALL be refused with `plugin.url_has_credentials`

### Requirement: `hook_webhook` SHALL retry a transient failure and then drop

A 2xx response SHALL complete an event. A 429, a 5xx, a timeout or a transport error SHALL be
retried after 1 s and then after 5 s, up to `retries` times, and the event SHALL then be
dropped. Any other status SHALL drop the event at once. Each drop SHALL be counted and logged
at warning level once per plugin and status, naming the plugin and the URL's host but not its
path, query or headers.

#### Scenario: A server that recovers

- **GIVEN** a test server answering 503 once and then 200
- **WHEN** one event is sent
- **THEN** the server SHALL receive the event twice and the plugin SHALL count no drop

#### Scenario: A rejected event is not retried

- **GIVEN** a test server answering 400
- **WHEN** one event is sent
- **THEN** the server SHALL receive it once, the plugin SHALL count one drop, and the log
  SHALL hold one warning without the URL's path

#### Scenario: A slow server does not stop Flezha

- **GIVEN** a test server that never answers, `timeout_ms` 500 and a queue of 10 frames
- **WHEN** 50 relays park in the plugin's namespace
- **THEN** every park SHALL complete, and the plugin SHALL report `plugin_lagging`

