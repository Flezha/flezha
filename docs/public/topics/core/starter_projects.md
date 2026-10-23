# Starter projects

A starter is a small, complete Flezha project in one SDK language, to grow into your own: one
relay, a tasklet that keeps state, a test, and a README with the commands to build and run it.
Each release publishes one per language as a download, and an AI assistant gets the same files
through MCP. Neither needs a controller or the release installed.

**Contents**

1. [What the starter is](#what-the-starter-is)
2. [Download a starter](#download-a-starter)
3. [With an AI assistant](#with-an-ai-assistant)
4. [Limits](#limits)

## What the starter is

The starter is one relay, `starter`, that counts visits per page:

- Its entry channel `hit` takes `{"page": "..."}`. It is
  [described](sending_and_calling.md#a-relay-s-api), so `flz relay info` shows it and an AI
  agent can call it as a tool.
- The channel is keyed by page, so each page has a [cell](streams.md#keys-and-cells) of its own.
- The tasklet `count` keeps the page's total in [state](state.md), changes it only through a
  recorded change, and answers the new total on the output `counts`.
- A test runs the tasklet with the SDK's test harness and no controller, and checks that
  replaying the recorded changes rebuilds the state. The Elixir test runs the relay in
  [local mode](running_and_testing.md#local-mode) instead.

Each SDK's repository holds the same project as `examples/starter`, tested by its own suite.

## Download a starter

Each release page lists `flezha-<language>-starter.tar.gz` for every language. One command
downloads and unpacks it into a new folder, `flezha-<language>-starter/`:

<!-- tabs-open -->

### Java

```sh
$ curl -fsSL https://github.com/flezha/flezha/releases/download/v0.1.0-alpha.1/flezha-java-starter.tar.gz | tar xz
```

### Rust

```sh
$ curl -fsSL https://github.com/flezha/flezha/releases/download/v0.1.0-alpha.1/flezha-rust-starter.tar.gz | tar xz
```

### Node

```sh
$ curl -fsSL https://github.com/flezha/flezha/releases/download/v0.1.0-alpha.1/flezha-node-starter.tar.gz | tar xz
```

### Python

```sh
$ curl -fsSL https://github.com/flezha/flezha/releases/download/v0.1.0-alpha.1/flezha-python-starter.tar.gz | tar xz
```

### Ruby

```sh
$ curl -fsSL https://github.com/flezha/flezha/releases/download/v0.1.0-alpha.1/flezha-ruby-starter.tar.gz | tar xz
```

### PHP

```sh
$ curl -fsSL https://github.com/flezha/flezha/releases/download/v0.1.0-alpha.1/flezha-php-starter.tar.gz | tar xz
```

### Elixir

```sh
$ curl -fsSL https://github.com/flezha/flezha/releases/download/v0.1.0-alpha.1/flezha-elixir-starter.tar.gz | tar xz
```

### Go

```sh
$ curl -fsSL https://github.com/flezha/flezha/releases/download/v0.1.0-alpha.1/flezha-go-starter.tar.gz | tar xz
```

<!-- tabs-close -->

The folder's `README.md` has the commands: build and test, start the runner, then start the
relay and call it from another terminal, with a controller running
([Start the controller](../../getting_started/up_and_running.md#start-the-controller)). The
call answers `{"page":"home","total":1}`, and each call for the same page the next total.

## With an AI assistant

An assistant connected to [`flz mcp`](mcp_server.md) has the knowledge tool
`flezha_new_project`. Given a language, and an example that defaults to `starter`, it answers
the files the download holds, each with its path, content and executable bit, and the README's
commands. The working rules tell the assistant to start a new project from it, rather than
write SDK code from memory. A language or example there is no template for is refused with
`template.unknown`, whose hint lists the valid ones.

## Limits

- **One starter.** `starter` is the only example for now.
- **Nothing is renamed.** The package, module and project names stay the starter's until you
  change them; the README says what to rename.
- **The SDKs are not published yet.** Each README says how to make its SDK available from a
  Flezha checkout first.
