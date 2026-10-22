# Template commands

`flz template create` writes a new Flezha project from a template the release
carries: a small, complete project in any SDK language, with a
relay, a tasklet that keeps state, a test and a README with the commands to
build and run it. It needs no controller, and an AI assistant gets the same
files through MCP.

## What the starter is

The `starter` template is one relay, `starter`, that counts visits per page:

- Its entry channel `hit` takes `{"page": "..."}`. It is
  [described](../core/sending_and_calling.md#a-relay-s-api), so `flz relay info` shows it and
  an AI agent can call it as a tool.
- The channel is keyed by page, so each page has a
  [cell](../core/streams.md#keys-and-cells) of its own.
- The tasklet `count` keeps the page's total in [state](../core/state.md),
  changes it only through a recorded change, and answers the new total on the
  output `counts`.
- A test runs the tasklet with the SDK's test harness and no controller, and
  checks that replaying the recorded changes rebuilds the state. The Elixir
  test runs the relay in [local mode](../core/running_and_testing.md#local-mode) instead.

Each SDK's repository holds the same project as `examples/starter`, written in
its current form and tested by its own suite, and the release carries a copy of
each.

## List the templates

From the release folder:

```sh
$ ./bin/flz template list
```

```output
Templates (flz template create <language> <dir> [--example <example>]):

  LANGUAGE  EXAMPLE   SUMMARY
  elixir    starter   A relay that counts visits per page
  go        starter   A relay that counts visits per page
  java      starter   A relay that counts visits per page
  node      starter   A relay that counts visits per page
  php       starter   A relay that counts visits per page
  python    starter   A relay that counts visits per page
  ruby      starter   A relay that counts visits per page
  rust      starter   A relay that counts visits per page
```

`--json` prints the same as data, with each template's files and commands.

## Create a project

Name the language and a folder that does not exist yet, or is empty:

```sh
$ ./bin/flz template create java ../my-app
```

```output
Created ../my-app with 7 files. Next, in ../my-app:
    mvn -q package
    java -jar target/starter.jar
    flz relay start starter --id starter
    flz relay call starter --channel hit --arg page=home
README.md explains each step.
```

The commands are the README's: build and test, start the runner, then start the
relay and call it, from another terminal and with a controller running
([Start the controller](../../getting_started/up_and_running.md#start-the-controller)). `flz` there
is the release's `bin/flz`. The call answers `{"page":"home","total":1}`, and
each call for the same page the next total.

The folder name is the only thing you choose: nothing inside the files is
renamed, and the README says what to rename. A folder that already holds files
is refused with `template.dir_not_empty`, and a language or example the release
does not carry with `template.unknown`, whose hint is `flz template list`.

Until the SDKs are published, each README also says how to make the SDK
available from a Flezha checkout.

## From a release in a container

With `--tar`, `flz template create` writes the files as a tar stream on stdout
and touches no folder, so a release running in a container hands them to the
host, owned by you:

```sh
$ docker exec flezha-controller /flezha/bin/flz template create python my-app --tar | tar x --keep-old-files
$ ls -A my-app
```

```output
count.py
.gitignore
README.md
requirements.txt
runner.py
starter_relay.py
test_starter.py
```

With `--keep-old-files`, `tar` keeps any file that is already there rather than
overwriting it. `--tar` refuses to write to a terminal: pipe it into `tar x`.

## With an AI assistant

An assistant connected to [`flz mcp`](../core/mcp_server.md)
has the knowledge tool `flezha_new_project`. Given a language, and an example
that defaults to `starter`, it answers the same files `flz template create`
writes, each with its path, content and executable bit, and the README's
commands. The working rules tell the assistant to start a new project from it
and write its files, rather than write SDK code from memory.

## Limits

- **One template.** `starter` is the only example for now.
- **Nothing is renamed.** The package, module and project names stay the
  starter's until you change them.
- **The SDKs are not published yet.** A created project builds once its SDK is
  made available as its README says.
