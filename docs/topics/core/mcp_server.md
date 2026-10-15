# MCP server

An AI coding assistant such as Claude Code, Cursor or Codex can build on Flezha
with `flz mcp`, an MCP server that ships in every release. The assistant starts
it and talks to it over stdin and stdout. It tells the assistant how to work
with Flezha, hands it the current SDK form for its language, and, once a
controller runs, lets it operate that controller as the token you give it.
Nothing is copied into your project.

## Add it to Claude Code

From the release folder:

```sh
$ claude mcp add --scope user flezha -- "$PWD/bin/flz" mcp
$ claude mcp list
```

```output
Added stdio MCP server flezha with command: /opt/flezha/bin/flz mcp to user config
File modified: /home/you/.claude.json
flezha: /opt/flezha/bin/flz mcp - ✔ Connected
```

`--scope user` makes it available in every project. The path must be absolute,
because the assistant starts the server from your project's folder.

With authorization off, as in [Up and running](../../getting_started/up_and_running.md), that
is all. With it on, which is a release's default, add it with a token of its own
([below](#which-token-to-give-it)) instead, removing the first one if you added
it:

```sh
$ claude mcp remove --scope user flezha
$ claude mcp add --scope user flezha -e FLEZHA_TOKEN=$FLEZHA_TOKEN -- "$PWD/bin/flz" mcp
```

`flz mcp` reaches the controller the way `flz` does, so the shell that starts
the assistant needs nothing else: the same machine, the same cookie and node
name ([cli.md](../cli/cli.md)).

## Add it to another client

Every MCP client that starts a stdio server takes the same three things: the
command, the argument `mcp`, and the token in the environment. Claude Code
writes them like this; another client names the same three in its own MCP
configuration:

```json
{
  "mcpServers": {
    "flezha": {
      "command": "/opt/flezha/bin/flz",
      "args": ["mcp"],
      "env": { "FLEZHA_TOKEN": "flzt_..." }
    }
  }
}
```

## What the assistant gets

Five tools need no controller and answer from the installed release, so they
always match the version you run:

| Tool | Answers |
| --- | --- |
| `flezha_guide` | The working rules and a map of every guide page |
| `flezha_sdk_example` | The echo example for one language: the current SDK form |
| `flezha_error` | What an error code means, and the hint for the next step |
| `flezha_docs` | One guide page, by the name the map gives it |
| `flezha_new_project` | The files of a new project in one language, and the commands that build, test and run it ([Template commands](../cli/template_commands.md)) |

The rules also go in the server's instructions, which most clients show the
model before it does anything: start a new project from `flezha_new_project`,
read the SDK example before writing SDK code, try
a relay in [local mode](running_and_testing.md#local-mode) first, act on an error's
`code` and `hint`, and call `relay_diagnose` first when a relay misbehaves.

Once a controller answers, each operation the token's role allows is a tool as
well: `relay_list`, `relay_start`, `relay_send`, `dlq_list`,
`dlq_replay` and the rest, the same set the [HTTP API](../../plugins/http_api.md)
serves. A failed call comes back with its [error](../../errors.md) code and hint,
so the assistant can correct itself. Before a controller runs, every operation
an assistant could reach is listed, whatever the token, and a call answers
`controller.unreachable` with the command that starts one; the next call after
it starts reaches it, with no restart.

Your relays are tools too. Each entry channel that has a description is a tool
named `<factory>_<channel>`, such as `orders_place`, whose arguments are the
relay to call, its key when the relay is a partition group, and the channel's input, so an agent calls your relays the way it
calls any tool (tool calling). They are listed for a token whose role may call
relays in its namespace, one per factory and channel however many relays run
it; `relay_api` shows every channel, described or not
([`sending_and_calling.md`](sending_and_calling.md#ai-agents-and-tool-calling)).

`relay_diagnose` is the tool to start from when a relay misbehaves. It answers
one relay's verdict (`ok`, `warning` or `problem`) and its findings, each with
a code, a message and a hint, as
[`flz relay diagnose`](running_and_testing.md#diagnose-a-relay) prints them.

## Which token to give it

Give the assistant its own token, in one namespace, with the narrowest role that
does its job, issued as the HTTP API page shows
([a token for an assistant](../../plugins/http_api.md#a-token-for-an-assistant)):
`flz mcp` takes the same token, in `FLEZHA_TOKEN` or `--token`. Never give it an
`admin@_system` token.

Whatever the token, the assistant cannot stop the controller, issue or revoke
tokens, bind roles or delete a namespace: those are not tools. Once the admin
is claimed, an assistant with no token gets the five knowledge tools and every
operation is refused with `authz.principal_not_bound`; one whose token does not
verify gets the token's own error, such as `authz.token_expired`.

## A controller on another machine

`flz mcp` reaches a controller over Erlang distribution, as `flz` does. For a
controller the assistant's machine cannot reach that way, enable the
[HTTP API plugin](../../plugins/http_api.md) on the controller and connect the
assistant to its `/mcp` address with a bearer token. It serves the same tools,
the five knowledge tools included.

## For a tool that ignores MCP instructions

A client that does not pass a server's instructions to the model can still be
pointed at them. Add this to the `AGENTS.md`, `CLAUDE.md` or rules file your tool
reads:

```markdown
## Flezha

This project runs on Flezha. Before changing Flezha code, call the `flezha_guide`
tool of the `flezha` MCP server and follow its rules. Call `flezha_sdk_example`
for this project's language before writing SDK code.
```

## Limits

- **Each message starts a short VM.** A call takes about a quarter of a second
  with a controller, and about twice that without one.
- **No streaming operations.** `flz relay listen` and `flz observe tail` are
  not tools, as over HTTP.
- **A message up to about 120 KiB.** A larger one is refused before it is sent.
- **The token is read when the assistant starts the server.** After issuing a new
  one, restart the assistant's session.
