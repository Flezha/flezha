# Tutorials

Each tutorial runs one small example application, page by page, so if you
follow the steps you have a working toy at the end. It is there to try things
on, not to deploy. The tutorials do not share their example, and nothing needs
to be left running from one to the next; each assumes you have done the ones
before it.

A tutorial page shows what to do and what you should see. What a piece of
Flezha is and how it works is on its topic page, which every tutorial links at
the first mention: the [topics](../topics/basics/building_blocks.md).

## How to follow a tutorial

Each step has a purple box with a pencil, right above the command to run. The
steps are not optional: to reach the working example at the end, do every one
of them, in order. The box says what to do and names anything that has to be in
place first:

> Check that the controller is up and ready to take commands. It needs a
> controller started first.
> {: .step}

```sh
$ flz boot await
```

```output
boot await: ready
```

A command with no purple box above it is there to read or to check, and the
tutorial does not need it. The copy button on a command copies the command
alone, without the `$` prompt. What it prints is shown right below it, and your
ids, tokens and times will differ.

## The three tutorials

| Tutorial | What it runs |
|---|---|
| [The echo](echo/first_steps.md) | A controller, a runner of your own and an `echo` operator, with a file on each side of it |
| [The tradedesk](tradedesk/tradedesk.md) | A toy trader: an ETL stream that watches prices, and a workflow that places orders on a demo exchange |
| [The agentdesk](agentdesk/agentdesk.md) | An AI agent behind an HTTP service: sessions, streamed answers, calls between operators, several controllers and Kubernetes |

## Also useful

- [CLI](../topics/cli/cli.md): `flz` itself, how it reaches a controller and
  its output formats.
- [Troubleshooting](../troubleshooting/runner_cannot_connect.md): from a symptom
  to its fix.
- [Local mode](../topics/basics/local_mode.md): run an operator's graph in your
  own process, with no controller.
