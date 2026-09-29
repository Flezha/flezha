# How the guides work

The guides are three parts, read in order. Each part builds one small example,
page by page, so if you follow the steps you have a working toy at the end of
it. It is there to try things on, not to deploy.

The parts do not share their example, and nothing needs to be left running from
one part to the next. What carries over is what you learned: each part assumes
you have done the ones before it.

## How to follow a guide

A page mixes explanation with the steps you take. Each step has a purple box
with a pencil, right above the command to run or the code to write. The steps
are not optional: to try things for real and reach the working example at the
end of the part, do every one of them, in the order the page gives. The box says
what to do and what it does, and names anything that has to be in place first:

> Check that the controller is up and ready to take commands. It needs a
> controller started first.
> {: .step}

```sh
$ flz boot await
```

```output
boot await: ready
```

A command or a piece of code with no purple box above it is there to read: it
shows an option or a variant, and the guide does not need it.

The copy button on a command copies the command alone, without the `$` prompt.
What the command prints is shown right below it, and your ids, tokens and times
will differ from the ones on the page.

## The three parts

Each part opens with a page of its own: what it builds, what you need for it,
and what each of its pages does.

| Part | What it builds |
|---|---|
| [First steps](first_steps.md) | A controller, a runner of your own and an `echo` operator, with a file on each side of it |
| [Going further](tradedesk/going_further.md) | `tradedesk`, a toy trader: an ETL stream that watches prices, and a workflow that places orders on a demo exchange |
| [Advanced topics](advanced/advanced_topics.md) | Several controllers, plugins of your own, the delivery guarantee and Kubernetes, around an AI agent behind an HTTP service |

## Also useful

- [`cli.md`](cli.md): `flz` itself, how it reaches a controller, its output
  formats, and how a gated verb is identified.
- [`../troubleshooting/`](../troubleshooting/runner_cannot_connect.md): from a
  symptom to its fix.
- [`../quickstart.md`](../quickstart.md): run an operator's graph in your own
  process, with no controller and no deployment. The fastest way to see one
  work, and to test one.
