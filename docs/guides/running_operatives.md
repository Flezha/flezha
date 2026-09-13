# Running operatives

**Before you start:** finish [`runner_deployment.md`](runner_deployment.md). You
need a controller running and a runner connected to it, offering the `echo`
class.

## What an operative is

What you wrote is a **declaration**: a procedure, and the graph that wires it to
an input and an output. It runs nothing by itself. Your runner registers it
under a **factory id**, and an operative is one running instance created from
that declaration when you ask the controller for one. If the analogy helps, the
declaration is the definition and an operative is a thing made from it.

One declaration backs as many operatives as you need, each under a name you
choose and each independent of the rest: its own entities, its own state, its
own placement in the cluster. Ten exchanges to watch is one declaration started
ten times. What the declaration itself is, and why it is the unit the controller
schedules, is in [the runner guide](runner_deployment.md#the-operative).

## The two kinds

An operative's **kind** is fixed when you start it, and it decides when the
thing is over.

**Stream operatives are always on.** An endless input, no completion, recovery
per entity. `echo` is one, and everything below uses this kind. It runs until
you stop it.

**Workflow operatives terminate.** A graph of procedures run level by level
behind a commit barrier, ending by itself on chain completion, on
`ctx.complete`, on a durable loop's terminal predicate, or at `max_duration`.
Stopping one by hand ends it early; it is not how one is meant to finish.
[`workflow_basics.md`](workflow_basics.md) is the kind's own guide.

## Start one

The runner is connected and offering `echo`, and nothing is running yet.
Declaring an operative on a runner says that process is *able* to serve one.
Starting it is a separate step, and it is the one that creates a running
instance.

```bash
flz operative start my_echo echo
```

Two names, and they are not the same kind of thing:

| | What it is |
| --- | --- |
| `my_echo` | the **operative id**: the instance, a name you invent right here |
| `echo` | the **factory id**: the class, returned by `factoryId()` in your code |

The factory id is the blueprint and the operative id is the thing made from it,
so you can start three instances of one class under three different ids.
Everything after this addresses the instance.

## Confirm it is bound

```bash
flz operative list
```

```
Fetching operatives for namespace 'default'...
Operatives in namespace 'default':

  OPERATIVE                         HEALTH    PARTS   NODE                  RUNNER
  my_echo                           healthy   -       flezha@127.0.0.1      default_23fc85126379
```

`RUNNER` is the column that matters. A name there means the controller found a
runner offering `echo` and bound the instance to it. `(none)`, with
`(unplaced)` under `NODE`, means the instance exists but no runner has been
found to serve it. That almost always means the factory id is spelled one way
in your code and another way on the command line.

## Send one message and see what comes back

```bash
flz operative send my_echo --text hello --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello
```

The first line is the delivery verdict, and it only says the payload was
accepted. It is answered as soon as the input is durably accepted, before your
procedure runs, so it tells you nothing about what happened next.

The second line is the output: the channel name, a tab, and the payload your
procedure emitted. `out` is the name the operative gave that channel with
`.sink("out")`.

`--await 2000` attaches an output listener, delivers the payload, and prints
for two seconds. Read it as **everything this operative emitted during the
window**, not as the answer to your message. A graph is a stream and not a
function: a `filter` can drop your input and print nothing at all, a `flat_map`
or a `key_by` can turn one input into many lines, and an aggregation may emit
minutes later mixed in with earlier inputs. For the hello world those are the
same thing. For a real graph they are not, and the window is the honest
description of both.

## Or watch it from another terminal

```bash
flz operative listen my_echo
```

It prints one line per frame for 30 seconds, which `--duration-ms` changes, and
stops early on Ctrl-C. Send from a second terminal and the lines appear as they
arrive.

Each line is the channel, a tab, and the payload. A frame with no channel
prints `-` in its place, and a frame from a `keyed()` sink adds a third field
carrying the identity stamps, `entity=... seq=... key=...`.

Two options are worth knowing before you need them. `--input` listens to what
the operative ingests rather than what it emits, which is how you tell "my
message never arrived" apart from "it arrived and produced nothing". `--json`
prints one object per line with the channel, the payload and the frame's whole
metadata map, for when you want to pipe it somewhere instead of read it.

The listener is a subscription the controller holds for you. It is dropped when
the window closes, on Ctrl-C, and if your terminal goes away, so there is
nothing left attached and nothing to clean up.

## Stop it

`echo` is a stream operative, so nothing ends it but you:

```bash
flz operative stop my_echo
```

Two things about the lifetime surprise people.

**You start an operative once, not once per session.** The controller keeps the
operative list in a durable registry and rehydrates it on boot, so after a
controller restart `my_echo` comes back on its own. Running `start` again is
not how you bring it back.

**Running `start` again on a live id changes nothing, quietly.** It reports
that the existing instance was left untouched and that the options were not
compared. So editing the command to add `--partitions 3` and re-running it
looks like it worked and did nothing at all. Stop the instance first, then
start it with the options you want.

## Next

An operative with nothing feeding it does nothing.
[`sources_and_sinks.md`](sources_and_sinks.md) attaches a real input and a real
output to it.
