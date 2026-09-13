# Introduction

Flezha runs stateful compute across a cluster and keeps it running. You write a
small amount of code, a **procedure**, and declare how data reaches it, an
**operative**. Flezha decides where that runs, restarts it when it fails, moves
it when a machine goes away, and remembers its state across all of it.

Two kinds of process make that work:

- A **controller** is the brain. It owns the durable state, decides placement,
  heals what breaks, and serves the operator surface.
- A **runner** is a process of yours that executes operative code. It offers
  capacity to a controller and does what it is told.

The guides below build that up in order. **Part 1 gets something running.**
Parts 2 and 3 take it apart one subject at a time, and you can read those in any
order once Part 1 works.

## Part 1: getting started

The shortest path from nothing to data flowing. Read these in order; each one
assumes the one before it.

| Guide | What you end up with |
|---|---|
| [`controller_deployment.md`](controller_deployment.md) | A controller running, four ways to choose from, and a check that it is up |
| [`runner_deployment.md`](runner_deployment.md) | Your own runner connected to it, serving an operative |
| [`using_plugins.md`](using_plugins.md) | Data arriving from somewhere real, and results going somewhere real |

**Part 1 turns authorization off**, to keep things simple while you get a first
run working. That is a real feature switched off, not a default, and
[`authorization.md`](authorization.md) turns it back on.

## Part 2: the basics

One subject each. Read what you need.

| Guide | Covers |
|---|---|
| [`authorization.md`](authorization.md) | Turning enforcement on, claiming the admin, tokens, roles, and what a runner has to prove |
| [`clustering.md`](clustering.md) | More than one machine: how controllers find each other and what that buys |
| [`durability.md`](durability.md) | What survives a crash, and what it costs |
| [`stream_basics.md`](stream_basics.md) | The stream profile: channels, keys, entities, and state |
| [`workflow_basics.md`](workflow_basics.md) | The workflow profile: steps, retries, and compensation |

## Part 3: advanced topics

| Guide | Covers |
|---|---|
| [`custom_plugins.md`](custom_plugins.md) | Writing and installing a plugin of your own |
| [`effectively_once.md`](effectively_once.md) | The delivery guarantee, what it requires, and what it costs |
| [`erlang_vm_tuning.md`](erlang_vm_tuning.md) | Scheduler, memory and IO settings for a controller under load |

## When something is wrong

[`../troubleshooting/`](../troubleshooting/runner_cannot_connect.md) is symptom
to fix: what a message means and what to do about it.

## Trying it without deploying anything

Every SDK can run an operative's declared graph in your own process, with no
controller, no socket and no deployment. It is the fastest way to see one work
and the fastest way to test one. [`../quickstart.md`](../quickstart.md).
