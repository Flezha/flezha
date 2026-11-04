# Agentdesk: the Node project of the agentdesk tutorial

The chat sessions the [agentdesk tutorial](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/agentdesk/agentdesk.md)
builds, as a standalone project. Nothing here depends on the Flezha source tree:
`package.json` names one released version of `@flezha/sdk`, the way your own project
would. It needs Node 20 or later.

Every conversation is a session of its own: an instance of the steps relay `agent`,
which runs one turn per message and keeps the conversation as its state. A turn is
answered by a demo model that lives in this project. It is a stand-in, not a model:
it answers every message with the same sentence, one token at a time, and reports how
many tokens it used. `demo_model.mjs` is where a real model's client would be called.

In front of the sessions is `gateway`, a flow relay that carries the HTTP
plugin: a POST is a message for a user's session, answered 202 at once, a GET streams
the session's answer as it is written, and each user has a daily token budget.

| File | What is in it |
|---|---|
| `runner.mjs` | the runner: this process joins the `agents` namespace and registers the `agent` and `gateway` factories |
| `agent_relay.mjs` | a session, a steps relay: one turn per message, and a failed turn fails alone |
| `reply.mjs` | answers a message with the demo model, sends the answer as it grows, and keeps the conversation |
| `demo_model.mjs` | the demo model |
| `gateway_relay.mjs` | the HTTP side, a flow relay: it carries the HTTP plugin, answers every request, and calls the session a message is for |
| `request.mjs` | one HTTP request, as the plugin hands it to the gateway, and the two routes the gateway knows |
| `watch.mjs` | answers a request for a session's events with its stream, and any other request with 404 |
| `budget.mjs` | keeps the tokens each user's answers used today, and admits a message while they are under the day's budget |
| `admit.mjs` | answers a message 202 when it goes on to the session, and 429 when its user is over the budget |
| `Dockerfile` | the runner's image: the project and its `node_modules/` on Node 26 |
| `controller/Dockerfile` | the controller's image: Flezha's own, with the plugin packages in `controller/plugins/` |
| `compose.yaml` | one controller and one runner, in Docker Compose, with the gateway's HTTP port published on this machine only |
| `cluster.yaml` | a second controller with its runner, and a PostgreSQL database both controllers copy their state to; read on top of `compose.yaml` |
| `k8s/agentdesk.yaml` | the same images on Kubernetes: one pod with the controller and the runner, a volume, and a service |

Every tasklet is one object handed to `defineTasklet`, as in the tradedesk. What a
tasklet remembers, the changes that record it, and the values the tasklets take and
hand on are plain objects; only the gateway's input is a `Request`, which the entry
channel decodes the plugin's envelope into.

## Run it

You need Node 20 or later with npm, and Docker with Docker Compose.

```sh
npm install
docker compose up -d --build --wait
alias flz='docker compose exec controller bin/flz'
```

The runner's image copies `node_modules/` as npm left it. npm fetches the SDK's native
part for the machine it runs on, so run `npm install` on Linux, on the processor the
image runs on. A Node runner stops when its standard input closes, and a container's is
closed, so the image hands the runner a pipe that stays open. The alias runs `flz`
inside the controller's container. The runner joins `agents` on its own; the controller
lists it:

```sh
flz runner list --namespace agents
```

## Talk to a session

Start a session, then send it a turn and wait for its answer:

```sh
flz relay start agent --id anna --namespace agents
flz relay call anna --namespace agents --text 'Hello'
```

```
{"text":"Message 1: you said \"Hello\". I am a demo model.","tokens":10}
```

## Talk to it over HTTP

With the HTTP plugin's package in `controller/plugins/` ([Add your own plugin](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/agentdesk/add_your_own_plugin.md) builds it), start
the gateway, enable the plugin on it, and send a message from this machine:

```sh
flz relay start gateway --id gateway --namespace agents
flz plugin enable web http_server --relay_id=gateway --namespace agents --hook=on_terminate --Xport=8080 --Xbind=0.0.0.0 --Xchannel=http
curl -s -d 'Hello from the web' localhost:8080/sessions/anna/messages
curl -sN localhost:8080/sessions/anna/events
```

The example runs with authorization off, and the plugin authenticates nobody: it is a
demonstration, not a setup to expose.

`docker compose down -v` stops both containers and removes the sessions with their volumes.

## Run it as a cluster, or on Kubernetes

With `RELEASE_COOKIE` and `DURABILITY_PASSWORD` set in the shell,
`COMPOSE_FILE=compose.yaml:cluster.yaml docker compose up -d --build --wait` adds a
second controller; [Run a cluster](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/agentdesk/run_a_cluster.md)
walks it. [Run on Kubernetes](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/agentdesk/run_on_kubernetes.md)
builds the images and deploys `k8s/agentdesk.yaml` on k3s.

## Building from a checkout

Docker pulls the controller's image `flezha/flezha:0.1.0` from Docker Hub; the
commands below build it from the checkout instead.
To run the project against
a Flezha checkout instead of the published package, also build the SDK and its native
part. From the repository root:

```sh
scripts/build-node-sdk.sh --release
docker build -f deploy/docker/prod/Dockerfile.controller -t flezha/flezha:0.1.0 .
```

The first needs a Rust toolchain. Then, in this folder, install the checkout's package
instead of `npm install`, as a copy rather than a link, since the image copies
`node_modules/`, and put the native part beside it:

```sh
npm install --no-save --install-links ../..
mkdir -p node_modules/@flezha/sdk/native
cp ../../native/flz_core_ffi.node node_modules/@flezha/sdk/native/
```

`--no-save` keeps `package.json` as it is.

## Where to go next

The tradedesk in [`../tradedesk`](../tradedesk/README.md) is the tradedesk tutorial's project, and
the [calls contract](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/calls.md) is what `flz relay call`
follows.
