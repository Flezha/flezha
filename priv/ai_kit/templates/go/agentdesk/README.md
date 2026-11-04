# Agentdesk: the Go project of the agentdesk tutorial

The chat sessions the [agentdesk tutorial](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/agentdesk/agentdesk.md)
builds, as a standalone project. Nothing here depends on the Flezha source tree:
`go.mod` names one released version of `github.com/flezha/flezha_go_sdk`, the way
your own project would.

Every conversation is a session of its own: an instance of the steps relay `agent`,
which runs one turn per message and keeps the conversation as its state. A turn is
answered by a demo model that lives in this project. It is a stand-in, not a model:
it answers every message with the same sentence, one token at a time, and reports how
many tokens it used. `demo_model.go` is where a real model's client would be called.

In front of the sessions is `gateway`, a flow relay that carries the HTTP
plugin: a POST is a message for a user's session, answered 202 at once, a GET streams
the session's answer as it is written, and each user has a daily token budget.

| File | What is in it |
|---|---|
| `main.go` | the runner: this process joins the `agents` namespace and registers the `agent` and `gateway` factories |
| `agent.go` | a session, a steps relay: one turn per message, and a failed turn fails alone. The `Message` the gateway sends is here |
| `reply.go` | answers a message with the demo model, sends the answer as it grows, and keeps the conversation |
| `demo_model.go` | the demo model |
| `gateway.go` | the HTTP side, a flow relay: it carries the HTTP plugin, answers every request, and calls the session a message is for |
| `request.go` | one HTTP request, as the plugin hands it to the gateway, and the two routes the gateway knows |
| `http.go` | the gateway's answer to a request: a status and a body, or the session to stream |
| `watch.go` | answers a request for a session's events with its stream, and any other request with 404 |
| `budget.go` | keeps the tokens each user's answers used today, and admits a message while they are under the day's budget |
| `admit.go` | answers a message 202 when it goes on to the session, and 429 when its user is over the budget |
| `Dockerfile` | the runner's image: the binary on a Debian 13 runtime |
| `.dockerignore` | keeps everything but the binary out of the runner's image build |
| `controller/Dockerfile` | the controller's image: Flezha's own, with the plugin packages in `controller/plugins/` |
| `compose.yaml` | one controller and one runner, in Docker Compose, with the gateway's HTTP port published on this machine only |
| `cluster.yaml` | a second controller with its runner, and a PostgreSQL database both controllers copy their state to; read on top of `compose.yaml` |
| `k8s/agentdesk.yaml` | the same images on Kubernetes: one pod with the controller and the runner, a volume, and a service |

## Run it

You need Go with a C toolchain, and Docker with Docker Compose.

```sh
eval "$(go run github.com/flezha/flezha_go_sdk/cmd/flz-native@v0.1.1)"
go build
docker compose up -d --build --wait
alias flz='docker compose exec controller bin/flz'
```

The `eval` line fetches the SDK's prebuilt core library and sets `CGO_LDFLAGS`, as the
SDK's [Install](../../README.md#install) describes. The runner's image copies the binary
`go build` built on this machine, which links the C library dynamically; one built
against a C library newer than Debian 13's may not start in it. The runner stops when its standard input closes, and a container's is
closed, so the image hands the runner a pipe that stays open. The alias runs `flz`
inside the controller's container. The runner joins `agents` on its own; the
controller lists it:

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

Docker pulls the controller's image `flezha/flezha:0.1.1` from Docker Hub; the
commands below build it from the checkout instead.
Running the project against a Flezha checkout instead of the published module, or on a
platform with no prebuilt core library, needs the core built there. From the root of
the checkout, build the core and the image once:

```sh
scripts/build-go-sdk.sh --release
docker build -f deploy/docker/prod/Dockerfile.controller -t flezha/flezha:0.1.1 .
```

Then, from this folder, point Go at the checkout, with its absolute path:

```sh
go work init .
go work edit -replace github.com/flezha/flezha_go_sdk=/path/to/flezha/sdk/go
export CGO_LDFLAGS="-L/path/to/flezha/_build/.target/release/go"
```

Git ignores the `go.work` file.

## Where to go next

The tradedesk in [`../tradedesk`](../tradedesk/README.md) is the tradedesk tutorial's project, and
the [calls contract](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/calls.md) is what `flz relay call`
follows.
