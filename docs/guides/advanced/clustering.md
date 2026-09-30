# Clustering

So far one controller has carried every session. Several controllers can share them: they
form a cluster, each hosts a part of the operators, and when one stops, the others take its
operators over. On this page a second controller joins the desk, takes a share of the
sessions, and is stopped again, and its sessions and the gateway move back with what
they remember.

## A second controller

`cluster.yaml`, in the project's folder, adds a second controller with its own runner,
and a PostgreSQL database. Docker Compose reads it on top of `compose.yaml`:

```yaml
# A second controller and its runner beside compose.yaml's, the two controllers one cluster,
# and a PostgreSQL database both keep a copy of their state in. Used with compose.yaml:
# COMPOSE_FILE=compose.yaml:cluster.yaml.
services:
  controller:
    environment: &cluster
      RELEASE_COOKIE: ${RELEASE_COOKIE:?set RELEASE_COOKIE, the same for every controller}
      FLEZHA_NODE_HOST: controller.agentdesk
      FLEZHA_INITIAL_NODES: flezha@controller.agentdesk,flezha@controller-2.agentdesk
      FLEZHA_DURABILITY_BACKEND: postgres
      FLEZHA_DURABILITY_PG_HOST: postgres
      FLEZHA_DURABILITY_PG_PASSWORD_ENV: DURABILITY_PASSWORD
      DURABILITY_PASSWORD: ${DURABILITY_PASSWORD:?set DURABILITY_PASSWORD}
    networks:
      default:
        aliases: [controller.agentdesk]    # a node name needs a host name with a dot
    depends_on: &postgres
      postgres:
        condition: service_healthy
    stop_grace_period: 75s                 # time to hand its operators over

  controller-2:
    build: controller
    environment:
      <<: *cluster
      FLEZHA_NODE_HOST: controller-2.agentdesk
      FLEZHA_AUTHZ_ENABLED: "false"
    ports:
      - "127.0.0.1:8081:8080"              # the gateway's HTTP port, when it runs here
    volumes:
      - app_data_2:/flezha/app_data
      - sockets_2:/flezha/app_data/sockets
    networks:
      default:
        aliases: [controller-2.agentdesk]
    depends_on: *postgres
    stop_grace_period: 75s

  runner-2:
    build: .
    environment:
      FLEZHA_SOCKET_FILES_PATH: /flezha/app_data/sockets
      FLEZHA_PARK_IDLE_MS: "10000"
    volumes:
      - sockets_2:/flezha/app_data/sockets
    depends_on:
      controller-2:
        condition: service_healthy
    restart: on-failure

  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: flezha
      POSTGRES_PASSWORD: ${DURABILITY_PASSWORD}
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "flezha"]
      interval: 2s
    volumes:
      - durable:/var/lib/postgresql/data

volumes:
  app_data_2:
  sockets_2:
  durable:
```

- **The cookie** is the cluster's shared secret: controllers join only when they hold the
  same `RELEASE_COOKIE`. It comes from your shell, so none is written in the project.
- **The node names** are `flezha@` and a host name, and a host name needs a dot, so each
  controller gets a network alias with one. `FLEZHA_INITIAL_NODES` lists both, which is
  how each finds the other.
- **Each controller has its own runner**, reached through its own socket volume, as a
  runner on each machine would.
- **The database** holds a copy of each controller's state. A session that moves is
  restored from what its old controller had written; without a copy, a controller that
  stops can take that with it. [`durability.md`](../tradedesk/durability.md#where-it-is-kept)
  lists the backends.
- **Port 8081** is the gateway's while `controller-2` hosts it: the HTTP server runs on
  whichever controller hosts the gateway.

## Start the cluster

> Give the cluster its two secrets, and tell Docker Compose to read both files, in the
> shell you run the desk from.
> {: .step}

```sh
$ export RELEASE_COOKIE=$(openssl rand -hex 32) DURABILITY_PASSWORD=$(openssl rand -hex 16)
$ export COMPOSE_FILE=compose.yaml:cluster.yaml
```

> Start what is new, and restart the controller with its new settings.
> {: .step}

```sh
$ docker compose up -d --build --wait
```

The runners of both controllers serve the `agents` namespace:

```sh
$ flz runner list --namespace agents
```

```output
Fetching runners for namespace 'agents'...
Runners in namespace 'agents':
  - agents_401572df4feb
  - agents_2b3c601d59dc
```

> List the operators.
> {: .step}

```sh
$ flz operator list --namespace agents
```

```output
Fetching operators for namespace 'agents'...
Operators in namespace 'agents':

  OPERATOR                          HEALTH      PARTS   NODE                  RUNNER
  anna                              suspended   -       flezha@controller-2.agentdeskagents_2b3c601d59dc
  bob                               suspended   -       flezha@controller-2.agentdeskagents_2b3c601d59dc
  carol                             suspended   -       flezha@controller.agentdeskagents_401572df4feb
  dave                              healthy     -       flezha@controller.agentdeskagents_401572df4feb
  gateway                           healthy     -       flezha@controller-2.agentdeskagents_2b3c601d59dc
```

The new controller took a share: Anna's and Bob's sessions and the gateway moved to
`controller-2` as it joined, so the gateway now answers on port 8081. The `flz` alias
still runs the CLI in `controller`, which sees the whole cluster:

```sh
$ flz cluster status --namespace agents
```

```output
Cluster status for namespace 'agents':

  Leader:     flezha@controller.agentdesk
  Local node: flezha@controller.agentdesk (leader)

  Nodes:
    flezha@controller.agentdesk: 2 operator(s) recovery_state=normal [leader]
    flezha@controller-2.agentdesk: 3 operator(s) recovery_state=normal

  Operator health: suspended=3, healthy=2
```

## Stop a controller

> Stop the second controller and its runner, as if their machine were shut down.
> {: .step}

```sh
$ time docker compose stop controller-2 runner-2
```

```output
...
real	0m57.678s
user	0m0.081s
sys	0m0.034s
```

A controller that is stopped hands its operators to the others before it exits, which is
what `stop_grace_period` leaves time for. Here that took about a minute.

```sh
$ flz operator list --namespace agents
```

```output
Fetching operators for namespace 'agents'...
Operators in namespace 'agents':

  OPERATOR                          HEALTH      PARTS   NODE                  RUNNER
  anna                              suspended   -       flezha@controller.agentdeskagents_401572df4feb
  bob                               suspended   -       flezha@controller.agentdeskagents_401572df4feb
  carol                             suspended   -       flezha@controller.agentdeskagents_401572df4feb
  dave                              suspended   -       flezha@controller.agentdesk(none)
  gateway                           healthy     -       flezha@controller.agentdeskagents_401572df4feb
```

Everything is on `controller` again.

> Send Anna a message.
> {: .step}

```sh
$ flz operator call anna --namespace agents --text 'Are you still there?'
```

```output
{"text":"Message 8: you said \"Are you still there?\". I am a demo model.","tokens":13}
```

Her conversation came with her: her last message, in
[`streaming_to_the_browser.md`](streaming_to_the_browser.md), was her seventh.

> Send Dave a message over HTTP, on port 8080 again.
> {: .step}

```sh
$ curl -si -d 'Six' localhost:8080/sessions/dave/messages
```

```output
HTTP/1.1 429 Too Many Requests
date: Wed, 30 Sep 2026 07:10:30 GMT
content-length: 37
vary: accept-encoding
cache-control: max-age=0, private, must-revalidate
content-type: text/plain; charset=utf-8

dave has used 40 of today's 40 tokens
```

The gateway moved with the budget it kept, so Dave is still over it. Anna is not:

```sh
$ curl -si -d 'Good' localhost:8080/sessions/anna/messages
```

```output
HTTP/1.1 202 Accepted
date: Wed, 30 Sep 2026 07:10:30 GMT
content-length: 12
vary: accept-encoding
cache-control: max-age=0, private, must-revalidate
content-type: text/plain; charset=utf-8

sent to anna
```

Leave `controller-2` stopped: the rest of Part 3 runs on `controller`, and bringing a
stopped controller back into a running cluster is not covered by this guide.

> #### What a real cluster adds {: .info}
>
> The HTTP port moves with the gateway, so clients reach it through a proxy that tries
> each controller, as [`http_server.md`](../../plugins/http_server.md#deployment) says.
> The copy in the database is made in the background, so it can be a little behind;
> [`configuration.md`](../../configuration.md#state-wal-and-durability) lists how often
> it is made, and the other backends' settings.

## Next

[`effectively_once.md`](effectively_once.md) looks at the delivery guarantee: what
reaches a session and the gateway once, and what it takes.
