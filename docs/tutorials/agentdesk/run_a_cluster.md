# Run a cluster

On this page you add a second controller and its runner, stop them as if their
machine went down, and see their sessions carry on on the first.
`cluster.yaml` in the agentdesk folder adds the second controller, a runner for
it, and a PostgreSQL database both keep a copy of their state in.
[Clustering](../../topics/deploying/clustering.md) explains how controllers form a cluster
and hand work over.

> Give the cluster its two secrets, and tell Docker Compose to read both files, in this shell.
> {: .step}

```sh
$ export RELEASE_COOKIE=$(openssl rand -hex 32) DURABILITY_PASSWORD=$(openssl rand -hex 16)
$ export COMPOSE_FILE=compose.yaml:cluster.yaml
```

> Start what is new, and restart the controller with its new settings.
> {: .step}

```sh
$ docker compose up -d --build --wait
$ flz cluster status --namespace agents
```

```output
Cluster status for namespace 'agents':

  Leader:     flezha@controller.agentdesk
  Local node: flezha@controller.agentdesk (leader)

  Nodes:
    flezha@controller.agentdesk: 2 relay(s) recovery_state=normal [leader]
    flezha@controller-2.agentdesk: 3 relay(s) recovery_state=normal

  Relay health: suspended=3, healthy=2
```

> Stop the second controller and its runner, as if their machine were shut down.
> {: .step}

```sh
$ docker compose stop controller-2 runner-2
$ flz relay list --namespace agents
```

```output
  RELAY                          HEALTH      PARTS   NODE                  RUNNER
  anna                              suspended   -       flezha@controller.agentdeskagents_c8cade00c78f
  bob                               suspended   -       flezha@controller.agentdeskagents_c8cade00c78f
  carol                             suspended   -       flezha@controller.agentdeskagents_c8cade00c78f
  dave                              suspended   -       flezha@controller.agentdesk(none)
  gateway                           healthy     -       flezha@controller.agentdeskagents_c8cade00c78f
```

The stop drains the second controller, which takes about a minute. Everything
it ran is on the first controller now.

> Send Anna a message, and Dave one over HTTP.
> {: .step}

```sh
$ flz relay call anna --namespace agents --text 'Are you still there?'
$ curl -s -d 'Six' localhost:8080/sessions/dave/messages
```

```output
{"text":"Message 8: you said \"Are you still there?\". I am a demo model.","tokens":13}
dave has used 40 of today's 40 tokens
```

Dave's budget moved with the gateway.

## Next

[Kill the runner](kill_the_runner.md) looks at what a crash in the middle of a
message costs.
