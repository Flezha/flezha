# Kill the runner

On this page you kill the runner while a message goes through the gateway, and
see that its tokens are counted once. The sessions run in steps, which keep a
durable input log and a journal; the gateway runs in flow and declares
effectively-once delivery, `exactly: effectively_once`, which gives it a
durable input log too. [Effectively once](../effectively_once.html)
explains the guarantee, what it requires and what it costs.

> Read what Anna has used today.
> {: .step}

```sh
$ flz state get gateway/budget/anna --namespace agents
```

```output
state get gateway/budget/anna:
  change_seq: 4
  state:      {"budget":{"anna":{"day":"2026-09-30","tokens":36}}}
```

> Send Anna a message, and kill the runner a second later.
> {: .step}

```sh
$ curl -s -d 'Tell me everything' localhost:8080/sessions/anna/messages && sleep 1 && docker compose kill runner
```

> Start the runner again, and read the budget once Anna has answered.
> {: .step}

```sh
$ docker compose start runner
$ flz state get gateway/budget/anna --namespace agents
```

```output
state get gateway/budget/anna:
  change_seq: 6
  state:      {"budget":{"anna":{"day":"2026-09-30","tokens":48}}}
```

The message's 12 tokens were counted once, not twice. The gateway's input log
shows the request and the call's response, each consumed once:

```sh
$ flz state inbox gateway --namespace agents
```

```output
...
  offset=3 consumed 102 byte(s) channel=http
  offset=4 consumed 265 byte(s) channel=budget.response
```

## Next

[Run on Kubernetes](run_on_kubernetes.md) moves the same images to k3s.
