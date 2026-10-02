# Build an AI chat

Part 3 builds `agentdesk`, a chat in which every conversation is a session of its
own, and many run at once. A session remembers its conversation, answers each message
as it is written, sleeps while nobody talks to it, and can take turns back. The answers
come from a demo model inside the example, a stand-in that repeats one canned sentence,
so nothing outside the example is needed.

Everything runs in Docker containers: a controller and a runner, started with Docker
Compose. The later pages put the sessions behind an HTTP service with a plugin, run
several controllers, add the delivery guarantee, and move the same images to Kubernetes.

```mermaid
flowchart LR
    CLI(["flz"]) -->|a turn| A["anna<br/>agent session"]
    CLI -->|a turn| B["bob<br/>agent session"]
    A -->|the answer, token by token| CLI
```

> #### What you need {: .tip}
>
> - **Docker, with Docker Compose.** Both the controller and the runner run in
>   containers.
> - **For your runner**, one language: pick the one you want to write it in,
>   and install only what its row lists.
>
> | Language | You need |
> |---|---|
> | Java | Java 21 and Maven |
> | Elixir | Elixir 1.17 or later with Erlang/OTP |
>
>   Part 3 is not yet written for Rust, Node, Python, Ruby or PHP.
>
> - **A Kubernetes cluster, such as k3s, and `kubectl`**, for
>   [`kubernetes_deployment.md`](kubernetes_deployment.md).
> - **git**, to get the examples and the HTTP plugin.

| # | Page | What you do |
|---|---|---|
| 1 | [`sessions.md`](sessions.md) | Run agentdesk in Docker Compose, one session per conversation, an operator that runs in steps, send it turns with the CLI, and watch an idle one sleep |
| 2 | [`rewinding_a_turn.md`](rewinding_a_turn.md) | Take turns back with an undo, and let a failed turn fail alone |
| 3 | [`streamed_answers.md`](streamed_answers.md) | Send the answer token by token as parts, and watch it with `flz operator listen` |
| 4 | [`custom_plugins.md`](custom_plugins.md) | Install the HTTP plugin by rebuilding the controller's image, and see a plugin of your own |
| 5 | [`http_gateway.md`](http_gateway.md) | Serve HTTP from a gateway operator, and stream a session's answer to a browser |
| 6 | [`operator_calls.md`](operator_calls.md) | Call a user's session from the gateway, and count each answer's tokens against a daily budget |
| 7 | [`clustering.md`](clustering.md) | Run several controllers, stop one, and watch its sessions move |
| 8 | [`effectively_once.md`](effectively_once.md) | The delivery guarantee, what it requires and what it costs |
| 9 | [`kubernetes_deployment.md`](kubernetes_deployment.md) | Run the same images on k3s |
