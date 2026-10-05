# Stream an answer

On this page you watch Anna's answer arrive token by token while the turn runs.

`reply` sends each token as a part, through `ctx.part`, while it works. Parts
reach whoever listens on the `answer` output at once; the whole answer follows on
the same output once the turn's step is done, and also answers the caller.
[Streamed answers](../../topics/advanced/streamed_answers.md) explains parts.

<!-- tabs-open -->

### Java

```java
// AgentRelay.java: reply's output answers the caller and the listeners
        c.input(String.class, "turn")                    // entry 1: `flz relay call`
                .into(Reply.class)
                .output("answer");                       // to whoever called, and whoever listens
```

[`AgentRelay.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/src/main/java/com/flezha/examples/agentdesk/AgentRelay.java)

### Rust

TODO: this page is not yet written for Rust.

### Node

TODO: this page is not yet written for Node.

### Python

TODO: this page is not yet written for Python.

### Ruby

TODO: this page is not yet written for Ruby.

### PHP

TODO: this page is not yet written for PHP.

### Elixir

```elixir
# lib/agentdesk/agent_relay.ex: reply's output answers the caller and the listeners
    c
    # entry 1: `flz relay call`
    |> input("turn")
    |> into(Reply)
    # to whoever called, and whoever listens
    |> output("answer")
```

[`lib/agentdesk/agent_relay.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/agentdesk/lib/agentdesk/agent_relay.ex)

<!-- tabs-close -->

> In a second terminal, with the same `flz` alias, listen to Anna's session for five seconds.
> {: .step}

```sh
$ flz relay listen anna --namespace agents --duration-ms 5000
```

> In the first terminal, send Anna a message.
> {: .step}

```sh
$ flz relay call anna --namespace agents --text 'Tell me a story'
```

```output
{"text":"Message 4: you said \"Tell me a story\". I am a demo model.","tokens":13}
```

The listener shows the parts as they were sent, then the answer:

```output
answer	{"text":"Message ","tokens":1}	part=0 attempt=1 cell=anna/reply/
answer	{"text":"4: ","tokens":2}	part=1 attempt=1 cell=anna/reply/
answer	{"text":"you ","tokens":3}	part=2 attempt=1 cell=anna/reply/
...
answer	{"text":"model.","tokens":13}	part=12 attempt=1 cell=anna/reply/
answer	{"text":"Message 4: you said \"Tell me a story\". I am a demo model.","tokens":13}	cell=anna/reply/ seq=7.0
```

## Next

[Add your own plugin](add_your_own_plugin.md) installs the HTTP plugin the
gateway needs.
