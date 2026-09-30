# Build an echo

Part 1 builds the smallest complete Flezha deployment by hand: a controller, a
runner of your own that serves an `echo` operator, and a file on each side of
it. A line added to one file comes back as its echo at the end of the other.
Along the way you meet every piece a larger deployment is made of.

```mermaid
flowchart LR
    I(["echo-in.txt"]) --> R["file plugin<br/>reader"]
    R --> E["my_echo<br/>on your runner"]
    E --> W["file plugin<br/>writer"] --> O(["echo-out.txt"])
```

The pages share one running controller, started on the first page and stopped
on the last.

> #### What you need {: .tip}
>
> - **For the controller**, a Linux machine on x86-64 to run the release, or
>   Docker to run its image. [`controller_deployment.md`](controller_deployment.md)
>   shows both.
> - **For your runner**, one language: pick the one you want to write it in,
>   and install only what its row lists.
>
> | Language | You need |
> |---|---|
> | Java | Java 21 and Maven |
> | Rust | Rust with Cargo |
> | Node | Node.js 20 or later, with npm |
> | Python | Python 3.11 or later, with pip |
> | Ruby | Ruby 3.3 or later, with Bundler |
> | PHP | PHP 8.4 or later with `ext-ffi`, and Composer |
> | Elixir | Elixir 1.17 or later |
>
> - **git**, to get the examples.

| Page | What you end up with |
|---|---|
| [`controller_deployment.md`](controller_deployment.md) | A controller running, four ways to choose from, and a check that it is up |
| [`runner_deployment.md`](runner_deployment.md) | Your own runner connected to it, registering an operator factory |
| [`running_operators.md`](running_operators.md) | An operator started on that runner, taking messages and answering them |
| [`sources_and_sinks.md`](sources_and_sinks.md) | Data arriving from somewhere real, and results going somewhere real |
| [`stop_everything.md`](stop_everything.md) | Everything above stopped, and what comes back when you start again |
