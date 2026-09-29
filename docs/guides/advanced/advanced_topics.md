# Advanced topics

Part 3 goes deeper: several controllers, plugins of your own, the delivery
guarantee, tuning, and running on Kubernetes. Its example, not written yet, is
an AI agent behind an HTTP service, where every user talks to a session of
their own and many run at once. It runs in Docker Compose, which the first page
sets up; the rest can be read in any order.

> #### What you need {: .tip}
>
> - **For the controller**, a Linux machine on x86-64 to run the release, or
>   Docker to run its image. [`controller_deployment.md`](../controller_deployment.md)
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
> - **Docker, with Docker Compose**, from [`clustering.md`](clustering.md) on.
> - **A Kubernetes cluster, such as k3s, and `kubectl`**, for
>   [`kubernetes_deployment.md`](kubernetes_deployment.md).
> - **Elixir 1.17 or later**, for
>   [`elixir_applications.md`](elixir_applications.md).
> - **git**, to get the examples.

| Page | Covers |
|---|---|
| [`clustering.md`](clustering.md) | Set up the example in Docker, run several controllers, then stop one |
| [`custom_plugins.md`](custom_plugins.md) | Writing and installing a plugin of your own |
| [`full_form_tasklets.md`](full_form_tasklets.md) | Writing a tasklet without the sugar, and what only that form declares |
| [`effectively_once.md`](effectively_once.md) | The delivery guarantee, what it requires, and what it costs |
| [`erlang_vm_tuning.md`](erlang_vm_tuning.md) | Scheduler, memory and IO settings for a controller under load |
| [`elixir_applications.md`](elixir_applications.md) | Flezha inside an Elixir application of your own |
| [`kubernetes_deployment.md`](kubernetes_deployment.md) | Run the same images on k3s |
