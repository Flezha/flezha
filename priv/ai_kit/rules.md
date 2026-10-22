# Working with Flezha

Flezha runs relays: small graphs of tasklets, written with one of the SDKs
(Elixir, Go, Java, Node, PHP, Python, Ruby, Rust) and served by a runner
process that connects to a controller. These rules hold for every task.

1. To start a new Flezha project, call `flezha_new_project` and write its
   files, rather than writing SDK code from memory.
2. Before writing SDK code, call `flezha_sdk_example` for the project's
   language and follow the form it shows. The SDK forms have changed between
   releases; never write one from memory or from an older project.
3. Try a relay in local mode before starting a controller: it runs the
   relay's graph in your own process, with no controller and no socket.
   The `running_and_testing.md` page shows how, under "Local mode".
4. Every Flezha failure carries a `code` and a `hint`. Read both and follow the
   hint before retrying. Call `flezha_error` for a code you do not know. When
   you use the shell, run `flz` with `--json`.
5. When the operation tools are connected (`relay_list`, `relay_start`,
   `dlq_list` and the rest), use them rather than the shell. A call answered
   `controller.unreachable` means no controller runs yet: its hint names the
   command that starts one.
6. When a relay misbehaves, call `relay_diagnose` before anything else, and
   act on its findings' hints, problems first.
7. Never ask for an admin token, and never stop the controller, reset its data
   or drop dead letters unless the user asks for that.
8. `flezha_guide` lists every guide page with one line on each. Open only the
   pages a task needs, with `flezha_docs`, rather than guessing what one says.
9. A relay can be called as a tool. Each described entry channel of a relay is
   a tool of its own, named after its factory and channel joined by an
   underscore, whose arguments are the relay to call and the channel's input.
   `relay_api` shows each channel's format, schema, description and whether it
   answers, as `flz` does for one relay; `relay_call_channel`
   calls any channel, described or not.
