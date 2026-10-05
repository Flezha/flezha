# Files and sockets



## Where files land

Ten subtrees are all this product writes, and `lib/flezha/layout/table.ex`
declares them: for each, the knob that may move it, whether a directory
claim is owed before the first write, and how `flz reset` treats it. Both
runtimes read that declaration — the controller through
`Flezha.Layout.Table.dir/1`, `flz reset` through the matching rows in
`cli/src/reset/plan.rs` — so at the default settings the directory the
controller uses and the one a user's reset reports are one path. A knob
that moves a subtree moves it for the controller only.

| Subtree | Knob | `flz reset` |
|---|---|---|
| state | `FLEZHA_STATE_DIR` | cleared by default |
| process_hub | `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` | cleared by default |
| certs | `FLEZHA_CA_CERT_PATH` and its siblings | `--include-certs` |
| dlq | `FLEZHA_DLQ_DIR` | `--include-dlq` |
| schedules | `FLEZHA_SCHEDULE_DIR` | `--include-schedules` |
| auth | `FLEZHA_AUTHZ_DIR` | `--include-auth` |
| audit | `FLEZHA_AUDIT_DIR` | `--include-audit` |
| plugins | `FLEZHA_PLUGIN_ROOT` | `--include-plugins` |
| sockets | `FLEZHA_SOCKET_FILES_PATH` (the ladder) | `--include-sockets` |
| logs | `FLEZHA_LOG_PATH` | never |

Each knob's default and precedence are its row in
[Controller environment variables](environment_variables.md); this section
states only which subtrees exist and what reset does with them. `flz reset`
reports every row, the `never` row as kept, and reads none of these knobs but the
ladder's: it clears `<app_data root>/dlq`, not a directory `FLEZHA_DLQ_DIR`
moved it to. `--all` sets every flag in the table, so it also removes every
credential and erases the audit trail; see [`topics/basics/running_relays.md`](../topics/basics/running_relays.md)
for the window until the fresh bootstrap token is claimed.
Two rows are not subdirectories of the data root. `sockets` is resolved by
the ladder described under
[the socket-directory ladder](#the-socket-directory-ladder) below, is frequently outside
`app_data/` altogether, and `--app-data-root` does not move it. `logs` defaults
to `logs/`, relative to the working directory, and the data root does not move
it either.

## The socket-directory ladder

Where the UDS control and data sockets live is one question with one answer,
resolved by `flz_transport` and shared by every SDK — Rust, Java and Node reach
it through `flz_core`, Elixir through `flz_nif`, and the controller through the
same NIF. The candidates, in order:

| # | Candidate | When |
|---|---|---|
| 1 | `$FLEZHA_SOCKET_FILES_PATH` | whenever it is set — **used outright, with no fallback to any later candidate** |
| 2 | `$XDG_RUNTIME_DIR/flezha` | when `XDG_RUNTIME_DIR` is set (every systemd and desktop session sets it) |
| 3 | `/run/flezha` | a controller running as a system service |
| 4 | `app_data/sockets` | the repository, so the development loop needs no configuration |

The **controller** takes the first candidate it can create, stepping over one
another controller is already listening in — taking that directory would wipe a
live controller's sockets and strand its fleet. An explicit
`FLEZHA_SOCKET_FILES_PATH` is still resolved unconditionally: it is what the
user named. It is not *prepared* unconditionally, though — a controller
probes the directory it resolved before clearing it, and refuses to start when
a live peer answers there, naming the path and this variable. A socket nobody
answers is a controller that died hard and is cleared as normal. See
[Several controllers on one machine](../topics/deploying/deploying_a_controller.md#several-controllers-on-one-machine). A **runner** takes
the first holding a live control socket, and when two candidates hold one and
no variable says which is meant, it **refuses to connect and names both** rather
than attaching to a controller the user did not choose. Every path is
reported absolutely, whichever candidate produced it.

Two consequences worth knowing:

- Sockets are runtime state, so they are no longer under `app_data/` by
  default — that directory holds certificates, credentials and durable state.
- `$XDG_RUNTIME_DIR` is per user, so the zero-configuration case is
  same-user. Across users, set `FLEZHA_SOCKET_FILES_PATH` explicitly on both
  sides and use `FLEZHA_SOCKET_DIR_GROUP` on the controller.

**The socket directory is at most 60 bytes.** A Unix domain socket address holds
107 bytes (`sun_path` less its NUL), and the longest name placed in the
directory is a runner's data socket, `secondary_stream_<id>.sock`, whose id
field is at most 24 bytes (a longer runner id is shortened to 24), so 46 bytes
after the separating `/`. A controller with UDS enabled refuses to boot on a
longer directory, before it removes anything in it, naming the directory,
its length and `FLEZHA_SOCKET_FILES_PATH`; a directory of 61 to 79 bytes would
otherwise hold the control socket and refuse every runner whose id is long at
connect. The ladder's own candidates fit on an ordinary host, and the
repository fallback fits while the controller's working directory is at most
43 bytes, which is another reason to prefer a short runtime directory over a
deep checkout.

`flz runner check` runs this resolution and its probe from a shell, with no
runner code and no running controller; see
[`../topics/deploying/deploying_a_controller.md`](../topics/deploying/deploying_a_controller.md).
