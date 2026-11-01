# The console

The console is Flezha's web console: a Phoenix app you run beside your controllers. It draws
each relay with its records moving through it, taps any channel, shows a keyed tasklet's
Cells, starts, feeds and stops relays, enables plugins and schedules, and says in plain words
what needs you. Its code is the `flezha_console` repository, under the MPL-2.0, like the
controller.

| Guide | How it runs |
|---|---|
| [Docker](console_docker.md) | The console, its database and a controller in one Compose file |
| [Kubernetes](console_kubernetes.md) | A deployment beside the controllers, found by a headless service |
| [Bare metal](console_bare_metal.md) | A release on your own machine, run by systemd |

## What it needs

- **A controller to reach.** The console is an Erlang node that connects to one controller
  node and calls the controller's published API through it.
- **The controller's cookie.** Erlang distribution admits a node that knows the cookie, so
  the console takes the same `RELEASE_COOKIE` as the controllers. Treat it as a secret: a node
  with the cookie can do anything on the controller.
- **A place inside the cluster's trust boundary.** Holding the cookie, the console's node is
  trusted like a controller: whoever controls the machine it runs on controls the cluster,
  whatever its token says. Run it where you would run a controller.
- **A token.** The console acts as the principal its token names. An `admin` token in the
  `_system` namespace lets it list every namespace; a narrower one shows less. A signed-in
  user can also store a token of their own under **Settings**, and then the audit log names
  that user.
- **A Postgres database.** The console keeps its own accounts in it: who can sign in, their
  sessions and the tokens they stored. Everything it shows about relays is the controller's
  answer, read when the screen asks.

## How it reaches the controller

The console starts as a hidden node: it connects to the controller without joining the
cluster's full mesh, so the controllers never place work on it. Three settings make the link:

| Variable | What it is |
|---|---|
| `RELEASE_DISTRIBUTION` | `name`, so the node uses a long name |
| `RELEASE_NODE` | The console's own node name, `flezha_console@<host>` |
| `RELEASE_COOKIE` | The controllers' cookie |
| `ERL_FLAGS` | `-hidden` |

It finds the controller one of two ways:

- **`FLEZHA_NODE`** names the controller node, such as `flezha@controller`. Use it where the
  controller keeps one name: Docker, a single machine.
- **`FLEZHA_NODE_QUERY`** names a DNS name instead, such as
  `flezha@flezha-controllers.flezha.svc.cluster.local`. The console resolves it on every
  call and connects to `flezha@<address>` for an address it returns, keeping a controller it
  is already connected to. Use it where controllers are named after addresses that change,
  as on Kubernetes.

The controller's node name is `flezha@` and its `FLEZHA_NODE_HOST`, `127.0.0.1` unless set.
Epmd (port 4369) and the controller's distribution port must be reachable from the console.

By default the link is plain TCP, guarded by the cookie alone. To encrypt it and have both
ends check certificates, run the controllers over
[mutual TLS](../topics/core/authorization.md#distribution-with-mutual-tls) and give the console
a certificate of its own from the same distribution authority
(`deploy/dist-tls/make-certs.sh node <dir> console --dns <its host>`), with the same options
file: `ELIXIR_ERL_OPTIONS="-proto_dist inet_tls -ssl_dist_optfile <file>"`.

## Settings

| Variable | Required | What it is |
|---|---|---|
| `DATABASE_URL` | yes | The console's database, `ecto://user:password@host/database` |
| `SECRET_KEY_BASE` | yes | Signs and encrypts sessions; 64 random bytes, `openssl rand -base64 48` |
| `PHX_HOST` | yes | The host name browsers use to reach the console |
| `PHX_URL_SCHEME`, `PHX_URL_PORT` | no | The scheme and port browsers use, `https` and `443` unless set. LiveView compares a page's origin with them, so a console served over plain `http` on another port must say so |
| `PORT` | no | The port the console listens on, `4000` unless set |
| `FLEZHA_NODE` or `FLEZHA_NODE_QUERY` | yes | The controller, as above |
| `FLEZHA_TOKEN` or `FLEZHA_TOKEN_FILE` | yes | The console's token, or a file holding it |
| `FLEZHA_CONSOLE_ADMIN_EMAIL`, `FLEZHA_CONSOLE_ADMIN_PASSWORD` | yes | The first account. Each start makes it exist and sign in with this password |
| `RELEASE_DISTRIBUTION`, `RELEASE_NODE`, `RELEASE_COOKIE`, `ERL_FLAGS` | yes | The node, as above |

## Accounts

There is no sign-up page. The account in `FLEZHA_CONSOLE_ADMIN_EMAIL` exists from the first
start, and its password follows `FLEZHA_CONSOLE_ADMIN_PASSWORD`. Make further accounts inside the
running console, where its release is:

```sh
$ bin/flezha_console rpc 'FlezhaConsole.Release.ensure_admin("ops@example.com", "a long passphrase")'
```

The same call sets a new password for an account that exists. A password is 12 to 72
characters.

## What it leaves to flz

Issuing and revoking tokens, binding roles, deleting a namespace, claiming the bootstrap and
stopping the controller are not offered by the console's pages, whatever its token; `flz`
does them. This is what the console offers, not a limit on what its node could do: holding
the cookie, it is inside the cluster's trust boundary.
