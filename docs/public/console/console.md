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

- **A controller to reach.** The console calls the controller's published API over the
  controller's [HTTP API](../plugins/http_api.md), `POST /v1/call/<Name>` for a call and
  `POST /v1/stream/<Name>` for a live stream. It joins no cluster and starts no Erlang
  distribution toward the controllers.
- **A client certificate.** Off loopback the HTTP API serves only TLS and asks every client
  for a certificate from the authority its `tls_client_cacertfile` names. The console holds
  one, and checks the controllers' server certificates against their authority in turn.
- **A token.** The console acts as the principal its token names. An `admin` token in the
  `_system` namespace lets it list every namespace; a narrower one shows less. A signed-in
  user can also store a token of their own under **Settings**, and then the audit log names
  that user. The HTTP API refuses a request without a token, even on a controller whose
  authorization is open.
- **A Postgres database.** The console keeps its own accounts in it: who can sign in, their
  sessions and the tokens they stored. Everything it shows about relays is the controller's
  answer, read when the screen asks.

The console never holds the controllers' cookie, so it sits outside the cluster's trust
boundary: whoever controls its machine can do what its certificate and its tokens allow,
and nothing more.

## How it reaches the controller

Enable the `http_api` plugin in `_system` on the controllers, listening off loopback over TLS
with `tls_client_cacertfile` ([Enable it](../plugins/http_api.md#enable-it)). Every controller
holding `_system` serves it on the same port, `8765` unless set. The channel takes an
authority of its own, made with `deploy/dist-tls/make-certs.sh` in a folder apart from any
distribution authority, so nothing it signs passes a distribution handshake. It signs each
controller's server certificate, naming the host in the console's URL, and the console's
client certificate, and it is the authority `tls_client_cacertfile` names.

| Variable | What it is |
|---|---|
| `FLEZHA_CONTROLLER_URL` | The controllers' HTTP API, `https://host:port`. `host` may be one DNS name for every controller: the console pins an address that answers and moves to another when it stops |
| `FLEZHA_CONTROLLER_CA` | The authority the controllers' server certificates chain to |
| `FLEZHA_CLIENT_CERT`, `FLEZHA_CLIENT_KEY` | The console's client certificate and its key |

Plain `http://` is taken only for `127.0.0.1` or `[::1]`, a controller on the console's own
machine. A token works only on the controllers that issued it, so a URL must not span two
clusters. Without the URL, or with an `https` URL and a file missing, the console refuses to
start and names the variable.

The console needs only the HTTP API port of a controller. EPMD (port 4369) and the
distribution port stay closed to it. A console that still connects over distribution is
refused with [`control.surface_retired`](../errors.md#control) on every call: update it to a
release that reaches the controller over the HTTP API.

## Settings

| Variable | Required | What it is |
|---|---|---|
| `DATABASE_URL` | yes | The console's database, `ecto://user:password@host/database` |
| `SECRET_KEY_BASE` | yes | Signs and encrypts sessions; 64 random bytes, `openssl rand -base64 48` |
| `PHX_HOST` | yes | The host name browsers use to reach the console |
| `PHX_URL_SCHEME`, `PHX_URL_PORT` | no | The scheme and port browsers use, `https` and `443` unless set. LiveView compares a page's origin with them, so a console served over plain `http` on another port must say so |
| `PORT` | no | The port the console listens on, `4000` unless set |
| `FLEZHA_CONTROLLER_URL` | yes | The controllers' HTTP API, as above |
| `FLEZHA_CONTROLLER_CA`, `FLEZHA_CLIENT_CERT`, `FLEZHA_CLIENT_KEY` | with `https` | The channel's certificates, as above |
| `FLEZHA_TOKEN` or `FLEZHA_TOKEN_FILE` | yes | The console's token, or a file holding it |
| `FLEZHA_CONSOLE_ADMIN_EMAIL`, `FLEZHA_CONSOLE_ADMIN_PASSWORD` | yes | The first account. Each start makes it exist and sign in with this password |

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
stopping the controller are not offered by the console, whatever its token; `flz` does
them. The HTTP API does not reach them either
([What is never exposed](../plugins/http_api.md#what-is-never-exposed)), so a console's
certificate and token cannot make them.
