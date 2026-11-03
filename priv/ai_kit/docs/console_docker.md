# The console on Docker

One Compose file runs a controller, the console and the console's database on one machine.
The console reaches the controller over its HTTP API with a client certificate and a token:
[The console](console.md) explains the channel and every setting used here.

> #### What you need {: .tip}
>
> - Docker with Compose, and `openssl`.
> - A checkout of the Flezha repository, for `deploy/dist-tls/make-certs.sh`.
> - The HTTP API's plugin package, `flezha_http-0.1.0.fpz`, built as
>   [Installing plugins](../topics/plugins/installing_plugins.md#example) shows.
> - The console's image, `flezha/flezha-console:0.1.0` on Docker Hub, for amd64 and arm64.

## The image

> Pull the console's image.
> {: .step}

```sh
$ docker pull flezha/flezha-console:0.1.0
```

The image holds the console as a release. On every start it runs its migrations, makes the
account in `FLEZHA_CONSOLE_ADMIN_EMAIL` exist with the password in `FLEZHA_CONSOLE_ADMIN_PASSWORD`,
and serves on port 4000.

To build the image yourself, clone the console and the stylebook side by side and build it
from the console's folder:

```sh
$ git clone https://github.com/flezha/flezha_console.git
$ git clone https://github.com/flezha/flezha_stylebook.git
$ cd flezha_console
$ docker build -f deploy/Dockerfile --build-context stylebook=../flezha_stylebook -t flezha/flezha-console:0.1.0 .
```

## The channel's certificates

> In a folder of its own, make the channel's authority, the controller's server certificate
> and the console's client certificate, and put the plugin package beside them.
> {: .step}

```sh
$ mkdir console-stack && cd console-stack
$ <flezha checkout>/deploy/dist-tls/make-certs.sh ca channel
$ <flezha checkout>/deploy/dist-tls/make-certs.sh node channel controller --dns controller
$ <flezha checkout>/deploy/dist-tls/make-certs.sh node channel console --dns console
$ mkdir plugins && cp <the plugin's folder>/_build/flezha_http-0.1.0.fpz plugins/
```

`channel/controller/` and `channel/console/` each hold `node.pem`, `node.key` and `ca.pem`.
The files are readable by their owner only, and both images run as uid 1000: where your user
is not uid 1000, `sudo chown -R 1000:1000 channel/controller channel/console`. Keep
`channel/ca.key` out of every container.

## The Compose file

> Save this as `compose.yaml` in the same folder.
> {: .step}

```yaml
# A controller, the console and the console's database. Secrets come from .env.
services:
  controller:
    image: flezha/flezha:latest
    environment:
      FLEZHA_DURABILITY_BACKEND: none
      FLEZHA_PLUGINS: /flezha/packages/flezha_http-0.1.0.fpz   # the HTTP API
    stop_grace_period: 75s
    volumes:
      - controller_data:/flezha/storage/app
      - ./plugins:/flezha/packages:ro
      - ./channel/controller:/etc/flezha/channel:ro

  db:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: console
      POSTGRES_PASSWORD: ${DB_PASSWORD:?set DB_PASSWORD in .env}
      POSTGRES_DB: console
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "console"]
      interval: 5s
    volumes:
      - db_data:/var/lib/postgresql/data

  console:
    image: flezha/flezha-console:0.1.0
    depends_on:
      db: { condition: service_healthy }
      controller: { condition: service_healthy }
    ports:
      - "127.0.0.1:4000:4000"                         # on this machine only
    environment:
      DATABASE_URL: ecto://console:${DB_PASSWORD}@db/console
      SECRET_KEY_BASE: ${SECRET_KEY_BASE:?set SECRET_KEY_BASE in .env}
      PHX_HOST: localhost
      PHX_URL_SCHEME: http
      PHX_URL_PORT: "4000"
      FLEZHA_CONTROLLER_URL: https://controller:8765
      FLEZHA_CONTROLLER_CA: /etc/flezha-console/channel/ca.pem
      FLEZHA_CLIENT_CERT: /etc/flezha-console/channel/node.pem
      FLEZHA_CLIENT_KEY: /etc/flezha-console/channel/node.key
      FLEZHA_TOKEN: ${CONSOLE_TOKEN:-}                # issued once the controller is up
      FLEZHA_CONSOLE_ADMIN_EMAIL: ${ADMIN_EMAIL:?set ADMIN_EMAIL in .env}
      FLEZHA_CONSOLE_ADMIN_PASSWORD: ${ADMIN_PASSWORD:?set ADMIN_PASSWORD in .env}
    volumes:
      - ./channel/console:/etc/flezha-console/channel:ro

volumes:
  controller_data:
  db_data:
```

- **`FLEZHA_PLUGINS`** names the HTTP API's package, which the controller's entrypoint
  installs before it starts.
- **`https://controller:8765`** is the HTTP API on the controller's Compose name, the name its
  server certificate carries. The console checks that certificate against `ca.pem` and shows
  its own `node.pem`.
- **No cookie.** The console holds no `RELEASE_COOKIE` and starts no Erlang distribution, so it
  stays outside the cluster's trust boundary ([The console](console.md#what-it-needs)).
- **`127.0.0.1:4000`** keeps the console on this machine. To serve it to others, put it
  behind a proxy with TLS and set `PHX_HOST`, `PHX_URL_SCHEME` and `PHX_URL_PORT` to the
  address browsers use.
- **`FLEZHA_DURABILITY_BACKEND: none`** keeps the controller's state on this machine only.
  [Durability](../topics/core/durability.md) has the backends that ship it elsewhere.

> Put the secrets in `.env` beside it. `CONSOLE_TOKEN` stays empty until the controller
> has issued one.
> {: .step}

```sh
$ cat > .env <<SECRETS
DB_PASSWORD=$(openssl rand -hex 16)
SECRET_KEY_BASE=$(openssl rand -base64 48)
ADMIN_EMAIL=admin@example.com
ADMIN_PASSWORD=$(openssl rand -base64 18)
CONSOLE_TOKEN=
SECRETS
```

## Start the controller and issue the console's token

> Start the controller and the database.
> {: .step}

```sh
$ docker compose up -d controller db
```

The controller enforces authorization, so the first admin token is claimed with the
bootstrap token it wrote at its first start ([Claim the admin](../topics/core/authorization.md#claim-the-admin)).

> Claim the admin once the controller is healthy (`docker compose ps` says so).
> {: .step}

```sh
$ docker compose exec controller sh -c 'bin/flz auth bootstrap < storage/app/auth/bootstrap.token'
```

```output
bootstrap claimed; admin@_system bound to admin-e49e99cea8afab0b
admin token (shown once, store it now): flzt_…
```

> Enable the HTTP API on every address of the controller, over TLS, asking each client for a
> certificate from the channel's authority.
> {: .step}

```sh
$ docker compose exec -e FLEZHA_TOKEN=flzt_… controller bin/flz plugin enable api http_api --namespace _system \
  --Xbind=0.0.0.0 \
  --Xtls_certfile=/etc/flezha/channel/node.pem \
  --Xtls_keyfile=/etc/flezha/channel/node.key \
  --Xtls_client_cacertfile=/etc/flezha/channel/ca.pem
```

```output
Enabling plugin...
Plugin api enabled.
```

> Issue the console a token of its own, as an `admin` in `_system`, with the admin token.
> {: .step}

```sh
$ docker compose exec -e FLEZHA_TOKEN=flzt_… controller bin/flz token issue console@_system _system admin --ttl never
```

```output
token issued for console@_system (admin@_system)
id:    aa76c6a84ae2
token (shown once, store it now): flzt_…
```

Keep the admin token somewhere safe: the console never needs it.

## Start the console

> Put the console's token in `.env` as `CONSOLE_TOKEN`, then start the console.
> {: .step}

```sh
$ docker compose up -d console
$ docker compose logs console | grep -E 'NodeConnector|Endpoint'
```

```output
[info] Running FlezhaConsoleWeb.Endpoint with Bandit 1.10.3 at :::4000 (http)
[info] [NodeConnector] Reached the controller controller at https://172.18.0.3:8765
```

Open http://localhost:4000 and sign in with `ADMIN_EMAIL` and `ADMIN_PASSWORD`. The top
bar names the controller it reached, and the key icon the principal it acts as,
`console@_system`.

## Add an account

```sh
$ docker compose exec console bin/flezha_console rpc 'FlezhaConsole.Release.ensure_admin("ops@example.com", "a long passphrase")'
```

## Stop and clean up

```sh
$ docker compose down
```

`down -v` also removes the volumes: the controller's state and the console's accounts.
