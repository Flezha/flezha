# The console on Docker

One Compose file runs a controller, the console and the console's database on one machine.
The console is a hidden node beside the controller: [The console](console.md) explains the
link and every setting used here.

> #### What you need {: .tip}
>
> - Docker with Compose.
> - The console's image. Until it is published, build it from the console's repository, with
>   the stylebook checkout beside it, as below.

## Build the image

> Clone the console and the stylebook side by side, and build the image from the console's
> folder.
> {: .step}

```sh
$ git clone https://github.com/flezha/flezha_console.git
$ git clone https://github.com/flezha/flezha_stylebook.git
$ cd flezha_console
$ docker build -f deploy/Dockerfile --build-context stylebook=../flezha_stylebook -t flezha-console:0.1.0 .
```

The image holds the console as a release. On every start it runs its migrations, makes the
account in `FLEZHA_CONSOLE_ADMIN_EMAIL` exist with the password in `FLEZHA_CONSOLE_ADMIN_PASSWORD`,
and serves on port 4000.

## The Compose file

> Save this as `compose.yaml` in a folder of its own.
> {: .step}

```yaml
# A controller, the console and the console's database. Secrets come from .env.
services:
  controller:
    image: flezha/flezha:latest
    environment:
      FLEZHA_NODE_HOST: controller.flezha.internal   # the controller is flezha@controller.flezha.internal
      RELEASE_COOKIE: ${RELEASE_COOKIE:?set RELEASE_COOKIE in .env}
      FLEZHA_DURABILITY_BACKEND: none
    networks:
      default:
        aliases: [controller.flezha.internal]        # a long node name needs a host with a dot
    stop_grace_period: 75s
    volumes:
      - controller_data:/flezha/storage/app

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
    image: flezha-console:0.1.0
    depends_on:
      db: { condition: service_healthy }
      controller: { condition: service_healthy }
    networks:
      default:
        aliases: [console.flezha.internal]
    ports:
      - "127.0.0.1:4000:4000"                         # on this machine only
    environment:
      DATABASE_URL: ecto://console:${DB_PASSWORD}@db/console
      SECRET_KEY_BASE: ${SECRET_KEY_BASE:?set SECRET_KEY_BASE in .env}
      PHX_HOST: localhost
      PHX_URL_SCHEME: http
      PHX_URL_PORT: "4000"
      RELEASE_DISTRIBUTION: name
      RELEASE_NODE: flezha_console@console.flezha.internal
      RELEASE_COOKIE: ${RELEASE_COOKIE}
      ERL_FLAGS: -hidden
      FLEZHA_NODE: flezha@controller.flezha.internal
      FLEZHA_TOKEN: ${CONSOLE_TOKEN:-}                # issued once the controller is up
      FLEZHA_CONSOLE_ADMIN_EMAIL: ${ADMIN_EMAIL:?set ADMIN_EMAIL in .env}
      FLEZHA_CONSOLE_ADMIN_PASSWORD: ${ADMIN_PASSWORD:?set ADMIN_PASSWORD in .env}

volumes:
  controller_data:
  db_data:
```

- **The aliases.** A controller with a long node name needs a host name with a dot in it,
  which a Compose service name lacks. Each service gets an alias with one, and the node names
  use it.
- **`ERL_FLAGS: -hidden`** connects the console to the controller without joining its
  mesh, so the controller never places work on it. It still holds the cookie, which puts it
  inside the cluster's trust boundary ([The console](console.md#what-it-needs)).
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
RELEASE_COOKIE=$(openssl rand -hex 32)
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
[info] [NodeConnector] Will connect to flezha@controller.flezha.internal
[info] [NodeConnector] Connected to flezha@controller.flezha.internal
[info] Running FlezhaConsoleWeb.Endpoint with Bandit 1.10.3 at :::4000 (http)
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
