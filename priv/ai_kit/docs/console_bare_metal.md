# The console on bare metal

The console runs as a release on the machine, started by systemd, beside a controller
release on the same machine. [The console](console.md) explains the link and every setting
used here.

> #### What you need {: .tip}
>
> - A controller release running on this machine
>   ([Deploying a controller](deploying_a_controller.md)), version 0.1.2 or later.
> - Elixir 1.14 or later with Erlang/OTP, and Node.js with npm, to build the release.
> - A Postgres database and a role for the console.

## Build the release

> Clone the console and the stylebook side by side, link the stylebook into the console's
> assets, and build the release.
> {: .step}

```sh
$ git clone https://github.com/flezha/flezha_console.git
$ git clone https://github.com/flezha/flezha_stylebook.git
$ cd flezha_console
$ npm install --prefix assets
$ export MIX_ENV=prod
$ mix deps.get --only prod
$ mix assets.deploy
$ mix release
```

`npm install` links the stylebook checkout into `assets/node_modules`; the release is in
`_build/prod/rel/flezha_console`.

> Install it under a user of its own.
> {: .step}

```sh
$ sudo useradd --system --home /opt/flezha_console --shell /usr/sbin/nologin flezha_console
$ sudo cp -a _build/prod/rel/flezha_console /opt/flezha_console
$ sudo chown -R flezha_console: /opt/flezha_console
```

## The database

> Give the console a role and a database.
> {: .step}

```sh
$ sudo -u postgres psql -c "CREATE ROLE console LOGIN PASSWORD 'console-db-password'" \
  -c "CREATE DATABASE console OWNER console"
```

## The controller's cookie and the console's token

The console joins the controller's node under the controller's cookie. A controller started
without `RELEASE_COOKIE` minted one at its first start and keeps it in
`storage/app/state/cookie` in its release folder.

> Claim the admin, if no one has yet, and issue the console a token of its own, from the
> controller's release folder.
> {: .step}

```sh
$ ./bin/flz auth bootstrap < storage/app/auth/bootstrap.token
```

```output
bootstrap claimed; admin@_system bound to admin-187c0f387e574b17
admin token (shown once, store it now): flzt_…
```

```sh
$ FLEZHA_TOKEN=flzt_… ./bin/flz token issue console@_system _system admin --ttl never
```

```output
token issued for console@_system (admin@_system)
id:    3cfbf924a52f
token (shown once, store it now): flzt_…
```

## The settings

> Write the console's settings to `/etc/flezha_console/console.env`, readable by root only.
> {: .step}

```sh
PHX_SERVER=true
PORT=4000
PHX_HOST=localhost
PHX_URL_SCHEME=http
PHX_URL_PORT=4000
DATABASE_URL=ecto://console:console-db-password@127.0.0.1/console
SECRET_KEY_BASE=<openssl rand -base64 48>
RELEASE_DISTRIBUTION=name
RELEASE_NODE=flezha_console@127.0.0.1
RELEASE_COOKIE=<the controller's cookie>
ERL_FLAGS=-hidden
FLEZHA_NODE=flezha@127.0.0.1
FLEZHA_TOKEN=flzt_…
FLEZHA_CONSOLE_ADMIN_EMAIL=admin@example.com
FLEZHA_CONSOLE_ADMIN_PASSWORD=<a passphrase of 12 to 72 characters>
```

```sh
$ sudo chmod 600 /etc/flezha_console/console.env
```

- **`PHX_SERVER=true`** makes the release serve the web endpoint.
- **`FLEZHA_NODE=flezha@127.0.0.1`** is the controller's node: `flezha@` and its
  `FLEZHA_NODE_HOST`, which is `127.0.0.1` unless set.
- **`127.0.0.1`** keeps both nodes on this machine. Put the console behind a proxy with TLS
  to serve it to others, and set `PHX_HOST`, `PHX_URL_SCHEME` and `PHX_URL_PORT` to the
  address browsers use.

## The systemd unit

> Save the unit as `/etc/systemd/system/flezha-console.service`.
> {: .step}

```ini
[Unit]
Description=Flezha Console
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
User=flezha_console
EnvironmentFile=/etc/flezha_console/console.env
WorkingDirectory=/opt/flezha_console
ExecStartPre=/opt/flezha_console/bin/flezha_console eval FlezhaConsole.Release.migrate
ExecStartPre=/opt/flezha_console/bin/flezha_console eval FlezhaConsole.Release.ensure_admin
ExecStart=/opt/flezha_console/bin/flezha_console start
ExecStop=/opt/flezha_console/bin/flezha_console stop
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Before each start it runs the migrations and makes the account in `FLEZHA_CONSOLE_ADMIN_EMAIL`
exist with its password.

> Start it, and have it start at boot.
> {: .step}

```sh
$ sudo systemctl daemon-reload
$ sudo systemctl enable --now flezha-console
$ journalctl -u flezha-console | grep -E 'NodeConnector|Endpoint'
```

```output
[info] [NodeConnector] Will connect to flezha@127.0.0.1
[info] [NodeConnector] Connected to flezha@127.0.0.1
[info] Running FlezhaConsoleWeb.Endpoint with Bandit 1.10.3 at :::4000 (http)
```

Open http://localhost:4000 and sign in with `FLEZHA_CONSOLE_ADMIN_EMAIL` and
`FLEZHA_CONSOLE_ADMIN_PASSWORD`.

## Add an account

```sh
$ sudo sh -c 'set -a; . /etc/flezha_console/console.env; set +a; /opt/flezha_console/bin/flezha_console rpc '\''FlezhaConsole.Release.ensure_admin("ops@example.com", "a long passphrase")'\'''
```

## Update it

Build the new release as above, copy it over `/opt/flezha_console`, and restart the unit. The
migrations run before it starts again.

```sh
$ sudo systemctl restart flezha-console
```
