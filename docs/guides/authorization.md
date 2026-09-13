# Authorization

Flezha enforces authorization by default. The Part 1 guides turn it off, so if
you followed them your controller is running with `FLEZHA_AUTHZ_ENABLED=false`
and every caller is trusted. This guide turns it back on.

**What "off" actually means.** The posture is `open`: no verb needs a token, no
runner is asked to prove the namespace it connects to, and the boot logs a
warning every time. Nothing else about the system changes, so turning it on
later is a restart, not a migration.

## Turn it on

Drop `FLEZHA_AUTHZ_ENABLED=false` and restart. Enforcement is the default, so
removing the variable is the whole change:

| Way | What to do |
|---|---|
| Docker | remove `-e FLEZHA_AUTHZ_ENABLED=false` from the `docker run` |
| Binaries | start without the variable in the environment |
| Elixir application | it is off by default as a library, so this one has to be asked for: `config :flezha, authn_authz: [enabled: true]` |

An enforced controller has no admin yet, so every gated verb refuses and names
the state it is in:

```
authz.namespace_create: refused: authorization is enforced but the bootstrap has not been claimed
```

It goes on to name `flz auth bootstrap` and the file holding the token.

Data keeps flowing while that is true. It is the operator surface that is shut,
not the controller.

## Claim the admin

The first enforced boot writes a one-time token to
`app_data/auth/bootstrap.token`, under the working directory the controller was
started from, or `/flezha/app_data/auth/bootstrap.token` in the container. Read
the file rather than the log: the path is logged only on the boot that creates
it.

```sh
$ cat app_data/auth/bootstrap.token
g5dy7mfj3z3ccnmgdzjzn74wuw6iqhqthz3gvht4clobnuaghwwa

$ flz auth bootstrap g5dy7mfj3z3ccnmgdzjzn74wuw6iqhqthz3gvht4clobnuaghwwa
bootstrap claimed; admin@_system bound to admin-ad133413363b2d3c
admin token (shown once, store it now): flzt_kpkt3cgsped2eqyei25nowypv6nu4d45o4ig37ikvyomwbbszwga
```

The claim unlinks the file, and "shown once" means it: nothing can print that
token again.

## Use it

A gated verb takes the token from `--token` or from `FLEZHA_TOKEN`:

```sh
$ export FLEZHA_TOKEN=flzt_kpkt3cgsped2eqyei25nowypv6nu4d45o4ig37ikvyomwbbszwga
$ flz auth whoami
principal: admin-ad133413363b2d3c
namespace: _system
role:      admin
```

That variable belongs in the shell that runs `flz`, not in the controller's
environment. The container is the one exception: its entrypoint runs
`flz controller stop` on SIGTERM, so a container with no `FLEZHA_TOKEN` is
forced down instead of drained.

## The first token lasts a day

The minted admin token carries the default lifetime of one day. Issue a longer
one before it lapses:

```sh
$ flz token issue admin _system admin --ttl 2592000
token issued for admin (admin@_system)
id:    60384fcaea2a
token (shown once, store it now): flzt_qngtidkyk7ljhvz7ohg4hjnhviqfkhepyvohqxkbvfomkrhb4mea
```

The positionals are `<principal> <namespace> <role>`, where the role is
`viewer`, `operator`, `admin` or `runner`.

## If you lose the admin

No token file to read means the bootstrap was already claimed. Mint a fresh one
without stopping the controller:

```sh
$ flz auth rebootstrap
Bootstrap token written to ./app_data/auth/bootstrap.token.
Claim it against the RUNNING controller:
    flz auth bootstrap 5ce1782ca518d067b7bbfa9dd81dd26a6c8df1af9f71425c4d5415d9a3da6a85
```

That claim unbinds every `admin@_system` and revokes its `_system` admin
tokens. Operatives, runner connections and runner credentials carry on.

Two refusals tell the two states apart. One naming `flz auth bootstrap` and a
file path means not claimed, and the token is in that file. `no verifiable
identity` means claimed, and you need a token.

## Turning it off again

`FLEZHA_AUTHZ_ENABLED=false` puts it back to the Part 1 posture, which is worth
knowing for a local experiment. It masks enforcement rather than clearing
anything, so a claimed bootstrap and its tokens survive and come back when the
variable goes. The boot says so on every start:

```
[warning] [authz] authn/authz is OFF; every caller is trusted.
```

## See also

[`../operations/authn_authz.md`](../operations/authn_authz.md) is the reference
behind this guide: postures, certificates, role bindings, the verb table and
recovery.

