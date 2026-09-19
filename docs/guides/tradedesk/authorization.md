# Authorization

Authorization is what keeps namespaces, the rooms of the next page, apart. The
controller asks everyone who they are and lets them do only what their role
allows: an `flz` command shows a token, and a runner shows the key issued for
its namespace. It is on by default. Part 1 turned it off with `FLEZHA_AUTHZ_ENABLED=false` to keep the first run
simple, and from here on it stays on, as it would in any real deployment.

In more technical terms, a token names a principal, such as the admin or
`runner@trading`, and binds it to a role in one namespace. The role decides
which commands it may run, and [Roles](#roles) below lists them. Only the
admin, bound in `_system`, may act on every namespace.

## Claim the admin

With authorization on, a controller has no administrator until someone claims
one, and until then every command that changes something is refused.

The controller's first start writes a one-time token to
`app_data/auth/bootstrap.token`, under the directory the controller was started
from. Whoever claims it becomes the controller's first administrator, and the
file is deleted.

> Needs a controller started from the binaries, as in Part 1's
> [Start a controller](../controller_deployment.md#start-a-controller), but
> without `FLEZHA_AUTHZ_ENABLED=false`, and the rest of Part 2 uses the same
> one.\
> Run this from the directory it was started in. Claims the admin with the
> one-time token. The answer is your admin token, shown once, so keep it.
> {: .step}

```sh
$ flz auth bootstrap "$(cat app_data/auth/bootstrap.token)"
```

```output
bootstrap claimed; admin@_system bound to admin-8cc3f398fb1b4ae5
admin token (shown once, store it now): flzt_fwn2ctvoleymwb4blf55ybn7baw6qns44iballspqtxethhlqjhq
```

This token lasts a day.
[Tokens, a lost admin, and turning it off](#tokens-a-lost-admin-and-turning-it-off)
shows how to issue one that lasts longer.

> Puts the admin token where every `flz` command in this shell finds it. A new
> terminal needs the same line again.
> {: .step}

```sh
$ export FLEZHA_TOKEN=flzt_fwn2ctvoleymwb4blf55ybn7baw6qns44iballspqtxethhlqjhq
```

`flz auth whoami` shows who the token says you are. The admin lives in
`_system`, the controller's own namespace, which is what lets it act on every
other one:

```sh
$ flz auth whoami
```

```output
principal: admin-8cc3f398fb1b4ae5
namespace: _system
role:      admin
```

## Give the runner its key

A runner proves which namespace it may join with a key issued for that
namespace: a token with the `runner` role. It never expires, because a runner
runs unattended, and a key that lapsed would stop it at its next restart.

> Issues the key for `trading`, the namespace [`namespaces.md`](namespaces.md)
> creates next; a key can be issued before its namespace exists. `--ttl never` is
> required, since without it the token gets the default lifetime of one day and
> is refused. Keep the token: the runner in [`stream_basics.md`](stream_basics.md)
> reads it from `FLEZHA_NAMESPACE_SECRET`.
> {: .step}

```sh
$ flz token issue runner@trading trading runner --ttl never
```

```output
token issued for runner@trading (runner@trading)
id:    c5c749c28131
token (shown once, store it now): flzt_3j5bftycxn43ivwoxbd2peghazjgtnru5bxqgshvfqcft7lrdsra
```

A runner with no key, or with a key for another namespace, is refused. The
runner itself only reports that it keeps reconnecting, so the reason is in the
controller's log: `Rejected runner connect (:namespace_not_proven)`.

## Roles

Every token binds a principal, the name it was issued to, to roles in one
namespace. Only `admin@_system` reaches across namespaces.

| Role | What it may do |
|---|---|
| `viewer` | read: operatives, plugins, entity state, audit, the schedule |
| `app` | what an application does to its own work: start, stop and send to an operative, listen to it, enable and disable a plugin |
| `operator` | everything `app` may, plus the rest of the operative lifecycle: the dead-letter queue, rewind, replay, rescale, schedules |
| `admin` | everything `operator` may, plus tokens, roles, namespaces and quotas, and stopping the controller |
| `runner` | connect a runner, and nothing else |
| `spawner` | what a runner needs so that a procedure can start a workflow: start and send, in its own namespace |
| `stopper` | what a runner needs to stop its own operatives |

The key makes the runner `runner@trading`, which holds only `runner`.
[`workflow_basics.md`](workflow_basics.md) binds `spawner` to it as well, when
the stream operative starts its first workflow.
[`../../operations/authn_authz.md`](../../operations/authn_authz.md) has every
verb and the role that grants it.

## Tokens, a lost admin, and turning it off

**A longer admin token.** Issue one before the token from the claim lapses.
The positionals are `<principal> <namespace> <role>`, and `--ttl` is in
seconds:

```sh
$ flz token issue admin _system admin --ttl 2592000
```

```output
token issued for admin (admin@_system)
id:    4f8cfd4bef6b
token (shown once, store it now): flzt_xiqktybnu4ohm5vyfkl74boaooerehchsrnitki7swtfqxkapivq
```

**Two refusals tell you which state you are in.** One naming
`flz auth bootstrap` and a file means the admin was never claimed, and the token
is in that file. `no verifiable identity` means it was claimed, and this shell
has no token:

```output
authz.whoami: refused: no verifiable identity — supply `--token <token>` or set FLEZHA_TOKEN
```

**If you lose the admin token,** mint a new one-time token without stopping the
controller, then claim it as before:

```sh
$ flz auth rebootstrap
```

```output
Bootstrap token written to ./app_data/auth/bootstrap.token.
Claim it against the RUNNING controller:
    flz auth bootstrap 260cd28f8acf6b95e5ac9bec6e7ed73b69665780724253baafcd2ccc14422f11
The claim unbinds every admin@_system and revokes its _system admin tokens before binding the new admin, and it is audited as a recovery.
```

Every old admin token stops working. Runner keys are untouched.

**Stopping the controller is an admin command too,** so `flz controller stop`
needs the token like any other.

**Turning it off** is `FLEZHA_AUTHZ_ENABLED=false` again, which is handy for a
quick local experiment. It hides enforcement rather than clearing anything: the
admin, the tokens and the keys come back when the variable goes.

## Next

You are the admin and hold the runner's key.
[`namespaces.md`](namespaces.md) creates `trading`, the namespace the key is
for.
