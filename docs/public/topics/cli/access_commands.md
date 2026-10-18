# Access commands

The groups that decide who may do what: namespaces and their quotas, the first
admin, tokens, roles, and the audit trail that records each action.

Each section shows, for the verbs used most, a command with its answer.
Unless a section names another page, the answers
come from the echo of [Up and running](../../getting_started/up_and_running.md#run-the-echo).
How `flz` reaches the controller, its options and its exit codes are on the
[CLI](cli.md) page, and every verb in one list is the
[command reference](command_reference.md).

## `flz namespace`

Creates and deletes namespaces, the separate areas relays and runners work in, and sets their quotas. Explained in [`namespaces.md`](../core/namespaces.md).

`flz namespace help` prints the group's verbs, and the [command
reference](command_reference.md) lists each with its usage line.

```sh
$ flz namespace create trading
```

```output
namespace trading created
namespace: trading
quotas:
  max_cells_per_relay: 1024
  max_mailbox_bytes_total: 268435456
  max_message_rate_per_second: 100000
  max_relays: 100
  max_runners: 32
  max_state_bytes: 107374182400
```

```sh
$ flz namespace list
```

```output
Fetching namespaces...
Namespaces:

  NAMESPACE                          RUNNING
  _system                            no
  trading                            no
```

```sh
$ flz namespace delete ghost
```

```output
error: namespace 'ghost' does not exist (namespace.not_found)
hint: flz namespace list
```

A namespace that still has a relay or a runner attached is refused
`namespace.in_use` (exit 6), naming them; stop them first
([`namespaces.md`](../core/namespaces.md#delete-a-namespace)).

## `flz auth`

Claims the first admin token, and shows who a token belongs to. Explained in [`authorization.md`](../core/authorization.md#claim-the-admin), [a lost admin](../core/authorization.md#a-lost-admin).

`flz auth help` prints the group's verbs, and the [command
reference](command_reference.md) lists each with its usage line.

The first admin is claimed with the token the controller wrote at its first
start, from [`authorization.md`](../core/authorization.md#claim-the-admin):

```sh
$ flz auth bootstrap "$(cat app_data/auth/bootstrap.token)"
```

```output
bootstrap claimed; admin@_system bound to admin-8cc3f398fb1b4ae5
admin token (shown once, store it now): flzt_fwn2ctvoleymwb4blf55ybn7baw6qns44iballspqtxethhlqjhq
```

```sh
$ flz auth whoami
```

```output
principal: admin-8cc3f398fb1b4ae5
namespace: _system
role:      admin
```

## `flz token`

Issues, lists and revokes tokens. Explained in [`authorization.md`](../core/authorization.md#tokens), [runner credentials](../core/authorization.md#runner-credentials).

`flz token help` prints the group's verbs, and the [command
reference](command_reference.md) lists each with its usage line.

A runner's credential, from
[`authorization.md`](../core/authorization.md#runner-credentials):

```sh
$ flz token issue runner@trading trading runner --ttl never
```

```output
token issued for runner@trading (runner@trading)
id:    c5c749c28131
token (shown once, store it now): flzt_3j5bftycxn43ivwoxbd2peghazjgtnru5bxqgshvfqcft7lrdsra
```

## `flz role`

Gives roles to a principal and takes them away. Explained in [`authorization.md`](../core/authorization.md#roles).

`flz role help` prints the group's verbs, and the [command
reference](command_reference.md) lists each with its usage line.

From [`authorization.md`](../core/authorization.md#roles):

```sh
$ flz role bind alice ops team-orders
$ flz role bind runner@team-orders spawner team-orders
$ flz role bind runner@team-orders stopper team-orders
$ flz role list
```

```output
alice  ops@team-orders
runner@team-orders  spawner,stopper@team-orders
```

## `flz audit`

Reads the record of who did what. Explained in [`authorization.md`](../core/authorization.md#the-audit-trail).

`flz audit help` prints the group's verbs, and the [command
reference](command_reference.md) lists each with its usage line.

`read` filters one file by source, name, time and count, which
`flz audit read --help` lists.

The audit log is off until `FLEZHA_AUDIT_ENABLED=true`, and every verb says so:

```sh
$ flz audit stats
```

```output
error: the audit log is disabled; enable it with FLEZHA_AUDIT_ENABLED=true, or :audit_log, :enabled in the configuration (audit.disabled)
```
