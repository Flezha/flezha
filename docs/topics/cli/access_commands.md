# Access commands

The groups that decide who may do what: namespaces and their quotas, the first
admin, tokens, roles, and the audit trail that records each action.

Each section shows what the group's help prints and, for the verbs used most,
a command with its answer. Unless a section names another page, the answers
come from the echo of [Up and running](../../getting_started/up_and_running.md#run-the-echo).
How `flz` reaches the controller, its options and its exit codes are on the
[CLI](cli.md) page, and every verb in one list is the
[command reference](command_reference.md).

## `flz namespace`

Creates and deletes namespaces, the separate areas relays and runners work in, and sets their quotas. Explained in [`namespaces.md`](../core/namespaces.md).

```sh
$ flz namespace help
```

```output
flz namespace - Namespace management commands

USAGE:
    flz namespace <COMMAND>

COMMANDS:
    list                                 List the auth catalog, with a running-hub column
    create <name> [--X<quota>=<value>]   Create a namespace in the auth catalog
    delete <name>                        Delete a namespace from the auth catalog
    show <name>                          Show a namespace's quotas
    set-quota <name> --X<quota>=<value>  Update quota overrides
    help                                 Show this help message

Quota fields: max_relays, max_cells_per_relay, max_mailbox_bytes_total,
              max_message_rate_per_second, max_state_bytes, max_runners

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var.
```

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

## `flz auth`

Claims the first admin token, and shows who a token belongs to. Explained in [`authorization.md`](../core/authorization.md#claim-the-admin), [a lost admin](../core/authorization.md#a-lost-admin).

```sh
$ flz auth help
```

```output
flz auth - Authentication bootstrap and identity

USAGE:
    flz auth <COMMAND>

COMMANDS:
    bootstrap <token>   Claim the one-time bootstrap token as admin@_system
    whoami              Show the identity the current token resolves to
    rebootstrap         Break-glass: recover a lost admin@_system
                        (running: writes app_data/auth/bootstrap.token and prints it;
                         stopped: writes app_data/auth/REBOOTSTRAP for the next boot)
    help                Show this help message

Break-glass needs write access to app_data/auth/ and nothing else, which is
already enough to stop the controller: a claim against a running controller
therefore grants no authority filesystem access does not already carry. It
unbinds every admin@_system and revokes its _system admin tokens before binding
the new admin, and it is audited as a recovery.

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var.
```

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

```sh
$ flz token help
```

```output
flz token - Scoped-token management

USAGE:
    flz token <COMMAND>

COMMANDS:
    issue <principal> <namespace> <role>   Issue a token (printed once)
        [--ttl <seconds>|never]
    revoke <token-id>                      Revoke by id (prefix of the token hash)
    list                                   List tokens visible to the caller
    help                                   Show this help message

A `runner` token is the credential a runner connection proves its namespace
with. It never expires and `--ttl` is refused for it: the holder is an
unattended process, and a credential that lapses fails it at a restart. A
namespace may hold several at once, so a rotation is issue, roll the fleet,
revoke the old one; revoking one closes the sessions it authenticated.

Identity for these verbs rides `--token <token>` or the FLEZHA_TOKEN env var.
```

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

```sh
$ flz role help
```

```output
flz role - Principal → role bindings

USAGE:
    flz role <COMMAND>

COMMANDS:
    bind <principal> <role> <namespace>   Add a role to a principal in its namespace
    unbind <principal> [<role>]           Remove one role, or every role when none is named
    list                                  List bindings visible to the caller
    help                                  Show this help message

A principal holds a set of roles in one namespace, and an action is allowed
when any of them grants it. A bind naming another namespace is refused.

Identity for these verbs rides `--token <token>` or the FLEZHA_TOKEN env var.
```

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

```sh
$ flz audit help
```

```output
flz audit - Durable audit log inspection

USAGE:
    flz audit <COMMAND> [OPTIONS]

COMMANDS:
    list      List the on-disk catalog of rotated files
    read      Read records from a specific file with optional filters
    tail      Stream live audit records until a duration elapses
    stats     Show operational snapshot (file path, size, totals, drops)
    rotate    Force an immediate rotation of the active file
    help      Show this help message

OPTIONS:
    --json    Emit machine-readable output (NDJSON for tail/read).
              --output json is the same request.
```

`read` filters one file:

```sh
$ flz audit read --help
```

```output
flz audit read <FILE> [OPTIONS]

OPTIONS:
    --json              Emit NDJSON
    --source <S>        Match records whose source equals S
    --name <N>          Match records whose name equals N
    --since <TS_US>     Match records with ts_us >= TS_US (monotonic)
    --until <TS_US>     Match records with ts_us <= TS_US
    --limit <N>         Return at most N records (default 1000)
```

The audit log is off until `FLEZHA_AUDIT_ENABLED=true`, and every verb says so:

```sh
$ flz audit stats
```

```output
error: the audit log is disabled; enable it with FLEZHA_AUDIT_ENABLED=true, or :audit_log, :enabled in the configuration (audit.disabled)
```
