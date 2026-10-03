# A controller will not start, or will not stop

## `the name flezha@127.0.0.1 seems to be in use by another Erlang node`

A controller is already running. A release node's name is
`<FLEZHA_NODE_NAME>@<FLEZHA_NODE_HOST>`, one process owns it, and the second
start fails after it has already minted its cookie, so the output looks like a
boot that got some way in:

```
[flezha] no RELEASE_COOKIE provisioned; using the node-local cookie in ...
Protocol 'inet_tcp': the name flezha@127.0.0.1 seems to be in use by another Erlang node
Unknown code returned: exit status: 1
```

Find what holds it:

```sh
pgrep -af 'beam\.smp.*-name flezha@'
```

That pattern matches the controller node and nothing else: a test-suite node is
named `flezha_test_<pid>`, so `mix test` is never caught by it.

Stop it properly first. The graceful verb needs to reach the node, and
`flzvm stop` needs only the cookie:

```sh
flz controller stop          # drains; needs authorization if it is enforced
_build/prod/rel/flezha/bin/flzvm stop
```

If neither returns, read the next section before reaching for a signal.

## Every command hangs or answers `:noconnection`

A controller can reach a state where its transport socket still answers while
its distribution is dead. `flz runner check` says the controller is reachable,
and every other verb hangs or reports:

```
--rpc-eval : RPC failed with reason :noconnection
```

The socket and the node are two different surfaces, and only one of them has
failed. Confirm it by looking at the accept queue on the distribution port:

```sh
pgrep -af 'beam\.smp.*-name flezha@'          # the pid
epmd -names                                    # the port it registered
ss -lnt 'sport = :<port>'
```

```
State   Recv-Q Send-Q Local Address:Port
LISTEN  27     128          0.0.0.0:46233
```

**`Recv-Q` on a listening socket counts completed connections nobody has
accepted.** A number above zero that grows each time you run a `flz` command is
a node whose acceptor is wedged: the kernel completes every handshake and the
Erlang side never takes one. Nothing you pass on the command line will reach it,
and the cookie is not the problem.

### Capture it, then end it

`SIGUSR1` makes the BEAM write a full crash dump and exit, which stops the
process **and** records what every one of its processes was doing, including
whatever should have been accepting:

```sh
kill -USR1 <pid>        # writes erl_crash.dump in the controller's working directory
```

If you do not want the dump, or the process ignores it:

```sh
pkill -9 -f 'beam\.smp.*-name flezha@'
```

A controller killed this way leaves its socket directory behind. Nothing needs
cleaning up: the next start clears it.

## Refusals at boot

These are deliberate refusals with a message, not crashes. Nothing was removed
in any of them.

| Refusal | What it means | Fix |
|---|---|---|
| `… is another controller's socket directory` | a live controller answered on the control socket in the directory this one was told to prepare | give this controller its own `FLEZHA_SOCKET_FILES_PATH`, or stop the controller listening there |
| `state root … is already claimed (held by pid N)` | another controller holds the advisory lock on that state root | give this controller its own `FLEZHA_STATE_DIR`, or stop pid N. If the holder is gone the lock is gone with it, so the message always names a live process |
| `no socket directory could be created` | no ladder candidate was writable | set `FLEZHA_SOCKET_FILES_PATH` to a directory this user can write |
| `the socket directory … is N bytes, which leaves no room for the data socket of a runner …` | the directory is over 60 bytes, so a runner's data socket address would not fit beside it | set `FLEZHA_SOCKET_FILES_PATH` to a directory of at most 60 bytes |
| `FLEZHA_SOCKET_DIR_GROUP=<group>: no such group` | the group does not exist, or this controller's user is not in it | use a group both users belong to, or drop the variable for owner-only |

## Running more than one on purpose

Two controllers on one machine is supported, and needs four things separated:
`FLEZHA_NODE_NAME`, `FLEZHA_SOCKET_FILES_PATH`, `FLEZHA_STATE_DIR` and
`FLEZHA_PROCESS_HUB_REGISTRY_ROOT`. `docs/topics/basics/deploying_a_controller.md`
has the full list.

## See also

- [`runner_cannot_connect.md`](runner_cannot_connect.md) - the runner side
- [`../topics/basics/deploying_a_controller.md`](../topics/basics/deploying_a_controller.md) - starting one
