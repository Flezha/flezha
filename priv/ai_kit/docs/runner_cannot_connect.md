# A runner cannot connect

`flz runner check` runs exactly the probe a runner runs. It needs no runner
code, no running runner and no controller. It prints the verdict, the resolved
directory and every candidate it checked, one per line, and exits zero only
when a controller answered.

```sh
$ flz runner check
controller reachable
  directory  /run/user/1000/flezha (from $XDG_RUNTIME_DIR/flezha)
  checked    /run/user/1000/flezha
             /run/flezha
             /srv/flezha/app_data/sockets
```

When none answered, a last `fix` line says what to do:

```sh
$ flz runner check
control socket absent
  directory  /run/user/1000/flezha (from $XDG_RUNTIME_DIR/flezha)
  checked    /run/user/1000/flezha
             /run/flezha
             /srv/flezha/app_data/sockets
  fix        start the controller, or wait for it to finish booting
```

A runner prints the same diagnosis once, on one line, before it starts retrying. The terse
line that repeats after that is the retry notice, not the diagnosis, so read the
first one.

## The verdicts

| Verdict | What it means | Fix |
|---|---|---|
| `socket directory absent` | no controller has ever used this path | start the controller, or set `FLEZHA_SOCKET_FILES_PATH` to the directory it established |
| `control socket absent` | the directory is there, the controller is not | start it, or wait for it to finish booting |
| `access denied` | the socket directory is the access boundary and this user is outside it. The message names the directory's owning uid and mode and the caller's uid | run the runner as the owning user, or start the controller with `FLEZHA_SOCKET_DIR_GROUP=<group>` naming a group both users belong to |
| `stale control socket` | a socket file nobody is listening on: a controller that died hard | clears on the next controller start |
| `more than one controller is listening` | two ladder candidates hold a live socket and none was named | set `FLEZHA_SOCKET_FILES_PATH` to the one this runner should attach to |

## Two things that are not verdicts

**The path is capped at 60 bytes.** A runner's data socket address has to fit
beside the directory, so a controller refuses to boot with a longer one, naming
the path and the limit. That is a controller-side refusal, not something
`flz runner check` reports.

**A connect can be refused after it succeeds.** Reaching the socket is not the
same as being admitted. With authorization enforced the controller also judges
the namespace the runner claims, and refuses with a reason of its own:
[`../topics/basics/authorization.md`](../topics/basics/authorization.md).

## See also

- [`../topics/deploying/connecting_a_runner.md`](../topics/deploying/connecting_a_runner.md) - reaching
  the controller in the first place, per deployment shape
