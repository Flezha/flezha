# Run the echo

On this page you start an operator from the `echo` factory your runner
registered, send it a message and watch what comes out. A factory is the
template, the code you wrote; an operator is one running instance of it, with
a name and state of its own.
[Running operators](../../topics/basics/running_operators.md) covers starting,
watching and stopping operators in full, from `flz` and from your own code.

> From a third terminal, start an operator from the `echo` factory and name it
> `my_echo`. The controller and your runner have to be running.
> {: .step}

```sh
$ flz operator start echo --id my_echo
```

```output
Started my_echo
```

`echo` is the factory id your code declares; `my_echo` is the operator id you
chose, and every later command finds the operator by it. List the operators to
see which runner it is bound to:

```sh
$ flz operator list
```

```output
Fetching operators for namespace 'default'...
Operators in namespace 'default':

  OPERATOR                          HEALTH      PARTS   NODE                  RUNNER
  my_echo                           healthy     -       flezha@127.0.0.1      default_23fc85126379
```

A runner name under `RUNNER` means the controller bound the operator to your
runner. `(none)` means no runner registered `echo`, which usually means the
factory id is spelled differently in the code and on the command line.

> Send `hello world` to `my_echo`, and print everything it sends out during
> the next two seconds.
> {: .step}

```sh
$ flz operator send my_echo --text 'hello world' --await 2000
```

```output
Delivered to 'my_echo'.
out	echo: hello world
```

The first line says the controller accepted the message. The second is the
output: the channel, `out`, a tab, and what the tasklet emitted.

To watch an operator's output from another terminal for as long as you like,
listen to it:

```sh
$ flz operator listen my_echo
```

It prints each message the operator sends out, for 30 seconds by default.
`--input` shows what goes in instead.

`echo` runs in flow, so it runs until you stop it. Leave it running: the next
page attaches a file to each side of it.

## Next

[Feed the echo a file](feed_the_echo_a_file.md) replaces `flz operator send`
with a real input and a real output.
