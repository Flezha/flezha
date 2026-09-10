# Your first deployment: controller, runner, operative

From a checked out repository to an operative processing data on a Java runner,
with authorization enforced. The deployed counterpart of
[`../quickstart.md`](../quickstart.md), which runs a graph in your own process
and needs none of this.

The namespace here is `java-test` and the operative is a Java stream operative
called `accumulate`. Substitute your own.

## 1. Build the release

```sh
make build-release
export PATH="$PWD/_build/prod/rel/flezha/bin:$PATH"
```

That builds the Rust NIF, the `flz` CLI and the Mix release into
`_build/prod/rel/flezha/bin`: `flz` is the operator CLI, `flzvm` the release
script it shells out to for every RPC.

## 2. Start the controller

```sh
flz controller start
flz boot await          # blocks until the boot completes, 60s default
```

`flz boot`, not `flz controller status`: the boot verbs are public, so a
liveness check needs no credential. You have none until step 3.

Two things about the environment it starts in:

- **The state root is relative to the working directory.** Start the controller
  and run `flz` from the same directory, or set `FLEZHA_STATE_DIR` to an
  absolute path on both sides. Two directories give two state roots and an
  authorization catalog the CLI cannot see.
- **The distribution cookie is minted, not shipped.** With no `RELEASE_COOKIE`,
  the first boot writes 32 random bytes to
  `<release root>/app_data/state/cookie` and reads them back afterwards. Fine
  for one node; a cluster needs one provisioned value on every process.

## 3. Claim the bootstrap admin

The production release enforces authorization by default. Until the bootstrap is
claimed, operator verbs refuse and say so, while data keeps flowing.

The first enforced boot writes a one-time token to
`app_data/auth/bootstrap.token`. It logs the path only on the boot that creates
the file, so read the file rather than the log:

```sh
cat app_data/auth/bootstrap.token
```

**No such file** means the bootstrap was already claimed on this controller. The
claim unlinks it, so there is nothing to read and nothing is wrong: mint a fresh
one instead, without stopping the controller.

```sh
flz auth rebootstrap      # only if the file was missing
```

Either way you now have a token to claim:

```sh
flz auth bootstrap <token>
# -> admin token (shown once, store it now): flzt_...

export FLEZHA_TOKEN=flzt_...
flz auth whoami
```

A `rebootstrap` claim invalidates `admin@_system` and its `_system` admin tokens
and nothing else. Operatives, runner connections and runner credentials carry
on.

`FLEZHA_TOKEN` belongs in the shell that runs `flz`, never in the controller's
environment. The minted admin token has the default TTL of one day; issue a
longer lived one before it lapses:

```sh
flz token issue admin _system admin --ttl 2592000
```

Two refusals distinguish the states: `no verifiable identity` means claimed, get
a token. One naming `flz auth bootstrap` and a file path means not claimed, and
the token is in that file.

## 4. Create the namespace

A runner cannot connect to a namespace the catalog does not hold.

```sh
flz namespace create java-test
flz namespace show java-test
```

Names match `[a-z][a-z0-9-]{0,62}`. **Underscores are not allowed**: `java_test`
is refused, `java-test` is not. `_system` is reserved.

## 5. Issue the runner credential

**The step most first deployments miss.** With authorization enforced a runner
proves its namespace rather than naming it. Over UDS there is no certificate to
read, so the proof is a credential in the connect frame.

```sh
flz token issue runner@java-test java-test runner --ttl never
# -> token (shown once, store it now): flzt_...
```

The positionals are `<principal> <namespace> <role>`. A `runner` entry is
refused any TTL but `never`, is refused `_system`, and closes the sessions it
authenticated when revoked. A namespace may hold as many as you issue, so
rotation is rolling: issue, move the fleet over, revoke the old id.

## 6. Start the runner

The credential reaches every SDK through `FLEZHA_NAMESPACE_SECRET`. The
controller never reads it.

```sh
export FLEZHA_NAMESPACE_SECRET=flzt_...
mvn -q compile
mvn -q exec:java -Dexec.mainClass=com.flezha.Main
```

From an IDE, set it in the run configuration, not only in your shell.

The Java side names the namespace and nothing else:

```java
Flezha.createRunner("java-test")
        .defineStream(AccumulateStreamOperative.class)
        .run();
```

That word and the namespace the credential entitles must match, or the connect
is refused naming both.

```sh
flz runner list -n java-test
```

If it does not appear, `flz runner check` walks the socket ladder a runner walks
and says why one cannot reach this host's controller. It needs no running runner
and no controller.

## 7. Start an operative

```sh
flz operative start accumulate accumulator_stream -n java-test
flz operative list -n java-test
```

The positionals are the operative id you choose and the factory id the SDK
registered. `list` and `info` are namespace wide and take no operative id. `-n`
is `--namespace`.

## 8. Start the testnet

A local websocket server that stands in for both a data source and a sink, so
the plugins in the next step have something to connect to.

```sh
cd test/apps/testnet
npm ci && npm run build
FLEZHA_TESTNET_WS_PORT=4501 npm start
```

One port, two directions separated by path: a line you type is broadcast to
every socket on `/in`, and a socket on `/out` is excluded from that and prints
what it receives. That is what lets one process be both ends of a run.

Its own [`README`](../../test/apps/testnet/README.md) covers the manual input
commands, the generators and the subscription dialects.

## 9. Attach plugins

Both ends of the testnet websocket:

```sh
flz plugin enable acc-in operative_websocket \
  --namespace java-test \
  --operative_id accumulate \
  --Xuri=ws://localhost:4501/in \
  --Xplugin_id=acc-in \
  --trigger interval

flz plugin enable acc-out operative_websocket \
  --namespace java-test \
  --operative_id accumulate \
  --Xuri=ws://localhost:4501/out \
  --Xplugin_id=acc-out \
  --extension=on_output
```

`--trigger` and `--extension` are what keep one plugin from being both sides of
the same pipe: a source is polled on a trigger, an output is declared
`--extension=on_output` and receives what the operative emits.
[`../plugins/installation.md`](../plugins/installation.md) has the rest.

## Connect refusals

Logged by `Flezha.Transport.Server` as
`Rejected runner connect (<reason>); closing stream`, counted on
`[:flezha, :transport, :connect_rejected]`.

| Reason | Fix |
| --- | --- |
| `:namespace_not_proven` | No credential in the connect frame, or not a live `runner` entry. Steps 5 and 6 |
| `{:namespace_not_entitled, claimed, proven}` | Make the word in `createRunner` match the credential's namespace |
| `:namespace_unknown` | `flz namespace create <name>`. Step 4 |
| `:system_namespace_refused` | No runner may be `_system`, on any transport, at any posture |

`Peer refused at the handshake` is a QUIC certificate problem instead, not an
authorization one.

## Turning authorization off

For local experiments only:

```sh
FLEZHA_AUTHZ_ENABLED=false flz controller start
```

The posture becomes `open`: every call is authorized, no runner is asked for a
credential, and a warning is logged on every boot. It masks the durable
bootstrap flag rather than clearing it, so removing the variable restores
enforcement.

## See also

- [`../operations/authn_authz.md`](../operations/authn_authz.md) - postures,
  certificates, role bindings, the verb table, recovery
- [`../configuration.md`](../configuration.md) - every knob, by layer
- [`../sdk/java_sdk.md`](../sdk/java_sdk.md) - the Java authoring surface
- [`../operations/deployment/`](../operations/deployment/) - bare metal, Docker,
  Kubernetes
