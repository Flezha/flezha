# Administering a namespace from application code

For application authors. A connected runner can administer the namespace its
connection proved: start, stop and feed operatives, enable and disable plugins,
and read what is running. No tasklet has to be running, and the application
holds no credential of its own beyond what its connection already presented.

The operator side is
[`../operations/authn_authz.md`](../operations/authn_authz.md#an-application-that-administers-its-own-namespace).

## Contract

| | |
|---|---|
| Reached from | the runner object, not a tasklet context |
| Namespace | the connection's, always. The request carries no namespace field |
| Identity | the `runner@<namespace>` principal the transport minted |
| Authority | the role that principal is bound to, read on every call |
| Default | `runner`, which grants the connect and nothing else: every call is refused until an operator binds the namespace |
| Expiry | none. A runner credential cannot expire, and no other credential is presented |
| Blocking | yes: the call answers the controller's own result |

```sh
flz role bind runner@team-orders app team-orders
```

That is the whole grant, once per namespace. `flz role unbind` revokes it, and
the revocation takes effect on the next call rather than the next reconnect.
A namespace bound to `spawner` instead, the role for tasklets that spawn
workflows, reaches `Operative.start` and `Operative.send` here and none of the
other verbs in the table below.

## The surface

| What | Verb on the wire | Arguments after the namespace |
|---|---|---|
| Start an operative | `Operative.start` | the spec `flz operative start` takes: `factory_id`, and optionally `id`, `kind`, `init_values`, `opts`; answers `{"id": ..., "outcome": "started" \| "already_running"}` |
| Stop an operative | `Operative.stop` | `operative_id` |
| Send to an operative | `Operative.send` | `operative_id`, `message`, `channel` (`""` for none) |
| Enable a plugin | `PluginManager.enable` | `plugin_id`, the fields `flz plugin enable` takes |
| Disable a plugin | `PluginManager.disable` | `plugin_id` |
| List operatives | `Operative.list_operatives` | none |
| List plugins | `PluginManager.list` | none |
| Read cell state | `Operative.cell_state` | `operative_id`, `cell_key` |

Each SDK spells these as typed methods on its own admin handle:

| SDK | Reached as | A start reads |
|---|---|---|
| Rust | `runner.admin()` | `admin.start_operative(&OperativeSpec::new("order_worker").id("orders"))` |
| Java | `runner.admin()` | `admin.startOperative(OperativeSpec.of("order_worker").id("orders"))` |
| Node | `runner.admin()` | `await admin.startOperative('order_worker', { id: 'orders' })` |
| Python | `runner.admin()` | `admin.start_operative("order_worker", id="orders")` |
| Ruby | `runner.admin` | `admin.start_operative("order_worker", id: "orders")` |
| PHP | `$runner->admin()` | `$admin->startOperative('order_worker', id: 'orders')` |
| Elixir | `Flezha.SDK.Admin` | `Admin.start_operative(runner, "order_worker", id: "orders")` |

The id is optional in every SDK: leave it out and the controller generates one from the factory
id, such as `order_worker-k7m2qzxw4pd3fn2a`. Every start answers the same two fields, `id` (the
id the operative runs under, given or generated) and `outcome` (`started`, or `already_running`
when a live instance already holds an id you gave), and nothing else; where it runs is
`flz operative info`'s to say.

A start without an id that is retried after its answer was lost starts a second operative, and
a second stream runs until it is stopped. Pass an id when a start may be retried.

Every handle also carries a generic `call(verb, args)` for a published function
the typed surface has not grown a method for. What succeeds follows from the
role's grants, never from a list any SDK keeps: an added grant reaches every
SDK at once, and an added verb costs one method in each language and no native
work at all.

## What it cannot do

`app` is a strict subset of `operator` for everything but the connect. It
destroys no record and moves no work it did not create, so a dead-letter drop
or replay, a state rotation or deletion, a rewind, a replay and a rescale are
all out of reach. So is everything node-wide or auth-state: installing,
upgrading or uninstalling a plugin, stopping the controller, retuning state
retention and the whole `authz` family. Those verbs are `scope: :system`, so a
namespace-bound caller is refused twice over, by grant and by scope.

A namespace other than the connection's is not refused, it is unexpressible:
the request has no field to name one, and the controller fills the namespace
slot each verb declares from the connection itself.

## The act is an operator's act

An SDK-issued act is judged at the same policy point, against the same declared
row, in the same scope, and writes the same audit record its operator-issued
equivalent writes, naming `runner@<namespace>` as the principal and `sdk` as
the surface. A read writes no record, because its row declares none. Quotas are
enforced unchanged: a start that would exceed `max_operatives` is refused with
the reason an operator would receive.

An operative started this way is the cluster's, not the connection's. It
survives the requesting runner's disconnection under the same rebinding rules
as one an operator started, and an operator sees it in `flz operative list` and
can stop it there.

## The wire

One frame for every verb, because the verb is data:

```proto
message AdminRequest {
    string verb = 1;   // "Operative.start"
    string args = 2;   // a JSON array of the call's remaining arguments
}

message AdminResponse {
    string result = 1; // the published function's own answer, as JSON
    string error = 2;  // a refusal; empty otherwise
}
```

Correlated by the `reference_id` the `CommandRequest` envelope already carries,
and riding the `control_command` payload id like every other SDK-initiated
variant. The hosts that share `flz_core` reach it through one verb-keyed C ABI
entry, `flz_core_admin_call`; the Elixir SDK re-expresses the same frame over
its own core.

## See also

- [`../operations/authn_authz.md`](../operations/authn_authz.md) — roles, the grant, the audit
- [`protocol_spec.md`](protocol_spec.md) — the control protocol the frame rides
- [`tasklet_requested_spawn.md`](tasklet_requested_spawn.md) — the other SDK-initiated request, for a workflow instance
