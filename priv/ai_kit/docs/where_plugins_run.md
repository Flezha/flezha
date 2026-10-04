# Where a plugin runs

An operator plugin runs at the edge of the operators bound to it. This page covers
how many copies of it run and where, when a copy stops, and how a plugin follows its
operator's durability. [Plugins](plugins.md) has the model: a plugin is installed,
enabled by name and bound to operators.

**Contents**

1. [Shared or one per operator](#shared-or-one-per-operator)
2. [A shared feed](#a-shared-feed)
3. [Partitioned operators](#partitioned-operators)
4. [When a copy stops](#when-a-copy-stops)
5. [A controller without the provider](#a-controller-without-the-provider)
6. [Plugins follow the operator's durability](#plugins-follow-the-operator-s-durability)

## Shared or one per operator

A plugin's provider declares whether one running copy can serve several operators:

- **Shareable** (a websocket, an HTTP poll, an HTTP push, the Kafka and RabbitMQ
  producers): each controller runs one copy for every unpartitioned operator bound to the
  plugin there, so a source opens one upstream connection and a sink runs one worker,
  however many operators share it.
- **Not shareable** (a file reader with one offset, a server on one port, a consumer
  holding one group member): the plugin serves one operator. Binding a second operator is
  refused with `plugin.not_shareable`, naming the operator that holds it. Enable another
  plugin of the same provider for the second operator.

`--single` at enable runs one copy in the whole cluster instead, on a controller the
cluster chooses, delivering to every bound operator on any controller. It applies to both
types. A `single` plugin takes no `on_output` hook (`plugin.single_sink`): one copy cannot
be every operator's sink. When its controller leaves gracefully, the copy moves with a
hand-over that opens the new connection before the old one closes; when its controller
fails, it starts on another and its operators are served again without being bound again.

## A shared feed

A shared source hands every record to every operator bound to it, straight from the
worker that reads it, without waiting on any operator. A slow operator falls behind in
its own input queue and loses nothing, and the others are not slowed. A newly bound
operator is told to the worker when it binds, so it gets what arrives from then on (a
shared HTTP poll offers it the current content at the next poll).

## Partitioned operators

A plugin bound to a partitioned operator runs one copy on every member of the group,
shareable or not and `single` or not, and a member's copy is never shared with another
operator. Unbinding the group stops the members' copies and leaves any shared copy
running for its other operators. A websocket source on a group subscribes each member to
its own share of the items, and an HTTP poll can name the member's partition in its URL.

A source that cannot tell which member a record's key belongs to refuses a group member
with `plugin.partitioned_host`: the Kafka consumer, the RabbitMQ consumer, Postgres change
capture, the Postgres poll source and the HTTP server. Bind it to an operator that is not
partitioned and route from there. [`partitioning.md`](../advanced/partitioning.md) covers
groups.

## When a copy stops

A running copy whose last bound operator on that controller leaves stops:

- at once when the leave was deliberate: an unbind, an operator stop, an operator that
  ended;
- after `FLEZHA_PLUGIN_IDLE_STOP_MS` (default `60000`) when the operator went down or
  moved, unless a bound operator arrives first. A crash followed by a restart within that
  time keeps the connection open throughout.

A stopped plugin stays enabled and starts again when a bound operator runs. On an
operator's move the plugin starts on the new controller when the operator brings its
plugins up there, and the copy on the old controller stops only once the operator has
left it and the idle stop has passed, so the new connection opens before the old one
closes.

Only `flz plugin disable <name>` turns a plugin off. It stops every running copy on every
controller and leaves every binding on its operator. Each bound operator keeps running and
reports `plugin_unhealthy` with `plugin.not_found` for that binding, and enabling a plugin
of that name again brings the bindings up.

## A controller without the provider

Each controller has its own installed packages. When another controller that holds the
namespace lacks the provider, `flz plugin enable` warns, naming it: the plugin will not run
there. An operator bound to the plugin and placed on such a controller still runs and
processes its inputs, without the plugin:

- its health reports `plugin_unhealthy` with `plugin.provider_not_loaded`, naming the
  controller, and one warning is logged naming the plugin and why;
- a delivery-guaranteed output to that sink waits in the outbox and is delivered once the
  binding starts; a plain output is dropped and counted;
- the binding is tried again at the operator's next bring-up, so it comes up on a
  controller that has the package.

A controller plugin runs no copy on a controller that lacks its provider.

## Plugins follow the operator's durability

The controller tells each plugin how the operator it serves keeps its input, over anything
the plugin's stored options say. A source moves its upstream position (an offset, a
cursor, an acknowledgement) only past records the operator took, and how often follows
the operator:

| The operator | A verdict `ok` means | The source moves its position |
| --- | --- | --- |
| a flow operator without `effectively_once` that does not sleep | the operator took the record | every few seconds |
| a durable steps operator, or a flow operator that sleeps | the record is in the operator's input log | after every confirmed batch |
| a flow operator with `effectively_once` | the record is in the operator's input log | after every confirmed batch; a sink that honours `effectively_once` writes each output once |

An operator whose input is durable (effectively-once, sleeping or durable steps) does not
share a shareable source: it runs the source on its own, so every record is confirmed to
it, and a slow one holds back only its own upstream connection.

On the sink side, a delivery-guaranteed output is held in the operator's outbox until the
sink confirms it. What each sink honours (written once, at least once, or best effort) is
the Durability row of its catalogue page. [`durability.md`](../basics/durability.md) has the levels,
and [`effectively_once.md`](../advanced/effectively_once.md) the tier.
