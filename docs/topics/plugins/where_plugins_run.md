# Where a plugin runs

A relay plugin runs at the edge of the relays bound to it. This page covers
how many copies of it run and where, when a copy stops, and how a plugin follows its
relay's durability. [Plugins](plugins.md) has the model: a plugin is installed,
enabled by name and bound to relays.

## Shared or one per relay

A plugin's provider declares whether one running copy can serve several relays:

- **Shareable** (a websocket, an HTTP poll, an HTTP push, the Kafka and RabbitMQ
  producers): each controller runs one copy for every unpartitioned relay bound to the
  plugin there, so a source opens one upstream connection and an output plugin runs one worker,
  however many relays share it.
- **Not shareable** (a file reader with one offset, a server on one port, a consumer
  holding one group member): the plugin serves one relay. Binding a second relay is
  refused with `plugin.not_shareable`, naming the relay that holds it. Enable another
  plugin of the same provider for the second relay.

`--single` at enable runs one copy in the whole cluster instead, on a controller the
cluster chooses, delivering to every bound relay on any controller. It applies to both
types. A `single` plugin takes no `on_output` hook (`plugin.single_output`): one copy cannot
be every relay's output plugin. When its controller leaves gracefully, the copy moves with a
hand-over that opens the new connection before the old one closes; when its controller
fails, it starts on another and its relays are served again without being bound again.

## A shared feed

A shared source hands every record to every relay bound to it, straight from the
worker that reads it, without waiting on any relay. A slow relay falls behind in
its own input queue and loses nothing, and the others are not slowed. A newly bound
relay is told to the worker when it binds, so it gets what arrives from then on (a
shared HTTP poll offers it the current content at the next poll).

## Partitioned relays

A plugin bound to a partitioned relay runs one copy on every member of the group,
shareable or not and `single` or not, and a member's copy is never shared with another
relay. Unbinding the group stops the members' copies and leaves any shared copy
running for its other relays. A websocket source on a group subscribes each member to
its own share of the items, and an HTTP poll can name the member's partition in its URL.

A source that cannot tell which member a record's key belongs to refuses a group member
with `plugin.partitioned_host`: the Kafka consumer, the RabbitMQ consumer, Postgres change
capture, the Postgres poll source and the HTTP server. Bind it to a relay that is not
partitioned and route from there. [`partitioning.md`](../core/partitioning.md) covers
groups.

## When a copy stops

A running copy whose last bound relay on that controller leaves stops:

- at once when the leave was deliberate: an unbind, a relay stop, a relay that
  ended;
- after `FLEZHA_PLUGIN_IDLE_STOP_MS` (default `60000`) when the relay went down or
  moved, unless a bound relay arrives first. A crash followed by a restart within that
  time keeps the connection open throughout.

A stopped plugin stays enabled and starts again when a bound relay runs. On a
relay's move the plugin starts on the new controller when the relay brings its
plugins up there, and the copy on the old controller stops only once the relay has
left it and the idle stop has passed, so the new connection opens before the old one
closes.

Only `flz plugin disable <name>` turns a plugin off. It stops every running copy on every
controller and leaves every binding on its relay. Each bound relay keeps running and
reports `plugin_unhealthy` with `plugin.not_found` for that binding, and enabling a plugin
of that name again brings the bindings up.

## A controller without the provider

Each controller has its own installed packages. When another controller that holds the
namespace lacks the provider, `flz plugin enable` warns, naming it: the plugin will not run
there. A relay bound to the plugin and placed on such a controller still runs and
processes its inputs, without the plugin:

- its health reports `plugin_unhealthy` with `plugin.provider_not_loaded`, naming the
  controller, and one warning is logged naming the plugin and why;
- a delivery-guaranteed output to that output plugin waits in the outbox and is delivered once the
  binding starts; a plain output is dropped and counted;
- the binding is tried again at the relay's next bring-up, so it comes up on a
  controller that has the package.

A controller plugin runs no copy on a controller that lacks its provider.

## Plugins follow the relay's durability

The controller tells each plugin how the relay it serves keeps its input, over anything
the plugin's stored options say. A source moves its upstream position (an offset, a
cursor, an acknowledgement) only past records the relay took, and how often follows
the relay:

| The relay | A verdict `ok` means | The source moves its position |
| --- | --- | --- |
| a flow relay without `effectively_once` that does not sleep | the relay took the record | every few seconds |
| a durable steps relay, or a flow relay that sleeps | the record is in the relay's input log | after every confirmed batch |
| a flow relay with `effectively_once` | the record is in the relay's input log | after every confirmed batch; an output plugin that honours `effectively_once` writes each output once |

A relay whose input is durable (effectively-once, sleeping or durable steps) does not
share a shareable source: it runs the source on its own, so every record is confirmed to
it, and a slow one holds back only its own upstream connection.

On the output plugin side, a delivery-guaranteed output is held in the relay's outbox until the
output plugin confirms it. What each output plugin honours (written once, at least once, or best effort) is
the Durability row of its catalogue page. [`durability.md`](../core/durability.md) has the levels,
and [`effectively_once.md`](../core/effectively_once.md) the tier.
