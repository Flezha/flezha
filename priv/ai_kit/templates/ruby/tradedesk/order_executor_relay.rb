# frozen_string_literal: true

require "flezha"
require_relative "reserve_funds"
require_relative "place_order"
require_relative "confirm_fill"
require_relative "record_order"

# The desk's steps relay: one instance per order, which runs its steps
# once and then ends.
#
# A steps relay declares its steps the way a flow relay declares
# its graph, on the same builder, and the controller schedules, restarts and
# relocates it in the same way. What differs is the lifecycle: each step's
# output is written down as it commits, so an instance that is interrupted
# resumes at the step it reached rather than from the beginning, and the
# instance ends by itself once the chain has run.
#
# The order an instance is started with is its first message on `order`, which
# names no codec, so it arrives as the Hash its JSON decodes to:
# `--text '{"symbol": "EUR/USD", "quantity": 10}' --channel order`.
#
# When a step fails and its retries are spent, the instance ends abnormally
# instead, and the default rollback undoes every step that finished, in
# reverse, through their `compensate` methods: the order is cancelled, then the
# money is released. A step with no `compensate` is skipped, which is why the
# desk's book is left alone.
class OrderExecutorRelay < Flezha::Relay
  factory_id "order_executor"
  # one instance per order; each step runs once
  # a failed step undoes the finished ones
  run steps: { each: :once, on_failure: :undo_all }

  # Two steps in order, then a fan-out: confirming the fill and booking the
  # order are two independent consequences of the placement, and neither waits
  # for the other. Each arm of `branch` receives the same edge, so the
  # declaration is exactly two `into` calls on the placed channel, written as
  # one.
  #
  # Nothing ends on an output: the last step ends the instance with its result,
  # which `flz relay info` shows. Nothing names a channel either, so each one
  # carries the name of the step that produces it.
  channels do |c|
    c.input("order")                                         # the order to carry out
     .into(ReserveFunds)                                     # step one
     .into(PlaceOrder)                                       # step two, once step one is done
     .branch(
       ->(fill) { fill.into(ConfirmFill) },                  # two arms, side by side
       ->(book) { book.into(RecordOrder) }
     )
  end
end
