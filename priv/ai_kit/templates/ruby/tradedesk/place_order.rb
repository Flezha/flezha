# frozen_string_literal: true

require "flezha"

# Step two: place the order with the paper exchange, keeping the id it answers
# with, and cancel the order again if it is undone. It has the same shape as
# step one.
class PlaceOrder < Flezha::Tasklet
  tasklet_name "place_order"

  # What this step remembers, and how the id is written down.
  def init_state(_cell_key) = { order_id: "" }
  def apply_change(state, change) = state[:order_id] = change[:order_id]

  # The step: place the order under an id, remember the id, and hand the order on.
  def run(reserved, ctx)
    order_id = "ord-#{ctx.stable_id}"
    ctx.change({ order_id: order_id })
    ctx.emit({ order: reserved[:order], order_id: order_id })
  end

  # The undo: cancel the order, by the id the state kept.
  def compensate(_outputs, ctx)
    puts "[undo] cancelled #{ctx.state[:order_id]}"
  end
end
