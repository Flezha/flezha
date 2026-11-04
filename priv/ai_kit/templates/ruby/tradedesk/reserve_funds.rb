# frozen_string_literal: true

require "flezha"

# Step one: put the money for the order aside, and release it again if the
# order is undone.
#
# The reservation id comes from `ctx.stable_id`, and the step both remembers it
# and hands it on, beside the order.
class ReserveFunds < Flezha::Tasklet
  tasklet_name "reserve_funds"

  # What this step remembers, and how the id is written down.
  def init_state(_cell_key) = { id: "" }
  def apply_change(state, change) = state[:id] = change[:id]

  # The step: reserve the money under an id, remember the id, and hand the order on.
  def run(order, ctx)
    id = "res-#{ctx.stable_id}" # the same id when the step runs again
    ctx.change({ id: id })
    ctx.emit({ order: order, reservation_id: id })
  end

  # The undo: release the money, by the id the state kept.
  def compensate(_outputs, ctx)
    puts "[undo] released #{ctx.state[:id]}"
  end
end
