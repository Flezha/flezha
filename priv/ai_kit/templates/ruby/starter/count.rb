# frozen_string_literal: true

require "flezha"

# Counts the visits of one page and answers the new count.
#
# The relay keys the channel by page, so each page has a cell of its own, and
# `{ total: }` is that cell's state. The state changes only through the change
# `run` records, which `apply_change` applies, so a runner that restarts finds
# the count it had.
class CountHits < Flezha::Tasklet
  tasklet_name "count" # state is saved under this name, so choose it once

  def init_state(_cell_key) = { total: 0 } # what each page's cell remembers

  def apply_change(seen, _counted) = seen[:total] += 1 # how a visit changes it

  def run(hit, ctx)
    ctx.change({})
    ctx.emit({ page: hit[:page], total: ctx.state[:total] })
  end
end
