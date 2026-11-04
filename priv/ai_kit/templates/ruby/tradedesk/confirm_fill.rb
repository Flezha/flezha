# frozen_string_literal: true

require "flezha"

# Asks the exchange whether the order filled, on one arm of the fan-out. The
# paper exchange rejects anything above its own size, which is what makes this
# the step that fails, and takes ten seconds to answer, which leaves time to
# stop the runner in the middle of an order.
#
# Under the default thread executor the wait holds one worker. When every
# worker has been busy for 100 ms the runner adds one, up to
# `FLEZHA_WORKER_POOL_MAX` (ten times the processor count by default), so the
# price stream keeps flowing; more orders waiting at once than that would stall
# it. Under the fiber executor `sleep` yields its fiber instead, so the wait
# stalls nothing and that cap does not apply.
#
# It remembers nothing and has nothing to undo, so it writes only `run`. It
# ends the instance with a result rather than emitting one: the order's outcome
# is the instance's own answer, which `flz relay info` reads back, so the
# desk needs no output here.
#
# A failure here is retried first, and once the retries are spent the instance
# ends abnormally. Everything that finished is then undone in reverse, across
# both arms: the order is cancelled and the money released, while the desk's
# book is skipped because it writes no `compensate`.
class ConfirmFill < Flezha::Tasklet
  tasklet_name "confirm_fill"

  EXCHANGE_MAX = 100

  def run(placed, ctx)
    sleep 10                                                 # the paper exchange takes ten seconds to answer
    quantity = placed[:order][:quantity]
    if quantity > EXCHANGE_MAX                               # tried again, then the order is undone
      raise "the exchange fills at most #{EXCHANGE_MAX} a time, and this is #{quantity}"
    end

    ctx.complete("filled #{placed[:order][:symbol]} #{quantity} as #{placed[:order_id]}")
  end
end
