# frozen_string_literal: true

require "flezha"

# Buys when a price is far enough above its symbol's average.
#
# It remembers nothing: the move it is handed already carries the average, so
# the decision is a function of one value, and a restart costs this stage
# nothing because there is nothing to bring back. That is why it writes no
# `init_state`, beside `TrackPrices`, which keeps the window.
#
# It does two things per input, both through the context: it starts a steps
# relay and it emits.
class BuyAboveAverage < Flezha::Tasklet
  tasklet_name "buy_above_average"

  BUY_ABOVE_PCT = 2.0
  UNITS_PER_PERCENT = 10

  def run(move, ctx)
    percent = (move[:price] - move[:average]) / move[:average] * 100.0
    return if percent < BUY_ABOVE_PCT

    quantity = (percent * UNITS_PER_PERCENT).round # a stronger move buys more

    order = { symbol: move[:symbol], quantity: quantity }
    ctx.spawn_steps("order_executor", order) # one steps relay per order

    ctx.emit(format("%<symbol>s %<price>.4f is %<percent>.2f%% above its average of %<average>.4f, buying %<quantity>d",
                    symbol: move[:symbol], price: move[:price], percent: percent,
                    average: move[:average], quantity: quantity))
  end
end
