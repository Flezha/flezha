# frozen_string_literal: true

require "flezha"

# Keeps a symbol's last ten prices, and compares each new trade with their
# average.
#
# One cell per symbol, because the relay keys the channel by symbol, and the
# window is that cell's state. Every change to it is recorded as it happens, so
# a runner that is killed and started again finds the window it had rather
# than an empty one.
#
# A trade is the Hash the exchange sent, of which the desk reads `:symbol` and
# `:price`; the move it hands on is a Hash of the symbol, the price and the
# average.
class TrackPrices < Flezha::Tasklet
  tasklet_name "track_prices"

  WINDOW = 10

  def init_state(_cell_key) = { prices: [] }                 # what each symbol's cell remembers

  def apply_change(state, change)
    state[:prices] << change[:price]                         # how a price joins the window
    state[:prices].shift if state[:prices].size > WINDOW
  end

  def run(trade, ctx)
    price = trade[:price]
    raise ArgumentError, "the price is not a number: #{price.inspect}" unless price.is_a?(Numeric) # JSON checks no types

    prices = ctx.state[:prices]
    unless prices.empty? # a symbol's first trade has nothing to compare with
      ctx.emit({ symbol: trade[:symbol], price: price, average: prices.sum.fdiv(prices.size) })
    end
    ctx.change({ price: price })
  end
end
