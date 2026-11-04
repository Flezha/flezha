# frozen_string_literal: true

require "flezha"

# Keeps the tokens a user's answers used today, and admits a message while they
# are under the day's budget. One cell per user.
#
# What reaches a user's cell is a message to admit, or the tokens an answer
# used; what it hands on is a verdict: the message, and the tokens the user had
# spent when it arrived.
class Budget < Flezha::Tasklet
  tasklet_name "budget"

  DAILY_TOKENS = 40

  def self.admitted?(verdict) = verdict[:spent] < DAILY_TOKENS

  def self.used(response)                                    # a failed turn used none
    { user: response.request[:user], tokens: response.ok? ? response.value[:tokens] : 0 }
  end

  def init_state(_user) = { day: "", tokens: 0 }             # what each user's cell remembers

  def apply_change(spent, counted)
    spent[:tokens] = on(spent, counted[:day]) + counted[:tokens]
    spent[:day] = counted[:day]
  end

  def run(event, ctx)
    today = Time.at(ctx.now / 1000, in: "UTC").strftime("%F")
    if event.key?(:tokens)                                   # the tokens an answer used: count them
      ctx.change({ day: today, tokens: event[:tokens] })
    else                                                     # a message to admit: hand on its verdict
      ctx.emit({ message: event, spent: on(ctx.state, today) })
    end
  end

  private

  def on(spent, today) = spent[:day] == today ? spent[:tokens] : 0
end
