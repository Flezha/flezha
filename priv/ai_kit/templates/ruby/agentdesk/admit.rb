# frozen_string_literal: true

require "flezha"
require_relative "budget"

# Answers a message 202 when it goes on to the session, and 429 when its user is
# over the budget.
class Admit < Flezha::Tasklet
  tasklet_name "admit"

  def run(verdict, ctx)
    user = verdict[:message][:user]
    ctx.emit(
      if Budget.admitted?(verdict)
        { status: 202, body: "sent to #{user}" }
      else
        { status: 429, body: "#{user} has used #{verdict[:spent]} of today's #{Budget::DAILY_TOKENS} tokens" }
      end,
      key: user
    )
  end
end
