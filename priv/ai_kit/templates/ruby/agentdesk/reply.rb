# frozen_string_literal: true

require "flezha"
require_relative "demo_model"

# Answers a message with the demo model, sending the answer as it grows, and
# keeps the conversation so the next answer knows what came before.
class Reply < Flezha::Tasklet
  tasklet_name "reply"

  def init_state(_session) = { messages: [] } # what the session remembers

  def apply_change(conversation, change)
    case change
    in { kind: "said", message: } then conversation[:messages] << message
    in { kind: "unsaid" } then conversation[:messages].pop
    end
  end

  def run(message, ctx)
    # Each piece streams out as it is made; then the whole answer.
    ctx.emit(DemoModel.answer(ctx.state[:messages], message) { |part| ctx.part(part) })
    ctx.change({ kind: "said", message: message })
  end

  # The undo, when a turn is taken back: the conversation forgets its message.
  def compensate(_answers, ctx)
    puts "[undo] forgot \"#{ctx.state[:messages].last}\""
    ctx.change({ kind: "unsaid" })
  end
end
