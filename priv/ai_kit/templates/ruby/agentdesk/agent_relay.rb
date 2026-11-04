# frozen_string_literal: true

require "flezha"
require_relative "reply"

# One conversation: a relay that runs in steps, one turn per message it is
# sent, for as long as the session lasts. Every session is an instance of its
# own, with its own state.
class AgentRelay < Flezha::Relay
  factory_id "agent"
  run steps: { each: :per_input, on_failure: :keep_progress } # one turn per message; a failed turn fails alone
  durable local: { sleep_after: 10_000 } # an idle session sleeps after 10 s

  channels do |c|
    c.input("turn", codec: :text)                            # entry 1: `flz relay call`
     .into(Reply)
     .output("answer")                                       # to whoever called, and whoever listens
    c.input("message")                                       # entry 2: the gateway, into the same Reply
     .map { |message| message[:text] }
     .into(Reply)
  end
end
