# frozen_string_literal: true

require "flezha"
require_relative "echo_tasklet"

# The deployable unit. A relay declares the graph a message travels: the
# entry channel it arrives on, the steps it passes through, and the channel its
# result leaves on.
#
# The graph is checked as a whole when the runner declares it, so one that
# cannot work is refused at startup. The entry names the text codec, so the
# tasklet is handed a String, and the output names none, so a String result
# leaves as its own bytes.
class EchoRelay < Flezha::Relay
  # The relay's stable identity, and the name `flz relay start` takes.
  factory_id "echo" # the name `flz relay start` takes

  channels do |c|
    c.input("in", codec: :text) # messages arrive on the entry channel "in"
     .into(EchoTasklet)          # each one runs through the echo tasklet
     .output("out")              # the result leaves on "out", for a plugin to pick up
  end
end
