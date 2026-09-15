# frozen_string_literal: true

require "flezha"
require_relative "echo_procedure"

# The deployable unit. An operative declares the graph a message travels: the
# entry channel it arrives on, the steps it passes through, and the channel its
# result leaves on.
#
# The graph is checked as a whole when the runner declares it, so one that
# cannot work is refused at startup. The entry names the text codec, so the
# procedure is handed a String, and the sink names none, so a String result
# leaves as its own bytes.
class EchoOperative < Flezha::StreamOperative
  # The operative's stable identity, and the name `flz operative start` takes.
  factory_id "echo"

  channels do |c|
    c.input("in", codec: :text)
     .into(EchoProcedure)
     .sink("out")
  end
end
