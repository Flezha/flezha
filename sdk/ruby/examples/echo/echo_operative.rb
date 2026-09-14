# frozen_string_literal: true

require "flezha"
require_relative "echo_procedure"

# The deployable unit. An operative declares the graph a message travels: the
# entry channel it arrives on, the steps it passes through, and the channel its
# result leaves on.
#
# The graph is checked as a whole when the runner declares it, so one that
# cannot work is refused at startup. Values cross the wire as MessagePack unless
# a channel says otherwise; this one carries plain text both ways.
class EchoOperative < Flezha::StreamOperative
  # The operative's stable identity, and the name `flz operative start` takes.
  factory_id "echo"

  channels do |c|
    c.input("in", decode: ->(bytes) { bytes.dup.force_encoding("UTF-8") })
     .into(EchoProcedure)
     .sink("out", encode: ->(value) { value.to_s.b })
  end
end
