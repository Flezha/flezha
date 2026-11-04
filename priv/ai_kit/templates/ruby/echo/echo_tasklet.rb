# frozen_string_literal: true

require "flezha"

# A processing stage of the relay's graph, run once per message. This one
# prefixes the input and emits it through the context, the one way a value
# leaves a tasklet; what `run` returns is not output.
#
# `tasklet_name` is required. State is filed under it, so renaming the class
# keeps it, and renaming `tasklet_name` declares a different tasklet. With no
# `init_state` and no `apply_change`, nothing is kept between messages and
# nothing is journaled. Where the result goes is not decided here: the relay
# decides that, so the graph can be rewired without touching this class.
class EchoTasklet < Flezha::Tasklet
  tasklet_name "echo" # state is saved under this name, so choose it once

  def run(input, ctx) = ctx.emit("echo: #{input}") # where it goes is the relay's business
end
