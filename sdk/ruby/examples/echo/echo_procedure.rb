# frozen_string_literal: true

require "flezha"

# A processing stage of the operative's graph, run once per message. This one
# prefixes the input, and what `process` returns is emitted.
#
# With no `initial_state` and no `apply_change`, nothing is kept between
# messages and nothing is journaled. Where the result goes is not decided here:
# the operative decides that, so the graph can be rewired without touching this
# class.
class EchoProcedure < Flezha::Procedure
  # The procedure's stable identity. State is filed under it, so renaming it
  # later declares a different procedure rather than renaming this one.
  procedure_name "echo"

  def process(input, _ctx) = "echo: #{input}"
end
