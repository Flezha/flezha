# frozen_string_literal: true

require "flezha"
require_relative "count"

# A visit arrives on `hit`, goes to the cell of its page, and the count leaves
# on the output `counts`, which also answers a caller. The description is shown
# by `flz relay info` and to AI agents.
class StarterRelay < Flezha::Relay
  factory_id "starter" # the name `flz relay start` takes

  HIT = { type: "object", properties: { page: { type: "string" } }, required: %w[page] }.freeze

  channels do |c|
    c.input("hit", description: "Counts visits per page", schema: HIT)
     .key_by { |hit| hit[:page] } # one cell, and one count, per page
     .into(CountHits)
     .output("counts")
  end
end
