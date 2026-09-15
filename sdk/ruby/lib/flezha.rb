# frozen_string_literal: true

# The Flezha Ruby SDK: a poll host over `flz_core`.
#
# The SDK contains no kernel code. Transport, framing, ordering, per-entity
# exclusivity, durability, retries, the retry-to-park ladder, timers, credit,
# routing, the workflow driver and reconnect all live in the core; this gem
# brings the authoring surface, the codec for user types, the threading host
# and idiomatic errors.
#
#   class Echo < Flezha::Procedure
#     def process(input, _ctx) = input
#   end
#
#   class EchoOperative < Flezha::StreamOperative
#     factory_id "echo"
#     channels { |c| c.input("input", codec: :text).into(Echo).sink("out") }
#   end
#
#   Flezha.create_runner("default").define_stream(EchoOperative).run

require_relative "flezha/version"
require_relative "flezha/errors"
require_relative "flezha/env"
require_relative "flezha/logger"
require_relative "flezha/identity"
require_relative "flezha/codec"
require_relative "flezha/state"
require_relative "flezha/builder"
require_relative "flezha/procedure"
require_relative "flezha/operative"
require_relative "flezha/graph"
require_relative "flezha/ctx"
require_relative "flezha/dispatch"
require_relative "flezha/native"
require_relative "flezha/admin"
require_relative "flezha/executor"
require_relative "flezha/runner"
require_relative "flezha/local"
require_relative "flezha/testing"
