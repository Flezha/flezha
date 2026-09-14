# frozen_string_literal: true

# Starts a runner: a process that connects to the controller and serves your
# operatives.
#
# Declare every operative you want served before `boot`. One runner can serve
# several, and `define_workflow` is the same call for the workflow kind.

require "flezha"
require_relative "echo_operative"

namespace = ARGV[0] || "default"

runner = Flezha.create_runner(namespace).define_stream(EchoOperative)

# `run` is `boot` followed by `await_termination`. Splitting them leaves room
# for the admin calls below, and still blocks until shutdown.
runner.boot

# Everything `flz operative` and `flz plugin` do is also a method here.
# Uncomment to drive this namespace from the application instead of from a
# terminal:
#
# admin = runner.admin
# admin.start_operative("my_echo", "echo")
# admin.enable_plugin("writer", {
#   "id" => "writer",
#   "provider" => "operative_file_sink",
#   "operative_id" => "my_echo",
#   "opts" => { "path" => "echo-out.txt" },
#   "hooks" => [["on_output", {}]]
# })
# admin.send_to("my_echo", "hello world", channel: "in")
#
# docs/sdk/admin.md has the whole surface. An enforced controller refuses all
# of it until `flz role bind runner@default app default`.

# Blocks until shutdown: a SIGTERM, or standard input closing.
runner.await_termination
