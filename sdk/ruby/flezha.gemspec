# frozen_string_literal: true

require_relative "lib/flezha/version"

Gem::Specification.new do |spec|
  spec.name = "flezha"
  spec.version = Flezha::VERSION
  spec.summary = "The Flezha Ruby SDK — a poll host over flz_core"
  spec.description = <<~TEXT
    Author Flezha operatives in Ruby: stream and workflow classes over the same
    shared kernel the Rust, Java, Node and Elixir SDKs drive. Transport,
    ordering, per-entity exclusivity, durability, retries, timers, credit and
    the workflow driver all live in the core; this gem brings the authoring
    surface, the MessagePack codec, and a thread host that releases the GVL
    around every blocking core call.
  TEXT
  spec.homepage = "https://github.com/alfetahe/flezha"
  spec.license = "Apache-2.0"
  spec.authors = ["Flezha contributors"]

  # 3.3 rather than the 3.2 the design leaned to: `Process.warmup` is 3.3+ and
  # the startup order requires it unconditionally, and 3.2 is past end of life.
  spec.required_ruby_version = ">= 3.3.0"

  spec.files = Dir[
    "lib/**/*.rb",
    "ext/**/*.rb",
    "LICENSE*",
    "README.md",
    "THIRD_PARTY_NOTICES.md"
  ]
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/flz_core_ffi/extconf.rb"]

  spec.add_dependency "msgpack", "~> 1.7"
  spec.add_development_dependency "rubocop", "1.90.0"

  spec.metadata = {
    "source_code_uri" => spec.homepage,
    "documentation_uri" => "#{spec.homepage}/blob/main/docs/sdk/ruby_sdk.md",
    "rubygems_mfa_required" => "true"
  }
end
