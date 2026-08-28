# frozen_string_literal: true

require "minitest/autorun"

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
require "flezha"

module Flezha
  module TestSupport
    # The repository root, for the fixtures the gem's tests share with the
    # other SDKs' tests.
    def self.repo_root = File.expand_path("../../..", __dir__)

    # Whether the compiled veneer is present. The pure-Ruby tests run without
    # it; the ones that drive the core say why they are skipped rather than
    # passing vacuously.
    def self.native?
      @native = begin
        Core.load_extension
        true
      rescue StandardError
        false
      end
      @native
    end
  end
end
