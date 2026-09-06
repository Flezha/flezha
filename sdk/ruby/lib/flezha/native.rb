# frozen_string_literal: true

require_relative "errors"
require_relative "version"

module Flezha
  # The compiled veneer over `flz_core`'s C ABI, and the two rules that hold
  # around it: one context per process, and no core call from a process that
  # did not start it.
  #
  # `Flezha::Native` itself is defined by the extension (`ruby_glue.rs`). This
  # wrapper adds no ABI surface — every method below forwards to exactly one
  # veneer function — it adds the fork guard, which is a host rule rather than
  # a core one.
  class Core
    # A core call from a process that did not start the core.
    class ForkError < Error; end

    def self.load_extension
      # `lib/flezha/flz_core_ffi.<dlext>`; Ruby derives `Init_flz_core_ffi` from the
      # basename, which is what the veneer exports.
      require "flezha/flz_core_ffi"
    rescue LoadError => e
      raise Error,
            "the flezha native extension could not be loaded (#{e.message}). Install a " \
            "precompiled platform gem, or build the source gem with a Rust toolchain."
    end

    # Create the process context, negotiating the activation schema version
    # before any controller connection is attempted.
    def self.create
      load_extension
      Native.context_new(ACTIVATION_SCHEMA_VERSION)
      new
    end

    # The entry points that take no context and therefore no guard: they read
    # a process-global clock, channel table and watch table, none of which a
    # fork breaks. Reachable before `create`, which is what lets a runner
    # answer `last_entry_delivery` and register an idle watch before it boots.
    def self.context_free
      load_extension
      Native
    end

    def initialize
      # Recorded at construction rather than at `start`, so a fork between the
      # two is caught by the same guard.
      @owner_pid = Process.pid
    end

    # The process that owns the core. A fork after this point does not carry
    # the core's Rust threads — transport, flusher, the runner loop — so the
    # child would connect to nothing and serve nothing, silently.
    def guard!
      return if Process.pid == @owner_pid

      raise ForkError,
            "the flezha core was started in process #{@owner_pid} and this is #{Process.pid}: " \
            "the core owns native threads that fork(2) does not carry, so a child cannot " \
            "serve. Declare classes, warm up, fork, and start the core in each child."
    end

    # Every ABI entry point a running host reaches, in the veneer's own
    # spelling.
    #
    # Generated rather than written out fifteen times over: the fork guard then
    # provably applies to all of them — there is no site left to forget it at —
    # and `native_test.rb` asserts this list is exactly what the veneer
    # exports, so an entry point added there and not here is a red build rather
    # than a method that quietly does not exist.
    ENTRY_POINTS = %i[
      declare_class check_class start poll wait_starvation complete abandon shutdown
      watch_stdin_eof workflow_emit workflow_recv workflow_cancelled spawn_workflow
      monotonic_nanos entry_last_delivery_nanos idle_watch_new idle_watch_poll
      idle_watch_free last_error
    ].freeze

    ENTRY_POINTS.each do |entry|
      define_method(entry) do |*args|
        guard!
        Native.public_send(entry, *args)
      end
    end

    # The one entry point not named after its own: `context_free` releases what
    # `context_new` created, and `create` is what made this object.
    def free
      guard!
      Native.context_free
    end
  end
end
