# frozen_string_literal: true

require_relative "builder"
require_relative "declarative"
require_relative "errors"

module Flezha
  # The base an authored operative subclasses. Its class body declares the
  # factory id and the channels graph; `spec` resolves both once.
  class Operative
    extend Declarative

    # The channels graph. The block receives the builder.
    block_setting :channels
    declares :persistent
    # `factory_id` is deliberately NOT inherited: two operatives cannot share
    # one, and a subclass silently taking its parent's would be refused by the
    # core at declare time rather than here.

    class << self
      def factory_id(id = nil)
        return @factory_id if id.nil?

        @factory_id = id.to_s
      end

      # Whether change bytes are flushed to the per-entity WAL. Default true.
      def persistent(flag = nil)
        return @persistent.nil? ? true : @persistent if flag.nil?

        @persistent = flag
      end

      def spec
        @spec ||= build_spec
      end

      private

      def build_builder
        unless @factory_id
          raise DeclarationError.new(name.to_s, nil, "declares no factory_id")
        end

        builder = Builder.new(@factory_id)
        channels&.call(builder)
        builder
      end
    end
  end

  # A stream operative: fed by the controller through its entry channels.
  class StreamOperative < Operative
    class << self
      private

      def build_spec
        builder = build_builder
        ClassSpec.new(
          factory_id: @factory_id, kind: :stream, persistent: persistent,
          effectively_once: builder.effectively_once_flag,
          ack_granularity: builder.ack_granularity,
          checkpoint_every: builder.checkpoint_every_value,
          checkpoint_outputs: builder.checkpoint_outputs_value,
          checkpoint_max_delay_ms: builder.checkpoint_max_delay_ms_value,
          correction_window_ms: builder.correction_window_ms_value,
          beyond_window_policy: builder.beyond_window_policy_value,
          compact_after_bytes: builder.compact_after_bytes_value,
          sync_durability: builder.sync_durability_flag,
          procedures: builder.procedures, channels: builder.channels
        )
      end
    end
  end

  # A workflow operative: a run-sugar body, or a structured chain over a
  # channels graph.
  class WorkflowOperative < Operative
    setting :loop_mode, :rollback_mode, :retry_attempts, :retry_backoff_ms,
            :barrier_retry_attempts, :max_duration_ms, :wake_after_ms
    flag :sync_durability
    # A run-sugar body: one imperative block over `(args, ctx)`.
    block_setting :run

    class << self
      private

      def build_spec
        if run && channels
          raise DeclarationError.new(@factory_id.to_s, nil,
                                     "declares both a run body and a channels graph; a " \
                                     "workflow is one or the other")
        end
        if !run && !channels
          raise DeclarationError.new(@factory_id.to_s, nil,
                                     "declares neither a run body nor a channels graph")
        end

        builder = build_builder
        ClassSpec.new(
          factory_id: @factory_id, kind: :workflow, persistent: persistent,
          effectively_once: builder.effectively_once_flag, ack_granularity: :per_input,
          checkpoint_outputs: :as_produced, beyond_window_policy: :process_as_arrival,
          sync_durability: sync_durability_declared?,
          procedures: builder.procedures, channels: builder.channels,
          workflow: {
            loop_mode: (loop_mode || :once).to_sym,
            rollback_mode: (rollback_mode || :all_or_nothing).to_sym,
            retry_attempts: retry_attempts, retry_backoff_ms: retry_backoff_ms,
            barrier_retry_attempts: barrier_retry_attempts,
            max_duration_ms: max_duration_ms, wake_after_ms: wake_after_ms
          },
          run: run
        )
      end

      def sync_durability_declared? = @sync_durability || false
    end
  end
end
