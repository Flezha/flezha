# frozen_string_literal: true

module Flezha
  # Base of everything this SDK raises, so a host can rescue one thing.
  class Error < StandardError; end

  # A registration refusal: names the offending class, the handler and the
  # defect. Registration-time refusal replaces the Rust SDK's compile-time one,
  # so the message carries what a compiler would have pointed at.
  class DeclarationError < Error
    attr_reader :factory_id, :handler, :defect

    def initialize(factory_id, handler, defect)
      @factory_id = factory_id
      @handler = handler
      @defect = defect
      super(handler ? "#{factory_id}: #{handler}: #{defect}" : "#{factory_id}: #{defect}")
    end
  end

  # An emit the declaration cannot carry — a channel the procedure does not
  # produce on, a reserved metadata key, a keyed channel with no key.
  class EmitError < Error; end

  # A value the durable codec cannot represent, named by the declaring type and
  # the path to the offending field.
  class UnrepresentableValueError < Error
    def initialize(type_name, path, reason)
      super("#{type_name}.#{path} cannot be encoded for durable state: #{reason}")
    end
  end

  # A handler error the retry ladder must not retry. Raise this (or set
  # `non_retryable` on any error) to park an item immediately.
  class NonRetryableError < Error
    def non_retryable? = true
  end
end
