# frozen_string_literal: true

require_relative "codec"
require_relative "declarative"
require_relative "errors"

module Flezha
  # The codec for an author-declared State or Change class.
  #
  # `to_h` on the way out and `.from_h` on the way back, over MessagePack —
  # never `Marshal`, which embeds the class name and the instance-variable
  # layout, so a rename would break the restore of every record written before
  # it. Encoding the Hash instead is what makes the rename survivable.
  #
  # Built on the same `state_codec` / `change_codec` seam a hand-written codec
  # uses, rather than beside it: the dispatcher and the restore fold already
  # ask that seam, and a second one would be a second answer to one question.
  class TypeCodec
    attr_reader :type

    def initialize(type, which)
      unless type.method_defined?(:to_h)
        raise DeclarationError.new(type.to_s, which,
                                   "declares no `to_h`; a durable #{which} is encoded as the " \
                                   "Hash it answers")
      end
      unless type.respond_to?(:from_h)
        raise DeclarationError.new(type.to_s, which,
                                   "declares no `.from_h`; a durable #{which} is rebuilt from " \
                                   "the Hash it was encoded as")
      end

      @type = type
    end

    def encode(value) = Codec.encode(value.to_h)
    def decode(bytes) = @type.from_h(Codec.decode(bytes))
  end

  # What a declared procedure resolves to: the scalar declarations, and the one
  # instance whose methods the dispatcher calls.
  #
  # Procedures hold no state of their own — an entity's state lives in the
  # registry and is handed to each invocation — so one instance per class is
  # enough, and the dispatcher never allocates per activation.
  class Definition
    attr_reader :name, :entity_keys, :idle_ttl_ms, :max_entities, :clock,
                :mailbox_capacity, :schema_version, :min_readable_schema_version,
                :state_codec, :change_codec, :migrate_state, :migrate_change, :instance

    # Read straight off the class rather than through a mirrored option hash:
    # the hash was the declaration written twice, and a field added to one half
    # and not the other is a declaration that silently does not reach the core.
    # Captured here rather than delegated, because the dispatcher reads these
    # on the hot path.
    def initialize(klass)
      @klass = klass
      @name = klass.procedure_name
      @entity_keys = klass.entity_keys
      @dynamic = klass.dynamic_entities?
      @idle_ttl_ms = klass.idle_ttl_ms
      @max_entities = klass.max_entities
      @clock = (klass.clock || :wall).to_sym
      @mailbox_capacity = klass.mailbox_capacity
      @schema_version = klass.schema_version
      @min_readable_schema_version = klass.min_readable_schema_version
      @state_codec = klass.state_codec
      @change_codec = klass.change_codec
      @migrate_state = klass.migrate_state
      @migrate_change = klass.migrate_change
      @instance = klass.new
    end

    def entity_mode
      return :static if @entity_keys
      return :dynamic if @dynamic

      :single
    end

    def declares?(method) = @klass.method_defined?(method) || @klass.private_method_defined?(method)

    def initial_state? = declares?(:initial_state)
    def apply_change? = declares?(:apply_change)
    def entities_for? = declares?(:entities_for)
    def event_time? = declares?(:event_time)
    def on_timer? = declares?(:on_timer)
    def compensate? = declares?(:compensate)

    def build_initial_state(entity_key)
      initial_state? ? @instance.initial_state(entity_key) : nil
    end

    def fold_change(state, change) = @instance.apply_change(state, change)
    def entities_for(input) = Array(@instance.entities_for(input))
    def event_time_of(input) = Integer(@instance.event_time(input))
  end

  # The base every authored procedure subclasses.
  #
  # The class body carries the scalar declarations; the instance methods carry
  # the behaviour. Which optional hooks a procedure declares is read from the
  # methods it defines — `apply_change` present means it folds changes,
  # `on_timer` present means it may register timers — so a declaration and its
  # implementation cannot disagree.
  class Procedure
    extend Declarative

    # The scalar declarations. Each is read by its bare name and written by
    # naming a value: `clock` answers, `clock :event_time` declares.
    setting :mailbox_capacity, :schema_version, :min_readable_schema_version,
            :state_codec, :change_codec, :clock
    block_setting :migrate_state, :migrate_change
    # Written by the declarations below, which take more than one value or
    # write more than one variable; named here so a subclass inherits them.
    declares :entity_keys, :dynamic, :idle_ttl_ms, :max_entities, :state_type, :change_type
    # `procedure_name` is deliberately NOT inherited: a node identity is per
    # class, and a subclass silently taking its parent's would make two
    # procedures claim one node — which the builder refuses, at the far end of
    # the declaration rather than here.

    class << self
      # The node identity within its operative. Defaults to the class name in
      # snake_case, which is what an author would have typed anyway.
      def procedure_name(name = nil)
        return @procedure_name || default_name if name.nil?

        @procedure_name = name.to_s
      end

      def entity_keys(*keys)
        return @entity_keys if keys.empty?

        @entity_keys = keys.flatten.map(&:to_s)
      end

      def dynamic_entities(idle_ttl_ms: nil, max_entities: nil)
        @dynamic = true
        @idle_ttl_ms = idle_ttl_ms
        @max_entities = max_entities
      end

      # Whether entities appear on first key rather than from a declared set.
      def dynamic_entities? = @dynamic || false

      attr_reader :idle_ttl_ms, :max_entities

      # Declare the class an entity's State is. Its `to_h` is what is encoded
      # and its `.from_h` is what rebuilds it, so a rename of the class does
      # not break the restore of records written before it — the durable form
      # is the Hash, not the class.
      def state_type(type = nil)
        return @state_type if type.nil?

        @state_type = type
        @state_codec = TypeCodec.new(type, "state")
      end

      # Declare the class a recorded Change is. Same contract as `state_type`.
      def change_type(type = nil)
        return @change_type if type.nil?

        @change_type = type
        @change_codec = TypeCodec.new(type, "change")
      end

      # The resolved declaration. Built once: it instantiates the procedure,
      # and the dispatcher reads it on the hot path.
      def definition
        @definition ||= build_definition
      end

      private

      def build_definition
        unless method_defined?(:process)
          raise DeclarationError.new("<procedure>", procedure_name,
                                     "declares no `process` method; a procedure is what runs " \
                                     "against an input")
        end
        if @entity_keys && @dynamic
          raise DeclarationError.new("<procedure>", procedure_name,
                                     "declares both a static key set and dynamic entities; the " \
                                     "two modes are exclusive")
        end
        if @entity_keys&.empty?
          raise DeclarationError.new("<procedure>", procedure_name,
                                     "declares an empty static key set; a static procedure " \
                                     "names its entities")
        end
        # State without a fold is legitimate: a value seeded at start and read
        # but never mutated has nothing to journal, and `ctx.change` already
        # refuses at the point of use. A fold with no state to fold into is not.
        if !method_defined?(:initial_state) && method_defined?(:apply_change)
          raise DeclarationError.new("<procedure>", procedure_name,
                                     "declares apply_change but no initial_state; there is " \
                                     "nothing for a change to fold into")
        end

        definition = Definition.new(self)
        # A state that cannot be encoded cannot be restored, and the first
        # anyone would learn of it is a restart. Walk the declared starting
        # state now.
        if definition.initial_state? && !state_codec
          Codec.assert_representable("#{procedure_name}.state",
                                     definition.build_initial_state(nil))
        end
        definition
      end

      def default_name
        name.to_s.split("::").last
            .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
            .gsub(/([a-z\d])([A-Z])/, '\1_\2')
            .downcase
      end
    end
  end
end
