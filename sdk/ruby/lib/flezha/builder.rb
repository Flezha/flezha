# frozen_string_literal: true

require_relative "errors"

module Flezha
  # What a `filter` yields for a value it rejects.
  #
  # A dropped value is not `nil` or `false` — both are values a handler may
  # legitimately carry — so the marker is an object nothing else can produce.
  # It stops the transform chain: the item is consumed without reaching a
  # handler, and at the prepare crossing it answers no key, which is how the
  # core learns not to route it.
  DROPPED = Object.new.freeze

  # A procedure as the graph holds it, with its position in the declaration.
  # `output` is the one channel it produces on, created on first wiring.
  ProcNode = Struct.new(:index, :name, :klass, :definition, :output)

  # One consumer of a channel, with the transforms and key of its own edge.
  ConsumerEdge = Struct.new(:procedure, :transforms, :key_by, keyword_init: true)

  # A channel as the graph holds it. Its position in the list IS its id.
  # `sink` is the plugin sink attached to it; nil means none. `sink_encode` is
  # that sink's payload encoder and belongs to the sink frame alone: an
  # inter-procedure frame is decoded with the class codec, so encoding one with
  # it hands the consuming procedure bytes it cannot read.
  ChannelSpec = Struct.new(
    :index, :name, :kind, :producer, :consumers, :overflow, :retry_max,
    :retry_backoff_ms, :keyed, :sink, :decode, :sink_encode,
    keyword_init: true
  )

  # Everything the graph knows about one declared class.
  ClassSpec = Struct.new(
    :factory_id, :kind, :persistent, :effectively_once, :ack_granularity,
    :checkpoint_every, :checkpoint_outputs, :checkpoint_max_delay_ms,
    :correction_window_ms, :beyond_window_policy, :compact_after_bytes,
    :sync_durability, :procedures, :channels, :workflow, :run,
    keyword_init: true
  )

  # Run an edge's transform chain, stopping at the first drop.
  #
  # One implementation for the three places a chain runs — an entry payload
  # before its handler, a payload at the prepare crossing, and an emit on its
  # way out — so a filter cannot mean one thing on one path and another
  # elsewhere.
  def self.apply_transforms(transforms, value)
    transforms.each do |transform|
      value = transform.call(value)
      return DROPPED if value.equal?(DROPPED)
    end
    value
  end

  # The fluent channel builder an operative's `channels` block receives.
  class Builder
    attr_reader :factory_id, :procedures, :channels, :effectively_once_flag,
                :ack_granularity, :checkpoint_every_value, :checkpoint_outputs_value,
                :checkpoint_max_delay_ms_value, :correction_window_ms_value,
                :beyond_window_policy_value, :compact_after_bytes_value,
                :sync_durability_flag

    def initialize(factory_id)
      @factory_id = factory_id
      @procedures = []
      @channels = []
      @effectively_once_flag = false
      @ack_granularity = :per_input
      @checkpoint_outputs_value = :as_produced
      @beyond_window_policy_value = :process_as_arrival
      @sync_durability_flag = false
    end

    def effectively_once
      @effectively_once_flag = true
      self
    end

    def checkpoint_ack
      amend_checkpoint {}
    end

    def checkpoint_every(count)
      amend_checkpoint { @checkpoint_every_value = count }
    end

    def outputs_at_checkpoint(max_delay_ms = nil)
      amend_checkpoint do
        @checkpoint_outputs_value = :at_checkpoint
        @checkpoint_max_delay_ms_value = max_delay_ms unless max_delay_ms.nil?
      end
    end

    def correction_window(milliseconds)
      @correction_window_ms_value = milliseconds
      self
    end

    def beyond_window_policy(policy)
      @beyond_window_policy_value = policy.to_sym
      self
    end

    def compact_after_bytes(bytes)
      @compact_after_bytes_value = bytes
      self
    end

    def sync_durability
      @sync_durability_flag = true
      self
    end

    # Declare a controller-fed entry channel. `decode` turns the wire bytes
    # into the value a handler sees; its absence means the MessagePack default.
    def input(name, decode: nil)
      channel = ChannelSpec.new(
        index: @channels.size, name: name.to_s, kind: :entry, consumers: [],
        overflow: :drop_nothing, keyed: false, decode: decode
      )
      @channels << channel
      Edge.new(self, [:entry, channel], {})
    end

    # The node for a procedure. Two unnamed mentions of one procedure are one
    # node — the naming convention every SDK mirrors — so a diamond that fans
    # out and back in has one instance, not two.
    def node_for(klass, name = nil)
      definition = klass.definition
      identity = (name || definition.name).to_s
      existing = @procedures.find { |node| node.name == identity && node.klass == klass }
      return existing if existing

      if @procedures.any? { |node| node.name == identity }
        raise DeclarationError.new(
          @factory_id, identity,
          "two different procedures claim this node identity; name one of them on the " \
          "`into(...)` so each node has its own"
        )
      end

      node = ProcNode.new(@procedures.size, identity, klass, definition, nil)
      @procedures << node
      node
    end

    # The channel this node produces on, created on first use.
    #
    # Lazily, so a leaf node never carries a phantom channel. Named by the
    # node's identity until a `named(...)` override, and reused by every later
    # call on an edge whose source is this node — which is what makes a fan-out
    # one channel with two consumers rather than two channels claiming one
    # producer.
    def output_channel_for(node)
      return node.output if node.output

      channel = ChannelSpec.new(
        index: @channels.size, name: node.name, kind: :inter_proc,
        producer: node.index, consumers: [], overflow: :drop_nothing, keyed: false
      )
      @channels << channel
      node.output = channel
      channel
    end

    private

    # Turn the checkpoint granularity on (idempotent) and amend its policy, so
    # each option is one call that preserves the others.
    def amend_checkpoint
      @ack_granularity = :checkpoint
      yield
      self
    end
  end

  # One edge under construction. Immutable: every fluent call answers a new
  # edge, so a shared prefix can be branched without the arms colliding.
  class Edge
    def initialize(builder, from, pending)
      @builder = builder
      @from = from
      @pending = pending
    end

    # Override the channel name this edge creates. The default is derived from
    # the producing node's identity.
    def named(name) = with(channel_name: name.to_s)

    # Transform every value on this edge. Evaluated producer-side.
    def map(&transform) = with(transforms: transforms + [transform])

    # Drop the values a predicate rejects, before this edge's key is computed.
    # A dropped value is consumed and reaches no handler.
    def filter(&predicate)
      with(transforms: transforms + [->(value) { predicate.call(value) ? value : DROPPED }])
    end

    # Route by a computed key: the consumer becomes a dynamic keyed procedure.
    def key_by(&key) = with(key_by: key)

    def on_full(policy) = with(overflow: policy.to_sym)
    def retry_max(attempts) = with(retry_max: attempts)
    def retry_backoff_ms(milliseconds) = with(retry_backoff_ms: milliseconds)

    # Stamp `flz.entity` / `flz.seq` / `flz.key` onto this channel's plugin frames.
    def keyed = with(keyed: true)

    # Fan out: each arm receives this same edge, so `branch` builds exactly the
    # topology the separate `into` statements would. At least two arms.
    def branch(*arms)
      if arms.size < 2
        raise DeclarationError.new(
          @builder.factory_id, @pending[:channel_name] || source_name,
          "branch(...) requires at least two arms; #{arms.size} given — a single arm is " \
          "`into(...)` written the long way"
        )
      end

      arms.each { |arm| arm.call(self) }
      self
    end

    # Wire this edge into a procedure.
    def into(klass, node_name = nil)
      node = @builder.node_for(klass, node_name)
      # The transforms and the key belong to this edge, not to the channel: one
      # channel may feed several procedures, and a chain declared before one
      # `into` must not reach another.
      channel.consumers << ConsumerEdge.new(
        procedure: node.index, transforms: transforms, key_by: @pending[:key_by]
      )
      Edge.new(@builder, [:procedure, node], {})
    end

    # Attach this edge's channel to the named plugin sink, and answer the edge.
    #
    # `sink(...).into(...)` wires a consumer behind the sink, so one emit
    # reaches both. A channel with no consumer behind it is the plugin-bound
    # terminal — terminality is the absence of further wiring, not a different
    # method.
    def sink(name, encode: nil)
      unless @from[0] == :procedure
        raise DeclarationError.new(
          @builder.factory_id, name,
          "a plugin sink is fed by a procedure; wire the entry channel into one first"
        )
      end

      target = channel
      if target.sink
        raise DeclarationError.new(
          @builder.factory_id, target.name, "declares a plugin sink more than once"
        )
      end

      target.sink = name.to_s
      target.sink_encode = encode
      self
    end

    private

    def transforms = @pending[:transforms] || []

    # The channel this edge's values leave on, with the pending options
    # applied. An entry edge already has one; a procedure edge's is created on
    # first use and reused after.
    def channel
      if @from[0] == :entry
        target = @from[1]
      else
        target = @builder.output_channel_for(@from[1])
        target.name = @pending[:channel_name] if @pending[:channel_name]
      end
      apply_pending(target)
      target
    end

    def with(**next_pending)
      Edge.new(@builder, @from, @pending.merge(next_pending))
    end

    def source_name = @from[1].name

    def apply_pending(channel)
      channel.overflow = @pending[:overflow] if @pending[:overflow]
      channel.retry_max = @pending[:retry_max] unless @pending[:retry_max].nil?
      channel.retry_backoff_ms = @pending[:retry_backoff_ms] unless @pending[:retry_backoff_ms].nil?
      channel.keyed = true if @pending[:keyed]
    end
  end
end
