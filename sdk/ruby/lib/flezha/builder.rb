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
  ProcNode = Struct.new(:index, :name, :klass, :definition)

  # A channel as the graph holds it. Its position in the list IS its id.
  ChannelSpec = Struct.new(
    :index, :name, :kind, :producer, :consumers, :overflow, :retry_max,
    :retry_backoff_ms, :keyed, :transforms, :key_by, :decode, :encode,
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

    def checkpoint_every(count)
      @effectively_once_flag = true
      @ack_granularity = :checkpoint
      @checkpoint_every_value = count
      self
    end

    def outputs_at_checkpoint(max_delay_ms = nil)
      @checkpoint_outputs_value = :at_checkpoint
      @checkpoint_max_delay_ms_value = max_delay_ms unless max_delay_ms.nil?
      self
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
        overflow: :drop_nothing, keyed: false, transforms: [], decode: decode
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

      node = ProcNode.new(@procedures.size, identity, klass, definition)
      @procedures << node
      node
    end

    def add_channel(**spec)
      channel = ChannelSpec.new(index: @channels.size, **spec)
      @channels << channel
      channel
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

    # Stamp identity and `flz.key` onto emitted plugin frames.
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

      if @from[0] == :entry
        # One entry channel may feed several procedures; the transforms and the
        # key belong to the channel, which is what the prepare crossing names.
        channel = @from[1]
        channel.consumers << node.index
        apply_pending(channel)
      else
        @builder.add_channel(
          name: @pending[:channel_name] || "#{@from[1].name}__#{node.name}",
          kind: :inter_proc, producer: @from[1].index, consumers: [node.index],
          overflow: @pending[:overflow] || :drop_nothing,
          retry_max: @pending[:retry_max], retry_backoff_ms: @pending[:retry_backoff_ms],
          keyed: @pending[:keyed] || false, transforms: transforms, key_by: @pending[:key_by]
        )
      end

      Edge.new(@builder, [:procedure, node], {})
    end

    # Terminate this edge on a plugin-bound channel.
    def sink(name, encode: nil)
      unless @from[0] == :procedure
        raise DeclarationError.new(
          @builder.factory_id, name,
          "a plugin sink is fed by a procedure; wire the entry channel into one first"
        )
      end

      @builder.add_channel(
        name: name.to_s, kind: :plugin, producer: @from[1].index, consumers: [],
        overflow: @pending[:overflow] || :drop_nothing,
        retry_max: @pending[:retry_max], retry_backoff_ms: @pending[:retry_backoff_ms],
        keyed: @pending[:keyed] || false, transforms: transforms,
        key_by: @pending[:key_by], encode: encode
      )
      nil
    end

    private

    def transforms = @pending[:transforms] || []

    def with(**next_pending)
      Edge.new(@builder, @from, @pending.merge(next_pending))
    end

    def source_name = @from[1].name

    def apply_pending(channel)
      channel.overflow = @pending[:overflow] if @pending[:overflow]
      channel.retry_max = @pending[:retry_max] unless @pending[:retry_max].nil?
      channel.retry_backoff_ms = @pending[:retry_backoff_ms] unless @pending[:retry_backoff_ms].nil?
      channel.keyed = true if @pending[:keyed]
      channel.key_by = @pending[:key_by] if @pending[:key_by]
      channel.transforms.concat(transforms)
    end
  end
end
