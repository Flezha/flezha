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

  # A transform/key chain declared on an edge but not yet closed by `into`.
  # Every `map` / `filter` / `key_by` opens a new one over the previous, which
  # is what lets a shared prefix branch without the arms colliding.
  # `superseded` is set when a later link built on this one, so only a chain's
  # tip can dangle; `consumed` when an `into(...)` closed it onto an edge;
  # `keyed` once a `key_by` link joined, so a later link knows it follows one.
  Chain = Struct.new(:source, :superseded, :consumed, :keyed, keyword_init: true)

  # One transform-chain defect, on the channel whose chain declared it, under
  # the reference's own error name.
  ChainIssue = Struct.new(:channel, :issue, keyword_init: true)

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
    :sync_durability, :procedures, :channels, :chain_issues, :workflow, :run,
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
    attr_reader :factory_id, :procedures, :channels, :chains, :effectively_once_flag,
                :ack_granularity, :checkpoint_every_value, :checkpoint_outputs_value,
                :checkpoint_max_delay_ms_value, :correction_window_ms_value,
                :beyond_window_policy_value, :compact_after_bytes_value,
                :sync_durability_flag

    def initialize(factory_id)
      @factory_id = factory_id
      @procedures = []
      @channels = []
      @chains = []
      # What each link refused as it was declared, in declaration order.
      @link_issues = []
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

    # Extend the chain at `base` (or open one on `source`) with one more link,
    # answering the new chain's id.
    #
    # The three refusals the references raise from their own `extend_pending`
    # are raised here: a link declared behind a plugin sink, a transform
    # declared after the key it would feed, and a second key — which without
    # this would overwrite the author's first without a word.
    def extend_chain(source, base, key:)
      channel = channel_name(source)
      @link_issues << ChainIssue.new(channel: channel, issue: :TransformAfterTerminal) if sink_attached?(source)
      prior_key = !base.nil? && @chains[base].keyed
      @chains[base].superseded = true unless base.nil?
      # The key is evaluated on the final post-transform value, so a transform
      # declared after it would hand it a value of a type it was never written
      # against.
      @link_issues << ChainIssue.new(channel: channel, issue: :TransformAfterKeyBy) if prior_key && !key
      @link_issues << ChainIssue.new(channel: channel, issue: :DuplicateKeyBy) if prior_key && key
      @chains << Chain.new(source: source, superseded: false, consumed: false, keyed: key || prior_key)
      @chains.size - 1
    end

    # Mark a chain closed by an `into(...)`. A bare edge closes nothing.
    def close_chain(chain)
      @chains[chain].consumed = true unless chain.nil?
    end

    # Every transform-chain defect this declaration carries: what each link
    # refused as it was declared, then the tips no `into(...)` closed — the
    # order the references report them in, so the sharper defect wins over the
    # dangling one it also produces.
    def chain_issues
      dangling = @chains.reject { |chain| chain.superseded || chain.consumed }
                        .map { |chain| ChainIssue.new(channel: channel_name(chain.source), issue: :DanglingTransformChain) }
      @link_issues + dangling
    end

    # The name of the channel an edge's values leave on: the entry's own, or the
    # producing node's — its materialised channel when one exists, since a
    # `named(...)` override lives there, and the name it would derive otherwise.
    def channel_name(source)
      return source[1].name if source[0] == :entry

      node = source[1]
      node.output ? node.output.name : node.name
    end

    # Whether the channel an edge's values leave on already has a plugin sink.
    # Only a procedure's can: `sink(...)` refuses an entry edge, and a node's
    # channel is created on first use, so one without a channel has no sink.
    def sink_attached?(source)
      source[0] != :entry && !source[1].output.nil? && !source[1].output.sink.nil?
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
    def map(&transform) = relink(transforms: transforms + [transform])

    # Drop the values a predicate rejects, before this edge's key is computed.
    # A dropped value is consumed and reaches no handler.
    def filter(&predicate)
      relink(transforms: transforms + [->(value) { predicate.call(value) ? value : DROPPED }])
    end

    # Route by a computed key: the consumer becomes a dynamic keyed procedure.
    def key_by(&key) = relink(key_by: key)

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
      @builder.close_chain(@pending[:chain])
      # The transforms and the key belong to this edge, not to the channel: one
      # channel may feed several procedures, and a chain declared before one
      # `into` must not reach another.
      target = channel
      existing = target.consumers.find { |e| e.procedure == node.index }
      declares = !transforms.empty? || !@pending[:key_by].nil?
      if existing.nil?
        target.consumers << ConsumerEdge.new(
          procedure: node.index, transforms: transforms, key_by: @pending[:key_by]
        )
      elsif !existing.transforms.empty? || !existing.key_by.nil? || declares
        # Re-declaring one where either side carries a transform or a key is
        # refused rather than silently dropped — the edge would otherwise keep
        # whichever closure was declared first and deliver the wrong value, to
        # the wrong Entity, forever. Re-declaring a bare edge is the fan-in /
        # repeated `into` case and stays a no-op.
        raise DeclarationError.new(
          @builder.factory_id, target.name,
          "EdgeAlreadyWired: already feeds procedure `#{node.name}`; an edge carries exactly " \
          "one transform and one key or neither, so a re-declaration where either side carries " \
          "one is refused rather than silently keeping the first"
        )
      end
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

    # This edge with one more chain link — the one place a transform's or a
    # key's result edge is built, so every link is registered and a chain no
    # `into(...)` closes is reported rather than silently discarded.
    def relink(**next_pending)
      with(**next_pending,
           chain: @builder.extend_chain(@from, @pending[:chain], key: next_pending.key?(:key_by)))
    end

    def source_name = @builder.channel_name(@from)

    def apply_pending(channel)
      channel.overflow = @pending[:overflow] if @pending[:overflow]
      channel.retry_max = @pending[:retry_max] unless @pending[:retry_max].nil?
      channel.retry_backoff_ms = @pending[:retry_backoff_ms] unless @pending[:retry_backoff_ms].nil?
      channel.keyed = true if @pending[:keyed]
    end
  end
end
