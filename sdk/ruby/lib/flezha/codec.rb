# frozen_string_literal: true

require "json"
require "msgpack"
require "set"

module Flezha
  # The MessagePack codec for durable `State` and `Change` bodies, and the Ruby
  # type mapping that goes with it.
  #
  # The mapping is specified here rather than inherited from a library default,
  # because the defaults are wrong for a format that has to survive a restart:
  #
  # | Ruby      | MessagePack            | Why not the default                    |
  # | --------- | ---------------------- | -------------------------------------- |
  # | `Symbol`  | extension 0            | otherwise it decodes back as a String, and `apply_change` reads a different value on replay than it did live |
  # | `Time`    | timestamp extension    | a string or a number loses the type     |
  # | Hash key  | symbolized on decode   | `{count: 1}` must fold back as `{count: 1}`, not `{"count" => 1}` |
  # | binary `String` | `bin`            | so a byte string survives as bytes rather than as invalid UTF-8 |
  #
  # Ruby's integers are arbitrary-precision, so the u64 offsets, seqs and
  # timestamps in the schema need no width policy: the hazard other SDKs carry
  # (a `number` losing precision above 2^53, a `PHP_INT_MAX` ceiling) has no
  # analogue here. Past `2^64 - 1` MessagePack itself has no integer, and it
  # raises rather than narrowing — which is the outcome that matters.
  #
  # `Marshal` appears nowhere on this path, or on the wire path. It embeds
  # class names and instance-variable layout, so renaming a State class breaks
  # the restore of every record written before the rename, and the first anyone
  # would learn of it is a restart.
  module Codec
    # Extension id for a Symbol. Ruby-side only: it rides inside a body whose
    # header already carries `sdk_lang`, so no other SDK ever decodes it.
    EXT_SYMBOL = 0x00

    FACTORY = MessagePack::Factory.new
    FACTORY.register_type(EXT_SYMBOL, Symbol)
    FACTORY.register_type(
      MessagePack::Timestamp::TYPE, Time,
      packer: MessagePack::Time::Packer, unpacker: MessagePack::Time::Unpacker
    )
    FACTORY.freeze

    module_function

    def encode(value)
      FACTORY.dump(value)
    end

    def decode(bytes)
      return nil if bytes.nil? || bytes.empty?

      FACTORY.load(bytes.b, symbolize_keys: true)
    end

    # A value the codec cannot carry, named by the declaring type and the path
    # to the offending field.
    #
    # Walked once at registration against what `initial_state` produces, so an
    # author learns at startup rather than at the first restore that their
    # state does not survive one.
    def assert_representable(type_name, sample)
      walk(type_name, sample, "", {})
    end

    def walk(type_name, value, path, seen)
      reason = unrepresentable_reason(value)
      raise UnrepresentableValueError.new(type_name, path.empty? ? "<root>" : path, reason) if reason
      return unless value.is_a?(Hash) || value.is_a?(Array)

      if seen.key?(value.object_id)
        raise UnrepresentableValueError.new(
          type_name, path.empty? ? "<root>" : path,
          "the value refers back to itself; MessagePack has no representation for a cycle"
        )
      end
      seen[value.object_id] = true
      if value.is_a?(Array)
        value.each_with_index { |item, index| walk(type_name, item, "#{path}[#{index}]", seen) }
      else
        value.each do |key, item|
          unless key.is_a?(Symbol) || key.is_a?(String)
            raise UnrepresentableValueError.new(
              type_name, path.empty? ? "<root>" : path,
              "a Hash key of #{key.class} decodes as a String; declare Symbol or String keys"
            )
          end
          walk(type_name, item, path.empty? ? key.to_s : "#{path}.#{key}", seen)
        end
      end
      seen.delete(value.object_id)
    end

    def unrepresentable_reason(value)
      case value
      when nil, true, false, Integer, Float, String, Symbol, Time, Hash, Array then nil
      when Proc, Method
        "a callable has no durable form; hold the data it closes over instead"
      when Set
        "a Set decodes as a Hash, losing its identity; declare an Array, or supply a " \
        "hand-written codec"
      else
        "an instance of #{value.class} decodes as a plain Hash, losing its class; declare a " \
        "Hash or an Array, or supply a hand-written codec"
      end
    end

    # The JSON form of a state value, for a live read and a migration handover.
    #
    # JSON has no form for three things this codec carries, so each is marked
    # rather than lost: a Symbol would come back a String, a Time a number, and
    # a binary String would be mangled by UTF-8 validation.
    def to_state_json(value)
      JSON.generate(json_safe(value))
    end

    def from_state_json(bytes)
      return nil if bytes.nil? || bytes.empty?

      json_revive(JSON.parse(bytes.b.force_encoding(Encoding::UTF_8)))
    end

    def json_safe(value)
      case value
      when Symbol then { "$sym" => value.to_s }
      when Time then { "$time" => [value.to_i, value.nsec] }
      when String then binary?(value) ? { "$bytes" => [value].pack("m0") } : value
      when Hash then value.to_h { |k, v| [k.to_s, json_safe(v)] }
      when Array then value.map { |item| json_safe(item) }
      else value
      end
    end

    def json_revive(value)
      case value
      when Hash
        marked = value.size == 1 ? value.keys.first : nil
        case marked
        when "$sym" then return value[marked].to_sym
        when "$bytes" then return value[marked].unpack1("m0").b
        when "$time"
          seconds, nsec = value[marked]
          return Time.at(seconds, nsec, :nanosecond)
        end
        value.to_h { |k, v| [k.to_sym, json_revive(v)] }
      when Array then value.map { |item| json_revive(item) }
      else value
      end
    end

    def binary?(value)
      value.encoding == Encoding::BINARY && !value.dup.force_encoding(Encoding::UTF_8).valid_encoding?
    end
  end
end
