# frozen_string_literal: true

require_relative "test_helper"

class CodecTest < Minitest::Test
  # Offsets, sequence numbers and timestamps are unsigned 64-bit in the core.
  # Ruby's integers are arbitrary-precision, so there is no width policy to get
  # wrong — but "no policy" is a claim, and this is what makes it one that
  # cannot quietly stop being true.
  def test_unsigned_64_bit_values_round_trip_without_narrowing
    values = [0, 1, (2**53) + 1, (2**63) - 1, 2**63, (2**63) + 1, (2**64) - 1]
    decoded = Flezha::Codec.decode(Flezha::Codec.encode({ seqs: values }))

    assert_equal values, decoded[:seqs]
    decoded[:seqs].each_with_index do |value, index|
      assert_kind_of Integer, value
      assert_equal values[index], value, "value at #{index} narrowed"
    end
  end

  def test_a_high_bit_sequence_number_survives_the_state_json_form
    value = (2**63) + 12_345
    revived = Flezha::Codec.from_state_json(Flezha::Codec.to_state_json({ seq: value }))

    assert_equal value, revived[:seq]
  end

  def test_symbols_times_and_binary_strings_round_trip
    original = {
      status: :open,
      at: Time.at(1_700_000_000, 123_456, :nanosecond),
      blob: "\xff\x00\xfe".b,
      nested: { list: [:a, "b", 3] }
    }

    assert_equal original, Flezha::Codec.decode(Flezha::Codec.encode(original))
    assert_equal original, Flezha::Codec.from_state_json(Flezha::Codec.to_state_json(original))
  end

  def test_hash_keys_come_back_as_symbols
    # Otherwise `apply_change` reads `{"count" => 1}` on the replay path and
    # `{count: 1}` on the live one, and the two folds disagree silently.
    decoded = Flezha::Codec.decode(Flezha::Codec.encode({ count: 1 }))

    assert_equal({ count: 1 }, decoded)
  end

  def test_a_value_the_codec_cannot_carry_is_refused_by_name
    error = assert_raises(Flezha::UnrepresentableValueError) do
      Flezha::Codec.assert_representable("Order.state", { lines: [{ price: Object.new }] })
    end

    assert_includes error.message, "Order.state.lines[0].price"
  end

  def test_a_cycle_is_refused
    state = { name: "a" }
    state[:self] = state

    assert_raises(Flezha::UnrepresentableValueError) do
      Flezha::Codec.assert_representable("S", state)
    end
  end

  # A rename of the declaring class must not break restore: the durable form is
  # the Hash `to_h` answers, not the class name `Marshal` would have embedded.
  def test_a_renamed_state_class_still_restores
    before = Struct.new(:total) do
      def to_h = { total: total }
      def self.from_h(hash) = new(hash[:total])
    end
    after = Struct.new(:total) do
      def to_h = { total: total }
      def self.from_h(hash) = new(hash[:total])
    end

    bytes = Flezha::TypeCodec.new(before, "state").encode(before.new(9))
    restored = Flezha::TypeCodec.new(after, "state").decode(bytes)

    assert_equal 9, restored.total
  end

  def test_a_type_missing_either_codec_method_is_refused_naming_it
    only_to_h = Class.new { def to_h = {} }
    only_from_h = Class.new { def self.from_h(hash) = hash }

    assert_includes(
      assert_raises(Flezha::DeclarationError) { Flezha::TypeCodec.new(only_to_h, "state") }.message,
      "from_h"
    )
    assert_includes(
      assert_raises(Flezha::DeclarationError) { Flezha::TypeCodec.new(only_from_h, "change") }.message,
      "to_h"
    )
  end
end

# `Marshal` embeds class names and instance-variable layout, so a rename breaks
# the restore of every record written before it — and the first anyone would
# learn of it is a restart. It is not used anywhere in this gem, and this is
# what keeps a later change from reintroducing it without noticing.
class NoMarshalTest < Minitest::Test
  SOURCES = Dir[File.expand_path("../lib/**/*.rb", __dir__)].sort

  def test_the_gem_ships_no_marshal_call
    offenders = SOURCES.each_with_object({}) do |path, found|
      File.readlines(path).each_with_index do |line, index|
        next if line.lstrip.start_with?("#")
        next unless line.match?(/\bMarshal\s*\.\s*(dump|load)\b/)

        (found[path] ||= []) << index + 1
      end
    end

    assert_empty offenders,
                 "Marshal reaches the persistence or wire path at #{offenders.inspect}; encode " \
                 "the Hash a type answers instead, so a class rename survives a restart"
  end

  def test_the_sources_it_scans_are_the_ones_that_ship
    # A guard that scans nothing passes forever.
    refute_empty SOURCES
    assert_includes SOURCES.map { |path| File.basename(path) }, "codec.rb"
  end
end
