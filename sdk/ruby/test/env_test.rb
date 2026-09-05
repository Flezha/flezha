# frozen_string_literal: true

require "test_helper"

# This runtime's reading of the shared parse fixture.
class EnvTest < Minitest::Test
  FIXTURE = File.expand_path("../../../test/fixtures/knob_parse_cases.txt", __dir__)

  RULE = {
    "positive_int" => ->(raw) { Flezha::Env.parse_positive(raw) },
    "ms" => ->(raw) { Flezha::Env.parse_ms(raw) },
    "int" => ->(raw) { Flezha::Env.parse_int(raw) },
    "bool" => ->(raw) { Flezha::Env.parse_bool(raw) }
  }.freeze

  def test_every_case_parses_the_way_the_fixture_says
    seen = []
    cases.each do |kind, raw, expected|
      seen << kind unless seen.include?(kind)
      rule = RULE[kind]
      refute_nil rule, "the fixture names a type this runtime has no rule for: #{kind}"
      got = rule.call(raw)
      assert_equal expected, got.nil? ? "error" : got.to_s, "#{kind}|#{raw}"
    end
    assert_equal %w[bool int ms positive_int], seen.sort
  end

  def test_an_unset_variable_leaves_the_default_standing
    absent = "FLZ_TEST_ABSENT_VARIABLE"
    assert_equal 7, Flezha::Env.positive(absent, 7)
    assert Flezha::Env.flag(absent, true)
  end

  private

  def cases
    File.readlines(FIXTURE, chomp: true).filter_map do |line|
      next if line.strip.empty? || line.start_with?("#")

      kind, raw, expected = line.split("|")
      [kind, raw == "<empty>" ? "" : raw, expected]
    end
  end
end
