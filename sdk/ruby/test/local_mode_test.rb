# frozen_string_literal: true

require_relative "test_helper"

# The Ruby SDK's local mode: a user's own operative class, its declared
# two-procedure graph, and no controller anywhere.
#
# The graph and its expected sink output are read from
# `test/fixtures/local_mode_word_count.txt`, which the Rust, Java, Node,
# Python and Elixir local-mode suites read too — so every core is compared
# against one recorded expectation instead of by inspection.
class LocalModeTest < Minitest::Test
  # One line into its words: the hop to `Tally` is the runtime's job.
  class Split < Flezha::Procedure
    procedure_name "split"
    def process(line, ctx)
      line.split.each { |word| ctx.emit(word.downcase) }
      nil
    end
  end

  # One Entity per word, minted on first sight of it.
  class Tally < Flezha::Procedure
    procedure_name "tally"
    dynamic_entities idle_ttl_ms: 300_000
    def initial_state(_key) = { seen: 0 }
    def apply_change(state, change) = state[:seen] = change[:seen]

    def process(word, ctx)
      seen = ctx.state[:seen] + 1
      ctx.change({ seen: seen })
      ctx.emit("#{word} #{seen}")
    end
  end

  class WordCount < Flezha::StreamOperative
    factory_id "local_word_count_ruby"
    channels do |c|
      c.input("lines", codec: :text)
       .into(Split)
       .key_by { |word| word }
       .into(Tally).keyed
       .sink("totals")
    end
  end

  def setup
    skip "needs the compiled veneer (scripts/build-ruby-sdk.sh)" unless Flezha::TestSupport.native?
  end

  # The scenario every core runs: the lines to feed, and the sink values.
  def scenario
    path = Flezha::TestSupport.contract_file("test/fixtures/local_mode_word_count.txt")
    lines = []
    totals = Hash.new(0)
    File.read(path).each_line(chomp: true) do |row|
      lines << row.delete_prefix("lines: ") if row.start_with?("lines: ")
      totals[row.delete_prefix("totals: ")] += 1 if row.start_with?("totals: ")
    end
    [lines, totals]
  end

  def test_a_declared_graph_runs_locally_and_releases_the_recorded_output
    lines, expected = scenario
    released = []
    run = Flezha::LocalOperative.start(WordCount)
    begin
      run.on_sink("totals") { |output| released << output }
      lines.each { |line| run.feed("lines", line) }
      run.run_until_idle(20_000)
    ensure
      run.close
    end

    counted = released.each_with_object(Hash.new(0)) { |o, acc| acc[o.payload.dup.force_encoding("UTF-8")] += 1 }

    assert_equal expected, counted,
                 "both procedures ran, the runtime performed the hop, and one Entity per word counted"
    assert_equal "totals", released.first.channel
    %w[flz.entity flz.seq flz.key].each do |stamp|
      assert_includes released.first.metadata, stamp,
                      "a keyed sink carries the identity stamps a deployed run stamps"
    end
  end

  def test_a_channel_the_class_does_not_declare_is_refused_by_name
    run = Flezha::LocalOperative.start(WordCount)
    begin
      message = assert_raises(Flezha::Native::Error) { run.feed("nope", "1") }.message

      assert_includes message, "nope"
      assert_includes message, "lines"
    ensure
      run.close
    end
  end

  # Reads the map an undeclared entry decodes, and emits a string and a map.
  class Defaults < Flezha::Procedure
    procedure_name "defaults"
    def process(value, ctx)
      ctx.emit("a is #{value[:a]}")
      ctx.emit({ a: value[:a] })
      nil
    end
  end

  class DefaultCodecs < Flezha::StreamOperative
    factory_id "local_default_codecs_ruby"
    channels { |c| c.input("in").into(Defaults).sink("out") }
  end

  def test_an_undeclared_entry_reads_json_and_an_undeclared_sink_follows_the_value
    released = []
    run = Flezha::LocalOperative.start(DefaultCodecs)
    begin
      run.on_sink("out") { |output| released << output.payload }
      run.feed("in", '{"a":1}')
      run.run_until_idle(5_000)
    ensure
      run.close
    end

    assert_equal ["a is 1", '{"a":1}'], released
  end
end
