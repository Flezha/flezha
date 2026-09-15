# frozen_string_literal: true

# The two-procedure word count, run in this process with no controller.
#
#   scripts/build-ruby-sdk.sh
#   ruby -Isdk/ruby/lib examples/ruby/local_word_count.rb
#
# `Split` and `Tally` are ordinary procedures with nothing local-mode about
# them: the same classes deploy against a controller unchanged. What local mode
# supplies is the sink — the values `Tally` releases arrive here instead of on
# a socket.
#
# State is held in memory and nothing is durable; the run says so on stderr.

require "flezha"

class Split < Flezha::Procedure
  procedure_name "split"

  def process(line, ctx)
    line.split.each { |word| ctx.emit(word.downcase) }
    nil
  end
end

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
  factory_id "local_word_count_example"
  channels do |c|
    c.input("lines", codec: :text)
     .into(Split)
     # One Entity per word: the hop from `Split` to `Tally` is the runtime's,
     # not this file's.
     .key_by { |word| word }
     .into(Tally).keyed
     .sink("totals")
  end
end

run = Flezha::LocalOperative.start(WordCount)
begin
  run.on_sink("totals") { |output| puts output.payload }
  run.feed("lines", "the quick brown fox")
  run.feed("lines", "the lazy dog")
  run.run_until_idle(10_000)
ensure
  run.close
end
