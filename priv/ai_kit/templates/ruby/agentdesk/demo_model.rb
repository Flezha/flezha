# frozen_string_literal: true

require "flezha"

# The demo model: a canned answer, one token at a time, and the number of
# tokens it used. It stands in for a real model so the example runs anywhere; a
# real model's client would be called here instead, handing each token it
# streams to the block the same way.
#
# An answer, and each part of one, is a Hash of its text and the tokens used.
module DemoModel
  MAX_MESSAGE = 200
  PER_TOKEN_S = 0.05

  def self.answer(earlier, message)
    if message.length > MAX_MESSAGE
      raise Flezha::NonRetryableError,
            "the demo model reads at most #{MAX_MESSAGE} characters, and this message has #{message.length}"
    end

    text = "Message #{earlier.size + 1}: you said \"#{message}\". I am a demo model."
    tokens = text.split(/(?<= )/)
    tokens.each.with_index(1) do |token, i|
      yield({ text: token, tokens: i })
      sleep PER_TOKEN_S
    end
    { text: text, tokens: tokens.size }
  end
end
