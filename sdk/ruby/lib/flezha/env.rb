# frozen_string_literal: true

module Flezha
  # The one parse rule per knob type, shared with every other Flezha runtime.
  #
  # A knob's name, type and default are declared once in `Flezha.Config.Keys`;
  # what this module owns is the rule that declaration promises, so a Ruby
  # runner answers `"0"`, `"on"` or a typo exactly as the controller does.
  # `test/fixtures/knob_parse_cases.txt` is that rule written down.
  module Env
    TRUE_WORDS = %w[1 true yes on].freeze
    FALSE_WORDS = %w[0 false no off].freeze

    module_function

    # A count of one or more, else nil - `"0"` already means unset.
    def parse_positive(raw)
      value = whole(raw)
      value if value&.positive?
    end

    # A whole number of milliseconds, zero included.
    def parse_ms(raw)
      value = whole(raw)
      value if value && !value.negative?
    end

    # A whole number, negative included.
    def parse_int(raw) = whole(raw)

    # `1/true/yes/on` and `0/false/no/off`, case-insensitively.
    def parse_bool(raw)
      return nil if raw.nil?

      text = raw.strip.downcase
      return true if TRUE_WORDS.include?(text)
      return false if FALSE_WORDS.include?(text)

      nil
    end

    # A count from the environment, with `fallback` for anything that is not one.
    def positive(name, fallback) = parse_positive(ENV.fetch(name, nil)) || fallback

    # A flag from the environment, with `fallback` for anything that is not one.
    def flag(name, fallback)
      parsed = parse_bool(ENV.fetch(name, nil))
      parsed.nil? ? fallback : parsed
    end

    def whole(raw)
      return nil if raw.nil?

      Integer(raw.strip, exception: false)
    end
  end
end
