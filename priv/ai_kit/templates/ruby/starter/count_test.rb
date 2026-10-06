# frozen_string_literal: true

require "minitest/autorun"
require_relative "count"
require "flezha/testing"

# Runs the tasklet with no controller: one harness per page stands in for that
# page's cell.
class CountHitsTest < Minitest::Test
  def test_counts_each_page_on_its_own_and_survives_a_restart
    cells = {}
    totals = %w[home home about home].map do |page|
      cells[page] ||= Flezha::Testing::Harness.new(CountHits, cell_key: page)
      cells[page].feed({ page: page }).outputs.first[:total]
    end

    assert_equal [1, 2, 1, 3], totals
    # Replaying the recorded changes rebuilds the state each cell had, as a
    # restart does; it raises when they differ.
    cells.each_value(&:replay_equals!)
  end
end
