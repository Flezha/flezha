defmodule FlezhaTest.Helper.TierE2EHarnessTest do
  @moduledoc """
  `unique/2` carries the operator-id cap itself. Three of its call sites
  compute their prefix rather than writing one down, so no rule about call
  sites can hold the bound; and the ids it mints name state trees on disk, so
  an id that fits today has to come out byte for byte as it did before.
  """
  use ExUnit.Case, async: true

  import FlezhaTest.Helper.TierE2EHarness, only: [unique: 2, unique_id_path: 1]

  alias Flezha.Identificator

  # The widest prefix the suite mints, and two computed ones. 24 bytes leaves
  # room for 7 digits, which is one more than a full run reaches.
  @widest "checkpoint_batched_store"
  @prefixes [@widest, "d_do_nothing", "pipe_compare_filter", "tier_commit_block_steps"]

  defp counter_of(digits), do: String.to_integer(String.duplicate("9", digits))

  test "an id fits the cap at every counter width a run can reach" do
    for prefix <- @prefixes, digits <- 1..12 do
      counter = counter_of(digits)
      id = unique(prefix, counter)

      assert Identificator.validate(:operator, id) == :ok,
             "#{byte_size(prefix)}-byte prefix at #{digits} digits: #{id} is #{byte_size(id)} bytes"

      assert String.ends_with?(id, "_" <> Integer.to_string(counter))
    end
  end

  test "truncation never engages on an id that already fits" do
    for digits <- 1..7 do
      counter = counter_of(digits)
      plain = @widest <> "_" <> Integer.to_string(counter)

      # The claim is only about ids that fit, so say so rather than skipping.
      assert byte_size(plain) <= Identificator.limit(:operator)
      assert unique(@widest, counter) == plain
    end
  end

  test "an id that would breach gives from the prefix, never from the counter" do
    for digits <- 8..12 do
      counter = counter_of(digits)
      suffix = "_" <> Integer.to_string(counter)
      id = unique(@widest, counter)

      assert byte_size(id) == Identificator.limit(:operator)
      assert String.ends_with?(id, suffix)

      kept = binary_slice(id, 0, byte_size(id) - byte_size(suffix))
      assert String.starts_with?(@widest, kept)
      refute kept == @widest
    end
  end

  test "two ids that truncate to the same prefix still differ by counter" do
    spent = String.duplicate("z", Identificator.limit(:operator) * 2)

    assert unique(spent, 11_111_111) != unique(spent, 22_222_222)
    assert Identificator.validate(:operator, unique(spent, 11_111_111)) == :ok
  end

  test "a runner id path is an absent file inside a directory this call made" do
    path = unique_id_path("harness_contract")

    refute File.exists?(path)
    assert File.dir?(Path.dirname(path))
    assert File.ls!(Path.dirname(path)) == []
  end
end
