defmodule Flezha.State.TestHelpersTest do
  @moduledoc """
  `tmp_app_data_dir/0` hands back a state root nothing else owns — the
  precondition every persistence module's `setup` rests on.
  """
  # async: false: writes the :state_root application env.
  use ExUnit.Case, async: false

  alias Flezha.State.TestHelpers

  test "a state root a previous BEAM left behind is never handed out again" do
    # `System.unique_integer([:positive])` restarts low in every BEAM, so run
    # N+1 re-mints run N's names. Claiming the whole band the next mint can land
    # in forces that collision rather than waiting for one.
    counter = System.unique_integer([:positive])
    band = 512

    planted =
      for i <- 1..band do
        dir = Path.join(System.tmp_dir!(), "flezha_state_test_#{counter + i}")
        File.mkdir_p!(dir)
        File.write!(Path.join(dir, "left_behind"), "")
        dir
      end

    on_exit(fn -> Enum.each(planted, &File.rm_rf/1) end)

    base = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf(base)
    end)

    refute base in planted
    assert File.ls!(base) == []

    # And the band really did cover the mint: the name handed back is past every
    # claimed one, so the walk happened. Without this the two assertions above
    # could hold over a collision that never occurred.
    minted =
      base
      |> Path.basename()
      |> String.replace_prefix("flezha_state_test_", "")
      |> String.to_integer()

    assert minted > counter + band, "the planted band did not cover the mint"
  end
end
