defmodule FlezhaTest.Helper.MimicGlobal do
  @moduledoc """
  `Mimic.set_mimic_global/1` that releases the mode when the test exits.

  Mimic resets `:global` to `:private` only when it sees the owner go down,
  and it monitors the owner only once that owner registers a stub — so a
  global-mode test that stubs nothing leaves `:global` behind, and the next
  test to call `stub/3` from its own pid fails with "only the global owner is
  allowed". Releasing in `on_exit` makes the hand-over synchronous.
  """

  @spec set_mimic_global_until_exit(map()) :: :ok
  def set_mimic_global_until_exit(_context \\ %{}) do
    Mimic.set_mimic_global()
    ExUnit.Callbacks.on_exit(fn -> Mimic.set_mimic_private() end)
    :ok
  end
end
