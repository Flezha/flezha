defmodule FlezhaTest.Helper.DlqFixtures do
  @moduledoc """
  Fixtures for driving the dead-letter queue's operator verbs.
  """

  alias Flezha.Dlq.Store

  @doc """
  An overwatch stand-in that re-parks whatever input it is handed, then tells
  `parent` it did.

  A stand-in for the runner's retry-then-park cycle: the replayed input comes
  back to the store under the same content identity, which is what makes the
  store bump `generation` instead of creating a second entry. `parent` receives
  `:re_parked` once.
  """
  @spec spawn_reparking_overwatch(map(), pid()) :: pid()
  def spawn_reparking_overwatch(entry, parent) when is_map(entry) and is_pid(parent) do
    spawn_link(fn ->
      receive do
        {:"$gen_cast", {:io_in, payload, _metadata}} ->
          {:ok, _ref} = Store.append(%{entry | payload: payload, attempts: 3})
          send(parent, :re_parked)
      end
    end)
  end
end
