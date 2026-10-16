defmodule FlezhaTest.Helper.DlqFixtures do
  @moduledoc """
  Fixtures for driving the dead-letter queue's administration verbs.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Flezha.Dlq.Store
  alias FlezhaTest.Helper.NodeTestHelper

  @doc """
  Point `:dlq` at a per-test tmp directory and register its cleanup. `opts` are
  merged into the `:dlq` config (`replay_grace_ms: 400`, `retention_seconds: 0`
  and the like); the directory is returned for a test that asserts on disk.

  `Flezha.Dlq.Store` reads its root per call, so this repoints the store the
  application tree already runs as well as any the test starts itself.
  """
  @spec isolated_dir(keyword()) :: String.t()
  def isolated_dir(opts \\ []) do
    dir = NodeTestHelper.claim_tmp_dir("flz_dlq")
    previous = Application.get_env(:flezha, :dlq, [])
    Application.put_env(:flezha, :dlq, Keyword.merge(previous, [dir: dir] ++ opts))

    on_exit(fn ->
      Application.put_env(:flezha, :dlq, previous)
      File.rm_rf!(dir)
    end)

    dir
  end

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
          {:existing, _ref} = Store.append(%{entry | payload: payload, attempts: 3})
          send(parent, :re_parked)
      end
    end)
  end
end
