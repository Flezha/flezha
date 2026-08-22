defmodule FlezhaTest.Operative.ReplayAdoptionTest do
  @moduledoc """
  `Replay.dispatch_restore/3` adopting an operative's durable state from
  another controller node.

  A partition rescale gathers the whole group's Entities onto the node that ran
  it and stamps that node on every new member's opts. ProcessHub then places the
  members wherever it likes, so a member that lands elsewhere has to pull its
  class tree across before it restores — otherwise it cold-starts on top of
  state that exists, just not here. Only a FOREIGN node means anything: a local
  value is already where the ordinary restore looks, and pulling from a node
  that no longer exists on every bind would be noise.
  """
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Operative.Builder
  alias Flezha.Operative.Overwatch
  alias Flezha.Operative.Replay
  alias Flezha.State.TestHelpers
  alias Flezha.State.WalTransfer

  setup :set_mimic_global_until_exit

  setup do
    root = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf(root)
    end)

    :ok
  end

  defp state(operative_id) do
    %Overwatch{
      id: operative_id,
      namespace: "adoption-test",
      factory_id: "candles",
      runner_id: "runner-1",
      builder: %Builder{id: operative_id, factory_id: "candles", kind: :stream}
    }
  end

  # `dispatch_restore/3` does its work off-process; the stub reports back so the
  # test observes the pull rather than racing the spawn.
  defp expect_pull(reply_to) do
    stub(WalTransfer, :pull_operative, fn source_node, operative_class ->
      send(reply_to, {:pulled, source_node, operative_class})
      0
    end)
  end

  test "a member placed off the assembling node pulls its class tree first" do
    expect_pull(self())
    peer = :"peer@127.0.0.1"

    :ok = Replay.dispatch_restore(state("ohlc#p1"), [state_source_node: peer], "runner-1")

    assert_receive {:pulled, ^peer, "ohlc#p1"}, 2_000
  end

  test "the operative_class opt names what is pulled, not the instance id" do
    expect_pull(self())
    peer = :"peer@127.0.0.1"

    :ok =
      Replay.dispatch_restore(
        state("ohlc#p1"),
        [state_source_node: peer, operative_class: "shared_class"],
        "runner-1"
      )

    assert_receive {:pulled, ^peer, "shared_class"}, 2_000
  end

  test "a member placed on the assembling node pulls nothing" do
    expect_pull(self())

    :ok = Replay.dispatch_restore(state("ohlc#p0"), [state_source_node: node()], "runner-1")

    refute_receive {:pulled, _, _}, 300
  end

  test "an operative that was never rescaled pulls nothing" do
    expect_pull(self())

    :ok = Replay.dispatch_restore(state("plain"), [], "runner-1")

    refute_receive {:pulled, _, _}, 300
  end
end
