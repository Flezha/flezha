defmodule FlezhaTest.Plugin.DurableInputTest do
  @moduledoc """
  "A source SHALL be told whether its relay's input is durable": the host
  option `durable_input` a plugin's activated options carry, from the
  relay's journal, over whatever the plugin's stored options said, and
  false on a shared copy, which answers every injection itself; and the
  commit cadence a pull source moves its position by, which that option
  decides (`Flezha.Plugin.Host.commit_position/3`).
  """

  use ExUnit.Case, async: true

  alias Flezha.Relay.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Host.MsgId
  alias Flezha.Plugin.Shared
  alias FlezhaTest.Helper.OverwatchHost

  defmodule Probe do
    @moduledoc false
    use Flezha.Plugin.ProviderBehaviour, options: [report_to: [type: :any, internal: true]]

    @impl true
    def activate(opts) do
      send(opts.report_to, {:activated, opts})
      :ok
    end

    @impl true
    def on_input(ctx, _opts), do: {:ok, ctx}
  end

  defp plugin(id, opts),
    do: %Plugin{
      id: id,
      provider: Probe,
      opts: Map.put(opts, :report_to, self()),
      hooks: [on_input: []],
      namespace: "default",
      relay_id: "durable_input_op"
    }

  defp activated(overrides, stored \\ %{}) do
    state = OverwatchHost.state("durable_input_op", [channel: nil] ++ overrides)
    plugin = plugin("probe", stored)

    assert {:reply, :ok, _state} =
             Overwatch.handle_call({:plugin_subscribe, "probe", plugin, plugin}, self(), state)

    assert_receive {:activated, opts}
    opts
  end

  test "a relay that journals its input tells its plugins so" do
    assert activated(effectively_once: true).durable_input == true
    assert activated(sleeps: true).durable_input == true
    assert activated(durable_steps: true).durable_input == true
  end

  test "a plain flow relay on local disk does not" do
    assert activated([]).durable_input == false
  end

  test "a stored durable_input is overwritten by the relay's setting" do
    assert activated([], %{durable_input: true}).durable_input == false
  end

  test "a shared copy's plugins are told their input is not durable" do
    name = "shared_probe_#{System.unique_integer([:positive])}"
    plugin = %{plugin(name, %{}) | relay_id: nil}

    # The copy is started directly: the provider's declaration is not what it reads.
    start_supervised!({Shared, %{namespace: "durable_input_ns", plugin: plugin}})

    assert_receive {:activated, opts}
    assert opts.durable_input == false
  end

  describe "Host.commit_position/3" do
    # A pull source's state, its position moved past what it committed; the
    # test process stands in for the host, so a commit is a cast it receives.
    defp source(extra) do
      Map.merge(
        %{
          offset: 5,
          committed: 1,
          committed_at: :erlang.monotonic_time(:millisecond),
          commit_interval_ms: 60_000,
          overwatcher_pid: self(),
          source_plugin_id: "src"
        },
        extra
      )
    end

    test "a source reading on commits after every batch only when its input is durable" do
      plain = source(%{durable_input: false})
      assert Host.commit_position(plain, :offset, :more) == plain
      refute_received {:"$gen_cast", {:plugin_opt_persist, _, _, _}}

      durable = Host.commit_position(source(%{durable_input: true}), :offset, :more)
      assert durable.committed == 5
      assert_received {:"$gen_cast", {:plugin_opt_persist, "src", :offset, 5}}
    end

    test "a plain source reading on commits once commit_interval_ms has passed" do
      due = source(%{committed_at: :erlang.monotonic_time(:millisecond) - 60_000})

      assert Host.commit_position(due, :offset, :more).committed == 5
      assert_received {:"$gen_cast", {:plugin_opt_persist, "src", :offset, 5}}
    end

    test "a source that stops reading commits at once, and an unmoved position is not written again" do
      caught_up = Host.commit_position(source(%{}), :offset, :caught_up)
      assert caught_up.committed == 5
      assert_received {:"$gen_cast", {:plugin_opt_persist, "src", :offset, 5}}

      assert Host.commit_position(caught_up, :offset, :stopped) == caught_up
      refute_received {:"$gen_cast", {:plugin_opt_persist, _, _, _}}
    end
  end

  describe "Host.MsgId" do
    require MsgId

    test "a literal takes <plugin_id>/<position>, a list position joined once" do
      assert MsgId.stamp(%{"channel" => "in"}, "orders", "t/0/41") ==
               %{"channel" => "in", "flz.msg_id" => "orders/t/0/41"}

      assert MsgId.stamp(%{}, "orders", ["t" | [0, 41]]) == %{"flz.msg_id" => "orders/t/0/41"}
    end

    # Fails when: the id is put whatever the map holds.
    test "an id the sender set wins" do
      preset = %{"flz.msg_id" => "mine"}
      assert MsgId.stamp(preset, "orders", 41) == preset
      assert MsgId.stamp(%{"k" => "v"}, "orders", 41)["flz.msg_id"] == "orders/41"
    end

    test "stamp_some stamps no id for a record without a position" do
      assert MsgId.stamp_some(%{"k" => "v"}, "orders", nil) == %{"k" => "v"}
      assert MsgId.stamp_some(%{"k" => "v"}, "orders", 7)["flz.msg_id"] == "orders/7"
    end
  end

  describe "Host.fan_out/3" do
    test "an effectively-once relay is confirmed, a verdict source confirms every one" do
      once = Host.target(:once, "a", true)
      plain = Host.target(:plain, nil, false)

      assert Host.fan_out([once, plain], %{"k" => "v"}, :confirming) == [
               {:once, %{"k" => "v", "channel" => "a"}, :confirming},
               {:plain, %{"k" => "v"}, :fire_and_forget}
             ]

      assert [{:once, _, :verdict}, {:plain, _, :verdict}] =
               Host.fan_out([once, plain], %{}, :verdict)

      assert [{:once, _, :fire_and_forget}, {:plain, _, :fire_and_forget}] =
               Host.fan_out([once, plain], %{}, :fire_and_forget)
    end
  end
end
