defmodule FlezhaTest.Plugin.DurableInputTest do
  @moduledoc """
  "A source SHALL be told whether its operator's input is durable": the host
  option `durable_input` a plugin's activated options carry, from the
  operator's journal, over whatever the plugin's stored options said, and
  false on a shared copy, which answers every injection itself; and the
  commit cadence a pull source moves its position by, which that option
  decides (`Flezha.Plugin.Host.commit_position/3`).
  """

  use ExUnit.Case, async: true

  alias Flezha.Operator.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Host
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
      operator_id: "durable_input_op"
    }

  defp activated(overrides, stored \\ %{}) do
    state = OverwatchHost.state("durable_input_op", [channel: nil] ++ overrides)
    plugin = plugin("probe", stored)

    assert {:reply, :ok, _state} =
             Overwatch.handle_call({:plugin_subscribe, "probe", plugin, plugin}, self(), state)

    assert_receive {:activated, opts}
    opts
  end

  test "an operator that journals its input tells its plugins so" do
    assert activated(effectively_once: true).durable_input == true
    assert activated(sleeps: true).durable_input == true
    assert activated(durable_steps: true).durable_input == true
  end

  test "a plain flow operator on local disk does not" do
    assert activated([]).durable_input == false
  end

  test "a stored durable_input is overwritten by the operator's setting" do
    assert activated([], %{durable_input: true}).durable_input == false
  end

  test "a shared copy's plugins are told their input is not durable" do
    name = "shared_probe_#{System.unique_integer([:positive])}"
    plugin = %{plugin(name, %{}) | operator_id: nil}

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
end
