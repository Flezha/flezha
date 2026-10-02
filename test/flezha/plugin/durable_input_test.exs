defmodule FlezhaTest.Plugin.DurableInputTest do
  @moduledoc """
  "A source SHALL be told whether its operator's input is durable": the host
  option `durable_input` a plugin's activated options carry, from the
  operator's journal, over whatever the plugin's stored options said, and
  false on a shared copy, which answers every injection itself.
  """

  use ExUnit.Case, async: true

  alias Flezha.Operator.Overwatch
  alias Flezha.Plugin
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
end
