defmodule Flezha.Service.Controller.ReservedPortsTest do
  @moduledoc """
  The ports the controller's own surfaces hold, which a plugin that listens
  leaves alone: the health port, and the `port` of each plugin enabled in
  `_system`, defaulted as its provider declares.
  """
  # async: false: stubs HubManager globally.
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Plugin
  alias Flezha.Service.Controller
  alias Flezha.Service.HubManager

  defmodule Listening do
    @moduledoc false
    use Flezha.Plugin.ProviderBehaviour,
      name: "listening",
      options: [port: [type: :int, default: 8765, doc: "The port"]]

    @impl true
    def on_output(ctx, _opts), do: {:ok, ctx}
  end

  defmodule Quiet do
    @moduledoc false
    use Flezha.Plugin.ProviderBehaviour,
      name: "quiet",
      options: [url: [type: :string, doc: "u"]]

    @impl true
    def on_output(ctx, _opts), do: {:ok, ctx}
  end

  setup :verify_on_exit!

  test "the health port, and each _system plugin's port by its provider's name" do
    entries = [
      %Plugin{id: "api", provider: Listening, opts: %{}},
      %Plugin{id: "api2", provider: Listening, opts: %{port: 9000}},
      %Plugin{id: "out", provider: Quiet, opts: %{url: "http://x"}}
    ]

    stub(HubManager, :on_hub_node, fn "_system", {HubManager, :plugin_entries}, ["_system"], _ ->
      entries
    end)

    healthz = Flezha.Config.get(:healthz_port)

    assert Controller.reserved_ports() == [
             {healthz, "FLEZHA_HEALTHZ_PORT"},
             {8765, "listening"},
             {9000, "listening"}
           ]
  end

  test "without a _system hub, only the health port" do
    stub(HubManager, :on_hub_node, fn _, _, _, none -> {:error, none} end)
    assert [{_port, "FLEZHA_HEALTHZ_PORT"}] = Controller.reserved_ports()
  end
end
