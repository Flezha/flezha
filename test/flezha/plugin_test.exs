defmodule Flezha.PluginTest do
  @moduledoc """
  Contract tests for `Flezha.Plugin`.

  Guards the cspec-persistence contract: anything that survives
  `Plugin.strip_transient/1` must round-trip cleanly through
  `:erlang.term_to_binary/1` because it ends up embedded in the
  ProcessHub child-spec via
  `Flezha.Service.HubManager.update_relay_plugins/3`. If a future
  field on `%Flezha.Plugin{}` holds a live pid or ref and is not
  dropped by `strip_transient/1`, it will resurface as a stale
  reference on the next Overwatch (re)start.
  """

  use ExUnit.Case, async: true

  alias Flezha.Plugin

  describe "strip_transient/1" do
    test "round-trips through term_to_binary/binary_to_term unchanged" do
      # If this fails, plugin entries persisted into the ProcessHub
      # cspec via Flezha.Service.HubManager.update_relay_plugins/3
      # will carry transient state (pids, refs) that is invalid after
      # any BEAM restart, breaking the plugin-subscription-persistence
      # contract.
      plugin =
        Plugin.new(
          id: "sink_p1_trades",
          provider: __MODULE__.NoopProvider,
          opts: %{topic: "p1.trades.v1", cell: 0},
          hooks: [{:on_output, []}],
          namespace: "default",
          relay_id: "p1"
        )

      stripped = Plugin.strip_transient(plugin)
      round_tripped = stripped |> :erlang.term_to_binary() |> :erlang.binary_to_term()

      assert round_tripped == stripped
    end
  end

  describe "new/1 from the shapes a wire carries" do
    # An application's enable reaches here as decoded JSON, so its options carry
    # text keys, while a provider declares its options as atoms.
    test "reads an option the provider declares, and leaves any other key as written" do
      plugin =
        Plugin.new(
          id: "writer",
          provider: "relay_file_sink",
          opts: %{"path" => "echo-out.txt", "not_an_option" => 1}
        )

      assert plugin.provider == Flezha.Plugin.Provider.File.Sink
      assert plugin.opts == %{:path => "echo-out.txt", "not_an_option" => 1}
    end
  end

  defmodule NoopProvider do
    @moduledoc false
  end
end
