defmodule FlezhaTest.Operative.OverwatchOutputStampPassthroughTest do
  @moduledoc """
  Output-identity pass-through: the SDK stamps `flz.segment` / `flz.seq` /
  `flz.key` on keyed-channel frames, and the controller must deliver them
  to the `:on_output` plugin chain unchanged — StreamChannel routes the
  frame's metadata as-is and Overwatch hands it to the plugin ctx as-is.
  """
  use ExUnit.Case, async: true

  alias Flezha.Executor.StreamChannel
  alias Flezha.Operative.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table

  defmodule ProbeProvider do
    def init_provider(opts), do: {:ok, opts}
    def activate(_opts), do: :ok

    def on_output(ctx, opts) do
      send(Map.fetch!(opts, :report_to), {:on_output_observed, ctx.message, ctx.metadata})
      {:ok, ctx}
    end
  end

  @stamped_metadata %{
    "channel" => "out",
    "flz.segment" => "operative-1/ohlc_operation/BTC",
    "flz.seq" => "42.1",
    "flz.key" => "BTC/1m/1720000000",
    "user_key" => "user_value"
  }

  test "flz.* stamps flow unchanged through StreamChannel and the :on_output chain" do
    message = "candle-bytes"

    # Leg 1 — StreamChannel: a secondary-stream data frame for a mapped
    # operative is cast to the Overwatch pid with metadata untouched.
    channel_state = %{
      operative_mapping: %{"operative-1" => [self()]},
      last_seen: 0,
      liveness_ref: nil
    }

    assert {:noreply, _state} =
             StreamChannel.handle_info(
               {:flz_data, 7, "operative-1", message, @stamped_metadata},
               channel_state
             )

    assert_receive {:"$gen_cast", {:io_out, ^message, forwarded_metadata}}
    assert forwarded_metadata == @stamped_metadata

    # Leg 2 — Overwatch: the forwarded frame reaches the :on_output plugin
    # chain with the same metadata map.
    state = %Overwatch{
      id: "operative-1",
      namespace: "default",
      executor_id: "exec-1",
      factory_id: "test_factory",
      channel: nil,
      extensions: Table.new(Overwatch.__extensions_catalog__()),
      builder: nil
    }

    plugin = %Plugin{
      id: "stamp_probe_plugin",
      provider: ProbeProvider,
      opts: %{report_to: self()},
      subscriptions: [on_output: []],
      namespace: "default",
      operative_id: "operative-1"
    }

    {:reply, :ok, state_with_plugin} =
      Overwatch.handle_call(
        {:plugin_subscribe, "stamp_probe_plugin", plugin},
        {self(), make_ref()},
        state
      )

    assert {:noreply, _} =
             Overwatch.handle_cast({:io_out, message, forwarded_metadata}, state_with_plugin)

    assert_receive {:on_output_observed, ^message, observed_metadata}

    assert observed_metadata == @stamped_metadata,
           "the :on_output plugin chain must observe the flz.* stamps unchanged; " <>
             "got #{inspect(observed_metadata)}"
  end
end
