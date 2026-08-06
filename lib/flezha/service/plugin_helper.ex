defmodule Flezha.Service.PluginHelper do
  @moduledoc """
  High-level helpers for common plugin procedures.
  Designed for programmatic callers (UI, API) — returns tagged tuples
  instead of printing to stdout.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Service.PluginManager

  @doc """
  Enables a MonitoredIOForward plugin that streams operative I/O to `recv_pid`.

  Returns `{:ok, plugin_id}` on success or `{:error, reason}` on failure.
  """
  def enable_io_listener(namespace, operative_id, io_type, recv_pid) do
    extension =
      case io_type do
        "input" -> :on_input
        "output" -> :on_output
        _ -> {:error, "Invalid io_type. Use \"input\" or \"output\"."}
      end

    with extension when is_atom(extension) <- extension do
      plugin_id =
        "io_listener_#{:rand.uniform(999_999) |> Integer.to_string() |> String.pad_leading(6, "0")}"

      plugin = %Plugin{
        id: plugin_id,
        provider: MonitoredIOForward,
        namespace: namespace,
        operative_id: operative_id,
        opts: %{
          recv_pid: recv_pid,
          plugin_id: plugin_id,
          operative_id: operative_id,
          namespace: namespace
        },
        subscriptions: [{extension, []}]
      }

      case PluginManager.enable_plugin(plugin_id, plugin) do
        {:ok, _msg} -> {:ok, plugin_id}
        {:error, reason} -> {:error, reason}
      end
    end
  end
end
