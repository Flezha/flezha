defmodule Flezha.Service.PluginHelper do
  @moduledoc """
  High-level helpers for common plugin operations.
  Designed for programmatic callers (UI, API) — returns tagged tuples
  instead of printing to stdout.
  """

  alias Flezha.Service.PluginManager
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Plugin.Driver.IOBindingPlugin

  @doc """
  Enables a MonitoredIOForward plugin that streams processor I/O to `recv_pid`.

  Returns `{:ok, plugin_id}` on success or `{:error, reason}` on failure.

  ## Parameters
    - `namespace` — processor namespace (e.g. "default")
    - `processor_id` — processor to watch
    - `io_type` — "input" or "output"
    - `recv_pid` — PID that will receive `{:message_forward, {message, metadata}}`
                    and `{:heartbeat_keepalive, pid}` messages
  """
  def enable_io_listener(namespace, processor_id, io_type, recv_pid) do
    trigger =
      case io_type do
        "input" -> :on_input
        "output" -> :on_output
        _ -> {:error, "Invalid io_type. Use \"input\" or \"output\"."}
      end

    with trigger when is_atom(trigger) <- trigger do
      plugin_id =
        "io_listener_#{:rand.uniform(999_999) |> Integer.to_string() |> String.pad_leading(6, "0")}"

      provider = %MonitoredIOForward{
        recv_pid: recv_pid,
        plugin_id: plugin_id,
        processor_id: processor_id,
        namespace: namespace
      }

      plugin = %IOBindingPlugin{
        provider: provider,
        namespace: namespace,
        processor_id: processor_id,
        trigger: trigger
      }

      case PluginManager.enable_plugin(plugin_id, plugin) do
        {:ok, _msg} -> {:ok, plugin_id}
        {:error, reason} -> {:error, reason}
      end
    end
  end
end
