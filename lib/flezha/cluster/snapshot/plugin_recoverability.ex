defmodule Flezha.Cluster.Snapshot.PluginRecoverability do
  @moduledoc """
  `recoverable?/1` predicate used by `Flezha.Cluster.Snapshot` to decide
  whether a plugin survives a snapshot/restore cycle.

  A plugin is recoverable iff its provider does not bind to per-process
  state that cannot survive a BEAM restart. Today the non-recoverable
  list is the two providers that hold a caller pid or a live socket ref:

    * `Flezha.Plugin.Provider.BeamMsgForward` — caller pid baked in.
    * `Flezha.Plugin.Provider.MonitoredIOForward` — monitor ref to a pid.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.BeamMsgForward
  alias Flezha.Plugin.Provider.MonitoredIOForward

  @non_recoverable [BeamMsgForward, MonitoredIOForward]

  @spec recoverable?(Plugin.t() | term()) :: boolean()
  def recoverable?(%Plugin{provider: provider}), do: provider not in @non_recoverable
  def recoverable?(_), do: false
end
