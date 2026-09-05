defmodule Flezha.Process.Hub do
  @moduledoc """
  The one guarded seam for reaching a ProcessHub coordinator.

  `ProcessHub.is_alive?/1` is `Process.whereis/1`, which answers `true` for a
  coordinator that is terminating — so the check alone is not the guarantee
  and the call itself must survive the hub going away underneath it. Every
  hub read and every hub call runs through `with_live/3`, which answers the
  caller's declared value either way: a namespace hub dying may not take a
  node-wide process with it.
  """

  require Logger

  @type hub_id :: atom()

  @doc "True while `hub_id`'s coordinator is registered."
  @spec alive?(hub_id()) :: boolean()
  def alive?(hub_id) when is_atom(hub_id), do: ProcessHub.is_alive?(hub_id)

  @doc """
  Run `fun` with `hub_id` when that hub is alive, else answer `on_dead` — and
  answer `on_dead` too when the hub goes away mid-call, which is the case the
  liveness check by itself cannot cover.
  """
  @spec with_live(hub_id(), term(), (hub_id() -> term())) :: term()
  def with_live(hub_id, on_dead, fun) when is_atom(hub_id) do
    if ProcessHub.is_alive?(hub_id), do: fun.(hub_id), else: on_dead
  catch
    kind, reason when kind in [:exit, :error] ->
      :telemetry.execute(
        [:flezha, :hub, :unavailable],
        %{count: 1},
        %{hub_id: hub_id, kind: kind, reason: reason}
      )

      Logger.warning(
        "[hub_manager] hub #{inspect(hub_id)} went away mid-call " <>
          "(#{kind} #{inspect(reason)}); answering #{inspect(on_dead)}"
      )

      on_dead
  end
end
