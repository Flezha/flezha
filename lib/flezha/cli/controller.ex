defmodule Flezha.Service.Cli.Controller do
  @moduledoc """
  `flz controller stop | status` — the stop's progress lines and outcome, and
  the controller status block.

  The stop's final line is a machine-readable marker the Rust CLI reads,
  because the node may be gone before the call's own exit reaches it:

      flz-controller-stop-result: clean | partial | forced
  """

  alias Flezha.Service.Admin
  alias Flezha.Service.Cli
  alias Flezha.Service.Cli.Context

  @doc """
  `flz controller stop`: drain and stop, printing each phase on the caller's
  stderr as it happens and the outcome, then the marker, on its stdout.
  """
  def stop(args_json, context, opts) do
    # The phase lines are emitted by the shutdown coordinator, not by this
    # process, so the device is resolved here, on the way in.
    notify = Context.notifier()
    emit = &report(&1, notify, opts)

    case Admin.run("Controller.stop", args_json, Map.put(context, :emit, emit), :given) do
      {:ok, %{outcome: :dispatched}} ->
        IO.puts("[shutdown] dispatched (no-wait)")
        IO.puts("flz-controller-stop-result: clean")

      {:ok, _stopped} ->
        :ok

      refused ->
        Cli.render(refused, "Controller.stop", args_json, opts)
    end
  end

  defp report({:progress, event, measurements, metadata}, notify, _opts) do
    case progress_line(event, measurements, metadata) do
      nil -> :ok
      line -> notify.(line)
    end
  end

  defp report({:stopped, answer, duration_ms}, _notify, opts) do
    case {answer, Context.format(opts)} do
      {{:ok, stopped}, "json"} ->
        IO.puts(Admin.json(stopped))

      {{:ok, %{already_stopping: true}}, _text} ->
        IO.puts("[shutdown] coordinator not running; the controller is already stopping")

      {{:ok, %{outcome: outcome}}, _text} ->
        IO.puts("[shutdown] complete duration_ms=#{duration_ms} outcome=#{outcome}")

      {{:error, {:partial_drain, still_local}}, _format} ->
        IO.puts(
          "[shutdown] complete duration_ms=#{duration_ms} outcome=partial " <>
            "still_local=#{length(still_local)}"
        )

      {{:error, _reason}, _format} ->
        :ok
    end

    if marker = marker(answer), do: IO.puts("flz-controller-stop-result: #{marker}")
  end

  # Any other failure prints no marker: the stop's own refusal is rendered
  # once it returns, and `flz` settles on its code.
  defp marker({:ok, %{outcome: outcome}}), do: outcome
  defp marker({:error, {:partial_drain, _still_local}}), do: :partial
  defp marker({:error, _reason}), do: nil

  defp progress_line([:flezha, :shutdown, :phase_started], _measurements, metadata),
    do: "[shutdown] phase=#{metadata.phase} started"

  defp progress_line([:flezha, :shutdown, :phase_completed], measurements, metadata),
    do:
      "[shutdown] phase=#{metadata.phase} completed duration_ms=#{measurements[:duration_ms] || 0}"

  defp progress_line([:flezha, :shutdown, :drain_progress], measurements, _metadata),
    do:
      "drain: migrated=#{measurements.migrated} remaining=#{measurements.remaining} " <>
        "elapsed_ms=#{measurements.elapsed_ms}"

  defp progress_line([:flezha, :shutdown, :timeout], _measurements, metadata),
    do: "[shutdown] timeout phase=#{metadata.phase} still_local=#{length(metadata.still_local)}"

  defp progress_line(_event, _measurements, _metadata), do: nil

  @doc "`flz controller status`."
  def status(status, _args, _opts) do
    IO.puts("node:                          #{status.node}")
    IO.puts("shutdown_in_progress:          #{status.shutdown_in_progress}")
    IO.puts("shutdown_phase:                #{inspect(status.shutdown_phase)}")
    IO.puts("signal_handler_status:         #{status.signal_handler_status}")
    IO.puts("graceful_shutdown_enabled:     #{status.graceful_shutdown_enabled}")
    IO.puts("graceful_shutdown_timeout_ms:  #{status.graceful_shutdown_timeout_ms}")
    IO.puts("durability_backend:            #{status.durability_backend}")
    IO.puts("durability_posture:            #{status.durability_posture}")
    IO.puts("durability_rpo_breaches:       #{status.durability_rpo_breaches}")
    IO.puts("durability_levels:             #{levels_line(status.durability_levels)}")
    IO.puts("durability_superseded:         #{status.durability_superseded}")
    IO.puts("state_root:                    #{status.state_root}")
    IO.puts("file_plugin_root:              #{status.file_plugin_root}")
  end

  defp levels_line(levels) when map_size(levels) == 0, do: "none"

  defp levels_line(levels),
    do: levels |> Enum.sort() |> Enum.map_join(" ", fn {class, level} -> "#{class}=#{level}" end)
end
