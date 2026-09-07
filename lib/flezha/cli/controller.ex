defmodule Flezha.Service.Cli.Controller do
  @moduledoc """
  `flz controller stop | status` — the graceful-shutdown drive with its progress
  handler, and the controller status block.
  """

  alias Flezha.Config
  alias Flezha.Service.Cli.Context

  alias Flezha.Config.Durations
  alias Flezha.Shutdown.Coordinator
  alias Flezha.State.DurabilityTier
  alias Flezha.State.Paths

  @doc """
  `flz controller stop` RPC. Drives `Flezha.Shutdown.Coordinator.shutdown/2`
  and prints structured progress lines. The final line is a
  machine-readable marker the Rust CLI scans to derive its exit code:

      flz-controller-stop-result: clean | partial | forced | already_in_progress

  The call's options (keys may be atoms or binaries):
    * `:timeout_ms` — overrides the configured timeout
    * `:no_wait` — fire-and-forget; print marker `clean` and return immediately
    * `:force` — skip quiesce + drain, finalise only
  """
  def controller_stop(%{opts: opts}) do
    do_controller_stop(normalize_controller_stop_opts(opts))
  end

  defp do_controller_stop(opts_map) do
    force = Map.get(opts_map, :force, false)

    shutdown_opts =
      []
      |> maybe_put(:timeout_ms, Map.get(opts_map, :timeout_ms))
      |> maybe_put(:force, force || nil)

    if Map.get(opts_map, :no_wait, false) do
      _ = Task.start(fn -> Coordinator.shutdown(:cli, shutdown_opts) end)
      IO.puts("[shutdown] dispatched (no-wait)")
      IO.puts("flz-controller-stop-result: clean")
      :ok
    else
      with_progress_handler(fn ->
        start_ms = System.monotonic_time(:millisecond)

        try do
          render_stop_outcome(Coordinator.shutdown(:cli, shutdown_opts), force, start_ms)
        catch
          :exit, reason -> render_stop_exit(reason)
        end
      end)
    end
  end

  # The phase lines the operator watches come from an attached handler, so the
  # detach must survive every exit from the drive — including a coordinator
  # that is already gone — and the way to reach the operator is bound here
  # rather than in the handler, which runs in whichever process emitted the
  # event.
  defp with_progress_handler(drive) do
    handler_id = {__MODULE__, :controller_stop_progress, make_ref()}

    :ok =
      :telemetry.attach_many(
        handler_id,
        [
          [:flezha, :shutdown, :phase_started],
          [:flezha, :shutdown, :phase_completed],
          [:flezha, :shutdown, :drain_progress],
          [:flezha, :shutdown, :timeout]
        ],
        &__MODULE__.controller_stop_progress_handler/4,
        %{caller: self(), notify: Context.notifier()}
      )

    try do
      drive.()
    after
      :telemetry.detach(handler_id)
    end
  end

  defp render_stop_outcome(:ok, force, start_ms) do
    duration = System.monotonic_time(:millisecond) - start_ms
    outcome = if force, do: :forced, else: :clean

    IO.puts("[shutdown] complete duration_ms=#{duration} outcome=#{outcome}")

    IO.puts("flz-controller-stop-result: #{outcome}")
    :ok
  end

  defp render_stop_outcome({:partial, still_local}, _force, start_ms) do
    duration = System.monotonic_time(:millisecond) - start_ms

    IO.puts(
      "[shutdown] complete duration_ms=#{duration} outcome=partial still_local=#{length(still_local)}"
    )

    IO.puts("flz-controller-stop-result: partial")
    :ok
  end

  defp render_stop_outcome(other, _force, _start_ms) do
    IO.puts("[shutdown] unexpected coordinator return: #{inspect(other)}")
    IO.puts("flz-controller-stop-result: partial")
    :ok
  end

  defp render_stop_exit({:noproc, _}) do
    IO.puts("[shutdown] coordinator not running")
    IO.puts("flz-controller-stop-result: already_in_progress")
    :ok
  end

  defp render_stop_exit(reason) do
    IO.puts("[shutdown] coordinator exit: #{Context.format_reason(reason)}")
    IO.puts("flz-controller-stop-result: already_in_progress")
    :ok
  end

  @doc false
  def controller_stop_progress_handler(event, measurements, metadata, config) do
    notify = Map.fetch!(config, :notify)

    case event do
      [:flezha, :shutdown, :phase_started] ->
        notify.("[shutdown] phase=#{metadata.phase} started")

      [:flezha, :shutdown, :phase_completed] ->
        notify.(
          "[shutdown] phase=#{metadata.phase} completed duration_ms=#{measurements[:duration_ms] || 0}"
        )

      [:flezha, :shutdown, :drain_progress] ->
        notify.(
          "drain: migrated=#{measurements.migrated} remaining=#{measurements.remaining} elapsed_ms=#{measurements.elapsed_ms}"
        )

      [:flezha, :shutdown, :timeout] ->
        notify.(
          "[shutdown] timeout phase=#{metadata.phase} still_local=#{length(metadata.still_local)}"
        )

      _ ->
        :ok
    end
  rescue
    _ -> :ok
  end

  defp normalize_controller_stop_opts(opts) when is_list(opts) do
    Enum.into(opts, %{}, fn
      {k, v} when is_atom(k) -> {k, v}
      {k, v} when is_binary(k) -> {String.to_atom(k), v}
    end)
  end

  defp maybe_put(list, _k, nil), do: list
  defp maybe_put(list, _k, false), do: list
  defp maybe_put(list, k, v), do: Keyword.put(list, k, v)

  @doc """
  `flz controller status` RPC. Prints a human-readable status block
  including graceful-shutdown state and configuration knobs.

  The resolved state root is one of them: it is where this controller's durable
  state actually is, and reading it here is what stops the answer depending on
  someone having watched the boot that announced it.
  """
  def controller_status(output_format, _call) do
    status =
      safe_shutdown_status()
      |> Map.merge(durability_status())
      |> Map.put(:state_root, Paths.state_root())

    case output_format do
      "json" ->
        json =
          status
          |> Map.put(:node, Atom.to_string(node()))
          |> jsonable()
          |> Jason.encode!(pretty: true)

        IO.puts(json)

      _ ->
        IO.puts("node:                          #{node()}")
        IO.puts("shutdown_in_progress:          #{status.shutdown_in_progress}")
        IO.puts("shutdown_phase:                #{inspect(status.shutdown_phase)}")
        IO.puts("signal_handler_status:         #{status.signal_handler_status}")
        IO.puts("graceful_shutdown_enabled:     #{status.graceful_shutdown_enabled}")
        IO.puts("graceful_shutdown_timeout_ms:  #{status.graceful_shutdown_timeout_ms}")
        IO.puts("durability_backend:            #{status.durability_backend}")
        IO.puts("durability_posture:            #{status.durability_posture}")
        IO.puts("durability_rpo_breaches:       #{status.durability_rpo_breaches}")
        IO.puts("durability_superseded:         #{status.durability_superseded}")
        IO.puts("state_root:                    #{status.state_root}")
    end

    :ok
  end

  # Remote durability tier, as an operator reads it: which backend is
  # configured, how many references are past their RPO target, and how many are
  # now shipped by another controller. A stalled shipper must be visible here
  # rather than only by inspecting the backend.
  #
  # A superseded count is not an alarm on its own — a handover produces one —
  # but a count that keeps climbing on a node that should still own its
  # operatives is how a placement problem shows up here.
  defp durability_status() do
    %{
      durability_backend: DurabilityTier.configured_backend(),
      # `local_only` is a supported posture, not a misconfiguration — but it is
      # one a fleet audit has to be able to find, so it is reported rather than
      # left to be inferred from the backend name.
      durability_posture: DurabilityTier.posture(),
      durability_rpo_breaches: length(DurabilityTier.breaches()),
      durability_superseded: Enum.count(DurabilityTier.status(), &(&1.superseded_by != nil))
    }
  end

  defp safe_shutdown_status() do
    try do
      Coordinator.status()
    catch
      :exit, _ ->
        %{
          shutdown_in_progress: false,
          shutdown_phase: nil,
          signal_handler_status: :unknown,
          graceful_shutdown_enabled: Config.get(:graceful_shutdown_enabled),
          graceful_shutdown_timeout_ms: Durations.get(:graceful_shutdown_timeout_ms)
        }
    end
  end

  defp jsonable(map) when is_map(map) do
    Enum.into(map, %{}, fn
      {k, v} when is_atom(v) and not is_boolean(v) and not is_nil(v) -> {k, Atom.to_string(v)}
      {k, v} -> {k, v}
    end)
  end
end
