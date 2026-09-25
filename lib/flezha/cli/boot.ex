defmodule Flezha.Service.Cli.Boot do
  @moduledoc """
  `flz boot` — the readiness verdict, the preconditions behind it, and the
  bounded wait for it.

  Both verbs read `Flezha.Boot.Readiness` and neither re-derives readiness from
  hub state, so the CLI, `GET /readyz` and the container layer cannot disagree.
  `boot_status/1` keeps the per-hub recovery table, which is the detail a
  caller needs once a precondition has been named as unmet.
  """

  alias Flezha.Boot.Readiness
  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.HubManager
  alias ProcessHub.Service.Recovery

  @doc """
  `flz boot status` — the readiness verdict and every precondition behind it,
  then per-namespace recovery state and persistent-registry row count. Returns
  immediately; for blocking semantics use `boot_await/2`.
  """
  def boot_status(_call) do
    {verdict, rows} = Readiness.verdict()

    IO.puts("readiness: #{verdict}")

    Enum.each(rows, fn %{key: key, met: met} ->
      IO.puts("  #{if met, do: "met  ", else: "unmet"}  #{key}")
    end)

    IO.puts("")
    IO.puts("namespace                          recovery_state    registry_rows")
    IO.puts("----------------------------------------------------------------")

    Enum.each(HubManager.get_running_namespaces(), fn namespace ->
      hub_id = Identificator.hub(namespace)
      state = safe(fn -> Recovery.recovery_state(hub_id) end, :unknown)
      rows = safe(fn -> hub_id |> ProcessHub.registry_dump() |> Enum.count() end, 0)

      IO.puts(
        String.pad_trailing(namespace, 35) <>
          String.pad_trailing(Atom.to_string(state), 17) <> Integer.to_string(rows)
      )
    end)

    :ok
  end

  @doc """
  `flz boot await --timeout SECS` — block until the readiness verdict is
  `:ready`, or the timeout elapses. A timeout of zero evaluates the verdict
  once, which is what the image `HEALTHCHECK` runs.
  """
  def boot_await(timeout_ms, _call) when is_integer(timeout_ms) do
    case poll(System.monotonic_time(:millisecond) + timeout_ms) do
      {:ready, _rows} ->
        IO.puts("boot await: ready")
        :ok

      {verdict, rows} ->
        unmet = for %{key: key, met: false} <- rows, do: key

        # `halt!/1` and not `fail!/2`: the container HEALTHCHECK and the
        # readiness drill both read `boot await: <verdict>`, so the line carries
        # no `error: ` prefix.
        Context.halt!("boot await: #{verdict}; unmet: #{Enum.join(unmet, ", ")}")
    end
  end

  defp poll(deadline) do
    case Readiness.verdict() do
      {:ready, _rows} = ready ->
        ready

      unready ->
        poll_ms = Durations.get(:boot_await_poll_ms)

        if System.monotonic_time(:millisecond) + poll_ms > deadline do
          unready
        else
          Process.sleep(poll_ms)
          poll(deadline)
        end
    end
  end

  # A hub can be torn down between the listing and the read; a status table
  # reports what it could not read rather than exiting the CLI.
  defp safe(read, fallback) do
    read.()
  catch
    _, _ -> fallback
  end
end
