defmodule Flezha.Service.Cli.Boot do
  @moduledoc """
  `flz boot` — boot status and the bounded await.
  """

  alias Flezha.Identificator
  alias Flezha.Service.HubManager
  alias ProcessHub.Service.Recovery

  @doc """
  `flz boot status` — per-namespace recovery state and persistent-registry
  row count. Returns immediately; for blocking semantics use `boot_await/1`.
  """
  def boot_status() do
    rows =
      HubManager.get_running_namespaces()
      |> Enum.map(fn namespace ->
        hub_id = Identificator.hub(namespace)
        recovery_state = safe_recovery_state(hub_id)
        registry_count = safe_registry_count(hub_id)

        %{namespace: namespace, recovery_state: recovery_state, registry_rows: registry_count}
      end)

    IO.puts("namespace                          recovery_state    registry_rows")
    IO.puts("----------------------------------------------------------------")

    Enum.each(rows, fn row ->
      IO.puts(
        String.pad_trailing(row.namespace, 35) <>
          String.pad_trailing(Atom.to_string(row.recovery_state), 17) <>
          Integer.to_string(row.registry_rows)
      )
    end)

    :ok
  end

  defp safe_recovery_state(hub_id) do
    try do
      Recovery.recovery_state(hub_id)
    catch
      _, _ -> :unknown
    end
  end

  defp safe_registry_count(hub_id) do
    try do
      hub_id |> ProcessHub.registry_dump() |> Enum.count()
    catch
      _, _ -> 0
    end
  end

  @doc """
  `flz boot await --timeout SECS` — block until every namespace hub
  reports `:normal`, or the per-hub timeout elapses.
  """
  def boot_await(timeout_ms \\ 60_000) when is_integer(timeout_ms) do
    namespaces = HubManager.get_running_namespaces()

    failures =
      Enum.flat_map(namespaces, fn namespace ->
        hub_id = Identificator.hub(namespace)

        try do
          case Recovery.await_normal(hub_id, timeout_ms) do
            :ok -> []
            {:error, reason} -> [{namespace, reason}]
            other -> [{namespace, other}]
          end
        catch
          kind, reason -> [{namespace, {kind, reason}}]
        end
      end)

    case failures do
      [] ->
        IO.puts("boot await: all namespaces reached :normal")
        :ok

      _ ->
        Enum.each(failures, fn {ns, reason} ->
          IO.puts(:stderr, "boot await: #{ns}: #{inspect(reason)}")
        end)

        exit(:cli_error)
    end
  end
end
