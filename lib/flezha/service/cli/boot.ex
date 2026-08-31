defmodule Flezha.Service.Cli.Boot do
  @moduledoc """
  `flz boot` — boot status, the bounded await, and the legacy snapshot import.
  """

  alias Flezha.Identificator
  alias Flezha.Service.Cli.Context
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

  @doc """
  `flz boot import-legacy-snapshot <file> [--force]` — read a legacy `.flz`
  ETF snapshot and write each operative's child spec into ProcessHub's
  persistent registry via `start_children/3`. Refuses to import on a
  non-empty hub unless `force?` is `true`.
  """
  def boot_import_legacy_snapshot(path, force? \\ false) do
    with {:ok, blob} <- File.read(path),
         {:ok, decoded} <- safe_binary_to_term(blob),
         {:ok, payload} <- validate_legacy_payload(decoded),
         :ok <- ensure_force_or_empty(payload, force?) do
      run_legacy_import(payload)
    else
      {:error, reason} ->
        Context.cli_error!("boot import-legacy-snapshot: #{inspect(reason)}", :stderr)
    end
  end

  defp safe_binary_to_term(blob) do
    try do
      {:ok, :erlang.binary_to_term(blob, [:safe])}
    rescue
      _ -> {:error, :corrupt_snapshot}
    end
  end

  defp validate_legacy_payload({:flezha_snapshot, 1, payload}) when is_map(payload),
    do: {:ok, payload}

  defp validate_legacy_payload(_), do: {:error, :unsupported_snapshot_format}

  defp ensure_force_or_empty(_payload, true), do: :ok

  defp ensure_force_or_empty(payload, false) do
    namespaces = Map.get(payload, :namespaces, [])

    Enum.find_value(namespaces, :ok, fn ns ->
      hub_id = Identificator.hub(ns)

      if safe_registry_count(hub_id) > 0 do
        {:error, {:hub_not_empty, ns}}
      else
        nil
      end
    end)
  end

  defp run_legacy_import(payload) do
    operatives = Map.get(payload, :operatives, [])

    Enum.each(operatives, fn entry ->
      namespace = Map.fetch!(entry, :namespace)
      cspec = Map.fetch!(entry, :cspec)
      hub_id = Identificator.hub(namespace)

      # Declared like any operator start: an imported operative the declared
      # set never learns about would be invisible to the reconcile — imported
      # once, restored never.
      _ =
        ProcessHub.start_children(hub_id, [cspec],
          async_wait: false,
          durable: HubManager.durable_starts?()
        )
    end)

    IO.puts("boot import-legacy-snapshot: imported #{length(operatives)} operatives")
    :ok
  end
end
