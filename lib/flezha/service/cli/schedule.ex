defmodule Flezha.Service.Cli.Schedule do
  @moduledoc """
  `flz schedule add | list | remove | run` — scheduled operative spawns.
  """

  alias Flezha.Plugin.SpecDecoder
  alias Flezha.Runner.FleetManager
  alias Flezha.Schedule.Cron
  alias Flezha.Schedule.Hub
  alias Flezha.Schedule.Job
  alias Flezha.Schedule.Store
  alias Flezha.Service.Cli.Context

  # Error reasons truncate to the column; the full reason stays in
  # log/telemetry.
  @schedule_result_width 48

  def schedule_add(
        name,
        cron_expr,
        timezone,
        factory_id,
        namespace \\ "",
        init_values \\ "",
        kind \\ "workflow",
        plugins_json \\ ""
      ) do
    ns = Context.normalize_namespace(namespace)

    with :ok <- schedule_workflow_only(kind),
         {:ok, init_map} <- schedule_decode_init(init_values),
         :ok <- schedule_check_plugins(plugins_json, ns, name),
         {:ok, entry} <-
           Store.add(%{
             name: name,
             cron: cron_expr,
             timezone: timezone,
             factory_id: factory_id,
             namespace: ns,
             init_values: init_map,
             plugins: plugins_json
           }) do
      schedule_warn_absent_factory(ns, factory_id)
      _ = Hub.assert(entry)

      IO.puts(
        "Schedule #{name} added (#{cron_expr} #{timezone}); next fire #{schedule_next_fire(entry)}"
      )
    else
      {:error, reason} -> render_schedule_error(reason)
    end
  end

  def schedule_list() do
    case Store.list() do
      [] ->
        IO.puts("no schedules")

      entries ->
        statuses = Hub.statuses()

        IO.puts(
          schedule_row([
            "NAME",
            "CRON",
            "TZ",
            "FACTORY",
            "NAMESPACE",
            "NODE",
            "NEXT FIRE (UTC)",
            "LAST FIRE (UTC)",
            "LAST RESULT"
          ])
        )

        Enum.each(entries, fn e ->
          status = Map.get(statuses, e.name, %{})
          last_fire = Map.get(status, :last_fire)

          IO.puts(
            schedule_row([
              e.name,
              e.cron,
              e.timezone,
              e.factory_id,
              e.namespace,
              Context.label_or(Map.get(status, :node), "-"),
              schedule_next_fire(e),
              schedule_last_fire_at(last_fire),
              schedule_last_result(last_fire)
            ])
          )
        end)
    end
  end

  defp schedule_last_fire_at(%{at: at}),
    do: at |> DateTime.truncate(:second) |> DateTime.to_iso8601()

  defp schedule_last_fire_at(_), do: "-"

  defp schedule_last_result(%{outcome: :ok}), do: "ok"
  defp schedule_last_result(%{outcome: :duplicate}), do: "duplicate"

  defp schedule_last_result(%{outcome: {:retrying, reason}}),
    do: String.slice("retrying: #{reason}", 0, @schedule_result_width)

  defp schedule_last_result(_), do: "-"

  def schedule_remove(name) do
    # Store first: retracting first would let the periodic re-assert sweep
    # resurrect the job before the row is gone.
    case Store.remove(name) do
      :ok ->
        _ = Hub.retract(name)
        IO.puts("Schedule #{name} removed")

      {:error, :not_found} ->
        render_schedule_error({:not_found, name})
    end
  end

  # Deliberately hub-free: a manual fire works on any node whether or not the
  # schedule hub is up.
  def schedule_run(name) do
    with {:ok, entry} <- schedule_entry(name),
         instance_id = "#{name}@manual-#{System.system_time(:second)}",
         {:ok, %{operative_id: id}} <-
           Job.spawn_instance(entry, instance_id) do
      IO.puts("Spawned #{id}")
    else
      :duplicate -> IO.puts("instance already running; no-op")
      {:error, reason} -> render_schedule_error(reason)
    end
  end

  defp schedule_entry(name) do
    case Store.get(name) do
      {:ok, entry} -> {:ok, entry}
      :error -> {:error, {:not_found, name}}
    end
  end

  defp schedule_workflow_only(kind) when kind in ["", "workflow"], do: :ok
  defp schedule_workflow_only(kind), do: {:error, {:unschedulable_kind, kind}}

  # Full provider-resolving check at add time, so a typo fails at the CLI
  # rather than at 08:00. A slot instance id only exists at fire time, so the
  # check runs against a sentinel that could never be one.
  defp schedule_check_plugins(plugins_json, namespace, name) do
    case SpecDecoder.decode(plugins_json, namespace, "#{name}@spec-check") do
      {:ok, _} -> :ok
      {:error, _} = err -> err
    end
  end

  defp schedule_decode_init(""), do: {:ok, %{}}

  defp schedule_decode_init(json) do
    case Jason.decode(json) do
      {:ok, map} when is_map(map) -> {:ok, map}
      _ -> {:error, {:invalid_init_values, json}}
    end
  end

  # Warn-only: the factory's runner may simply not have connected yet.
  defp schedule_warn_absent_factory(namespace, factory_id) do
    case FleetManager.runners_for_factory(namespace, factory_id) do
      [] ->
        IO.puts(
          "warning: factory #{factory_id} has no live runner in namespace #{namespace}; " <>
            "firings will fail until one connects"
        )

      _ ->
        :ok
    end
  catch
    :exit, _ -> :ok
  end

  defp schedule_next_fire(entry) do
    with {:ok, cron} <- Cron.parse(entry.cron),
         {:ok, next} <- Cron.next_fire(cron, entry.timezone, DateTime.utc_now()) do
      DateTime.to_iso8601(next)
    else
      _ -> "never"
    end
  end

  # First six columns padded; the last is printed raw.
  defp schedule_row(cells) do
    {padded, [last]} = Enum.split(cells, 8)

    Enum.zip_with(padded, [24, 18, 20, 24, 14, 22, 22, 22], &String.pad_trailing/2)
    |> Enum.join()
    |> Kernel.<>(last)
  end

  defp render_schedule_error(reason) do
    Context.cli_error!("error: " <> schedule_error_message(reason), :stderr)
  end

  defp schedule_error_message({:not_found, name}), do: "schedule #{name} not found"
  defp schedule_error_message({:invalid_cron, msg}), do: "invalid --cron: #{msg}"

  defp schedule_error_message({:invalid_timezone, tz}),
    do: "invalid --tz: #{inspect(tz)} is not an IANA time zone"

  defp schedule_error_message({:invalid_name, msg}), do: "invalid schedule name: #{msg}"

  defp schedule_error_message({:invalid_init_values, _}),
    do: "--init_values must be a JSON object"

  defp schedule_error_message({:unschedulable_kind, kind}),
    do: "only workflow operatives are schedulable (got kind #{kind})"

  defp schedule_error_message({:invalid_plugins, msg}), do: "invalid --plugins: #{msg}"

  defp schedule_error_message({:invalid_plugins_json, reason}),
    do: "invalid --plugins JSON: #{inspect(reason)}"

  defp schedule_error_message({:unknown_provider, id}),
    do: "invalid --plugins: unknown provider #{id}"

  defp schedule_error_message({:invalid_subscription, detail}),
    do: "invalid --plugins subscription: #{inspect(detail)}"

  defp schedule_error_message({:invalid_opts, detail}),
    do: "invalid --plugins opts: #{inspect(detail)}"

  defp schedule_error_message(:already_exists), do: "a schedule with that name already exists"
  defp schedule_error_message(reason), do: inspect(reason)
end
