defmodule Flezha.Service.Cli.Schedule do
  @moduledoc """
  `flz schedule add | list | remove | run` — scheduled operator spawns.

  Rendering only: the store and the hub are the effects the published
  `Flezha.Service.Schedule` wraps, and the authorization is each verb's row.
  `run` fires an instance directly, hub-free, and is judged as
  `operator.start` where the instance starts — the stored row's namespace.
  """

  alias Flezha.AuthZ.Namespace
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

  @doc "The stored schedule a `schedule run` fires, or the family's not-found."
  def locate([name | _]) do
    case Store.get(name) do
      {:ok, entry} -> {:ok, %{namespace: entry.namespace, entry: entry}}
      :error -> render_schedule_error({:not_found, name})
    end
  end

  @doc """
  The call's options carry the flags: `cron`, `tz` and `factory`; `namespace`,
  `init_values` and `plugins`, each defaulting as the help screen says when
  absent. The kind is the factory's: one a connected runner announced as a
  stream is refused here, and one nobody announces yet is judged at each fire.
  """
  def schedule_add(name, %{opts: opts}) do
    ns = Namespace.or_default(Keyword.get(opts, :namespace, ""))
    cron_expr = Keyword.get(opts, :cron, "")
    timezone = Keyword.get(opts, :tz, "")
    factory_id = Keyword.get(opts, :factory, "")
    plugins_json = Keyword.get(opts, :plugins, "")

    init_map = Context.json!(Keyword.get(opts, :init_values, ""), "--init-values")

    with :ok <- schedule_bounded_only(ns, factory_id),
         :ok <- schedule_check_plugins(plugins_json, ns, name),
         {:ok, entry} <-
           Store.add(%{
             name: name,
             cron: cron_expr,
             timezone: timezone,
             factory_id: factory_id,
             namespace: ns,
             init_values: init_map,
             text: Keyword.get(opts, :text),
             channel: Keyword.get(opts, :channel),
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

  def schedule_list(_call) do
    case Store.list() do
      [] -> IO.puts("no schedules")
      entries -> render_schedule_listing(entries, Hub.statuses())
    end
  end

  defp render_schedule_listing(entries, statuses) do
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

    Enum.each(entries, &IO.puts(schedule_row(schedule_fields(&1, statuses))))
  end

  defp schedule_fields(entry, statuses) do
    status = Map.get(statuses, entry.name, %{})
    last_fire = Map.get(status, :last_fire)

    [
      entry.name,
      entry.cron,
      entry.timezone,
      entry.factory_id,
      entry.namespace,
      Context.label_or(Map.get(status, :node), "-"),
      schedule_next_fire(entry),
      schedule_last_fire_at(last_fire),
      schedule_last_result(last_fire)
    ]
  end

  defp schedule_last_fire_at(%{at: at}),
    do: at |> DateTime.truncate(:second) |> DateTime.to_iso8601()

  defp schedule_last_fire_at(_), do: "-"

  defp schedule_last_result(%{outcome: :ok}), do: "ok"
  defp schedule_last_result(%{outcome: :duplicate}), do: "duplicate"

  defp schedule_last_result(%{outcome: {:retrying, reason}}),
    do: String.slice("retrying: #{reason}", 0, @schedule_result_width)

  defp schedule_last_result(_), do: "-"

  # Store first: retracting first would let the periodic re-assert sweep
  # resurrect the job before the row is gone.
  def schedule_remove(name, _call) do
    case Store.remove(name) do
      :ok ->
        _ = Hub.retract(name)
        IO.puts("Schedule #{name} removed")

      {:error, :not_found} ->
        render_schedule_error({:not_found, name})

      {:error, reason} ->
        render_schedule_error(reason)
    end
  end

  # Deliberately hub-free: a manual fire works on any node whether or not the
  # schedule hub is up.
  def schedule_run(_name, %{row: %{entry: entry}}) do
    instance_id = "#{entry.name}@manual-#{System.system_time(:second)}"

    case Job.spawn_instance(entry, instance_id) do
      {:ok, %{operator_id: id}} -> IO.puts("Spawned #{id}")
      :duplicate -> IO.puts("instance already running; no-op")
      {:error, reason} -> render_schedule_error(reason)
    end
  end

  defp schedule_bounded_only(namespace, factory_id) do
    case FleetManager.factory_kind(namespace, factory_id) do
      {:ok, :unbounded} -> {:error, {:unschedulable_kind, factory_id}}
      _bounded_or_unannounced -> :ok
    end
  catch
    :exit, _ -> :ok
  end

  # Full provider-resolving check at add time, so a typo fails at the CLI
  # rather than at 08:00. A slot instance id only exists at fire time, so the
  # check runs against a sentinel that could never be one.
  defp schedule_check_plugins(plugins_json, namespace, name) do
    case SpecDecoder.decode(plugins_json, namespace, "#{name}@spec-check") do
      {:ok, _} -> :ok
      {:error, _} = err -> err
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
  defp schedule_row(fields) do
    {padded, [last]} = Enum.split(fields, 8)

    Enum.zip_with(padded, [24, 18, 20, 24, 14, 22, 22, 22], &String.pad_trailing/2)
    |> Enum.join()
    |> Kernel.<>(last)
  end

  defp render_schedule_error(reason), do: Context.fail!(reason, &schedule_error_message/1)

  defp schedule_error_message({:not_found, name}), do: "schedule #{name} not found"
  defp schedule_error_message({:invalid_cron, msg}), do: "invalid --cron: #{msg}"

  defp schedule_error_message({:invalid_timezone, tz}),
    do: "invalid --tz: #{inspect(tz)} is not an IANA time zone"

  defp schedule_error_message({:invalid_name, msg}), do: "invalid schedule name: #{msg}"

  defp schedule_error_message({:unschedulable_kind, factory_id}),
    do: "only a bounded factory is schedulable; factory #{factory_id} is unbounded"

  defp schedule_error_message({:invalid_plugins, msg}), do: "invalid --plugins: #{msg}"

  defp schedule_error_message({:invalid_first_message, msg}),
    do: "invalid --text/--channel: #{msg}"

  defp schedule_error_message({:invalid_plugins_json, reason}),
    do: "invalid --plugins JSON: #{Context.format_reason(reason)}"

  defp schedule_error_message({:unknown_provider, id}),
    do: "invalid --plugins: unknown provider #{id}"

  defp schedule_error_message({:invalid_hook, detail}),
    do: "invalid --plugins hook: #{inspect(detail)}"

  defp schedule_error_message({:invalid_opts, detail}),
    do: "invalid --plugins opts: #{inspect(detail)}"

  defp schedule_error_message(:already_exists), do: "a schedule with that name already exists"
  defp schedule_error_message(_reason), do: nil
end
