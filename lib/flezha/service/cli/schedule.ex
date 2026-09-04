defmodule Flezha.Service.Cli.Schedule do
  @moduledoc """
  `flz schedule add | list | remove | run` — scheduled operative spawns.

  Rendering only for everything the policy names an action for.
  `Flezha.Service.Schedule` owns the store, the hub and the authorization, so
  the terminal and the console reach them through one gate instead of two.
  `run` fires an instance directly, hub-free, and is judged as
  `operative.start` where the instance starts.
  """

  alias Flezha.Plugin.SpecDecoder
  alias Flezha.Runner.FleetManager
  alias Flezha.Schedule.Cron
  alias Flezha.Schedule.Job
  alias Flezha.Schedule.Store
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.Schedule, as: ScheduleSvc

  # Error reasons truncate to the column; the full reason stays in
  # log/telemetry.
  @schedule_result_width 48

  @doc """
  `opts` carries the flags: `cron`, `tz` and `factory`; `namespace`,
  `init_values`, `kind` and `plugins`, each defaulting as the help screen says
  when absent; and `token`, the caller's credential.
  """
  def schedule_add(name, opts \\ []) do
    ns = Context.normalize_namespace(Keyword.get(opts, :namespace, ""))
    caller_context = Context.cli_context(opts)
    cron_expr = Keyword.get(opts, :cron, "")
    timezone = Keyword.get(opts, :tz, "")
    factory_id = Keyword.get(opts, :factory, "")
    plugins_json = Keyword.get(opts, :plugins, "")

    with :ok <- schedule_workflow_only(Keyword.get(opts, :kind, "workflow")),
         {:ok, init_map} <- schedule_decode_init(Keyword.get(opts, :init_values, "")),
         :ok <- schedule_check_plugins(plugins_json, ns, name),
         {:ok, entry} <-
           ScheduleSvc.add(
             %{
               name: name,
               cron: cron_expr,
               timezone: timezone,
               factory_id: factory_id,
               namespace: ns,
               init_values: init_map,
               plugins: plugins_json
             },
             caller_context
           ) do
      schedule_warn_absent_factory(ns, factory_id)
      _ = ScheduleSvc.assert_entry(entry, caller_context)

      IO.puts(
        "Schedule #{name} added (#{cron_expr} #{timezone}); next fire #{schedule_next_fire(entry)}"
      )
    else
      {:error, reason} -> render_schedule_error(reason, "schedule.add")
    end
  end

  def schedule_list(opts \\ []) do
    caller_context = Context.cli_context(opts)

    case ScheduleSvc.list(caller_context) do
      {:ok, []} -> IO.puts("no schedules")
      {:ok, entries} -> render_schedule_listing(entries, caller_context)
      {:error, reason} -> render_schedule_error(reason, "schedule.list")
    end
  end

  # The hub's view is a second read of `schedule.list` in the same scope, so a
  # caller that got the rows gets the statuses too unless the posture changed
  # between the two calls.
  defp render_schedule_listing(entries, caller_context) do
    case ScheduleSvc.statuses(caller_context) do
      {:error, reason} ->
        render_schedule_error(reason, "schedule.list")

      {:ok, statuses} ->
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

        Enum.each(entries, &IO.puts(schedule_row(schedule_cells(&1, statuses))))
    end
  end

  defp schedule_cells(entry, statuses) do
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

  def schedule_remove(name, opts \\ []) do
    caller_context = Context.cli_context(opts)

    # Store first: retracting first would let the periodic re-assert sweep
    # resurrect the job before the row is gone. The retraction is judged in the
    # same scope by a role that holds both actions, so it cannot refuse where
    # the removal passed.
    case ScheduleSvc.remove(name, caller_context) do
      :ok ->
        _ = ScheduleSvc.retract(name, caller_context)
        IO.puts("Schedule #{name} removed")

      {:error, :not_found} ->
        render_schedule_error({:not_found, name}, "schedule.remove")

      {:error, reason} ->
        render_schedule_error(reason, "schedule.remove")
    end
  end

  # Deliberately hub-free: a manual fire works on any node whether or not the
  # schedule hub is up. Firing starts an operative, so it is judged as
  # `operative.start` where the instance will start — the stored row's
  # namespace, which is why the row is read first.
  def schedule_run(name, opts \\ []) do
    case schedule_entry(name) do
      {:ok, entry} ->
        Context.with_cli_authorization(
          opts,
          :"operative.start",
          entry.namespace,
          %{schedule: name},
          fn -> schedule_spawn_manual(entry) end
        )

      {:error, reason} ->
        render_schedule_error(reason, "schedule.run")
    end
  end

  defp schedule_spawn_manual(entry) do
    instance_id = "#{entry.name}@manual-#{System.system_time(:second)}"

    case Job.spawn_instance(entry, instance_id) do
      {:ok, %{operative_id: id}} -> IO.puts("Spawned #{id}")
      :duplicate -> IO.puts("instance already running; no-op")
      {:error, reason} -> render_schedule_error(reason, "schedule.run")
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

  defp render_schedule_error({:forbidden, reason}, action), do: Context.refuse!(action, reason)

  defp render_schedule_error(reason, _action) do
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
