defmodule Flezha.Plugin.Extension.Table.Workers do
  @moduledoc """
  Starting and stopping the workers a provider declares.

  The one place a provider worker comes up and the one place it goes down. A
  provider names its workers in `worker_specs/1` and its module and restart
  value in its declaration; the family, the registration, the monitor and the
  tracking are the host's, and they are here.

  It takes the table as a plain map with an `:entries` and a `:monitors` field
  and never names `Extension.Table`, so the dependency runs one way.

  A shareable plugin on a table built with `shared:` is a subscriber: what
  backs it is the shared copy it joins (`join/3`), tracked like a worker under
  the key `:shared`, so its death reaches the table as a worker's would.
  """

  alias Flezha.Plugin.Extension.Table.Entry
  alias Flezha.Plugin.Extension.Table.Health
  alias Flezha.Plugin.Provider.WorkerSup

  @doc """
  Start every worker `provider` declares for `opts`, tracking each under the key
  its declaration names. A provider that declares none answers `:ok` having
  started nothing.

  On the way out it stops any worker this plugin still holds under a key the
  current declaration does NOT name: a forward whose target lost a partition
  member is the case that produces one, and nothing else would ever stop it.
  """
  @spec start(map(), binary(), module(), map(), ([term()] -> any())) ::
          {map(), :ok | {:error, term()}}
  def start(table, plugin_id, provider, opts, invoke) do
    case provider.__provider__().worker do
      nil ->
        {table, :ok}

      worker ->
        case invoke.(fn -> provider.worker_specs(opts) end) do
          specs when is_list(specs) -> ensure(table, plugin_id, provider, worker, specs)
          {:crashed, kind, payload} -> {table, {:error, {kind, payload}}}
          other -> {table, {:error, {:invalid_worker_specs, other}}}
        end
    end
  end

  @doc """
  Stop every worker this plugin owns, through the supervisor that started it. A
  provider never stops one, so a `:permanent` child cannot be exited out from
  under its own supervisor and restarted straight back.

  The keys are the DECLARED ones as well as the tracked ones: a `:transient` or
  `:permanent` worker that crashed was restarted by its own supervisor under the
  same key while the table dropped it from the record, and stopping only what is
  tracked would leave that replacement running for ever.
  """
  @spec stop_all(map(), binary(), module(), map(), ([term()] -> any())) :: map()
  def stop_all(table, plugin_id, provider, opts, invoke) do
    case provider.__provider__().worker do
      nil ->
        table

      _worker ->
        family = WorkerSup.family(provider)
        declared = declared_keys(provider, opts, invoke)

        Enum.reduce(
          Enum.uniq(Entry.keys(entry(table, plugin_id)) ++ declared),
          table,
          &stop(&2, plugin_id, family, &1)
        )
    end
  end

  @doc "Stop the worker under one key and forget it."
  @spec stop(map(), binary(), module(), term()) :: map()
  def stop(table, plugin_id, family, key) do
    entry = entry(table, plugin_id)

    table =
      case Entry.worker(entry, key) do
        nil ->
          table

        {_pid, ref} ->
          Health.safe_demonitor(ref)

          table
          |> put_entry(plugin_id, Entry.drop_worker(entry, key))
          |> Map.update!(:monitors, &Map.delete(&1, ref))
      end

    _ = WorkerSup.stop_worker(family, key)
    table
  end

  @doc "Whether `provider`'s plugins on `table` are subscribers of a shared copy."
  @spec subscriber?(map(), module()) :: boolean()
  def subscriber?(%{shared: nil}, _provider), do: false
  def subscriber?(%{shared: _shared}, provider), do: provider.__provider__().shareable

  @doc """
  Join the shared copy of `plugin` on this controller (or the cluster's one for
  a `single` plugin), starting it when none runs, and track it.
  """
  @spec join(map(), binary(), Flezha.Plugin.t()) :: {map(), :ok | {:error, term()}}
  def join(table, plugin_id, plugin) do
    case table.shared.join(plugin, table.host_pid) do
      {:ok, copy} -> {track(table, plugin_id, :shared, copy), :ok}
      {:error, _} = refused -> {table, refused}
    end
  end

  @doc """
  Leave the shared copy `plugin_id` joined, `:now` or `:idle`, and forget it.
  """
  @spec leave(map(), binary(), :now | :idle) :: map()
  def leave(table, plugin_id, how) do
    entry = entry(table, plugin_id)

    case Entry.worker(entry, :shared) do
      nil ->
        table

      {copy, ref} ->
        Health.safe_demonitor(ref)
        table.shared.leave(copy, table.host_pid, how)

        table
        |> put_entry(plugin_id, Entry.drop_worker(entry, :shared))
        |> Map.update!(:monitors, &Map.delete(&1, ref))
    end
  end

  @doc "Record a worker the host started, under a fresh monitor."
  @spec track(map(), binary(), term(), pid()) :: map()
  def track(table, plugin_id, key, pid) do
    entry = entry(table, plugin_id)

    case Entry.worker(entry, key) do
      {^pid, _ref} ->
        table

      previous ->
        _ = previous && Health.safe_demonitor(elem(previous, 1))
        ref = Process.monitor(pid)

        table
        |> put_entry(plugin_id, Entry.put_worker(entry, key, pid, ref))
        |> Map.update!(:monitors, &Map.put(&1, ref, {plugin_id, key}))
    end
  end

  ## Private

  defp ensure(table, plugin_id, provider, worker, specs) do
    family = WorkerSup.family(provider)
    reusable? = reusable(worker)

    specs
    |> Enum.reduce_while({table, :ok}, fn {key, worker_opts}, {acc, :ok} ->
      case WorkerSup.ensure_worker(
             family,
             key,
             {worker.module, worker_opts},
             reusable?,
             worker.restart
           ) do
        {:ok, pid} -> {:cont, {track(acc, plugin_id, key, pid), :ok}}
        {:error, reason} -> {:halt, {acc, {:error, {:worker_start_failed, key, reason}}}}
      end
    end)
    |> case do
      {acc, :ok} -> {drop_undeclared(acc, plugin_id, family, Enum.map(specs, &elem(&1, 0))), :ok}
      halted -> halted
    end
  end

  defp drop_undeclared(table, plugin_id, family, declared) do
    table
    |> entry(plugin_id)
    |> Entry.keys()
    |> Enum.reject(&(&1 in declared))
    |> Enum.reduce(table, &stop(&2, plugin_id, family, &1))
  end

  defp declared_keys(provider, opts, invoke) do
    case invoke.(fn -> provider.worker_specs(opts || %{}) end) do
      specs when is_list(specs) -> Enum.map(specs, &elem(&1, 0))
      _other -> []
    end
  end

  defp reusable(%{reusable: {module, fun}}), do: &apply(module, fun, [&1])
  defp reusable(_worker), do: &Process.alive?/1

  defp entry(table, plugin_id) do
    case Map.get(table.entries, plugin_id) do
      %Entry{} = entry -> entry
      _ -> %Entry{}
    end
  end

  defp put_entry(table, plugin_id, entry),
    do: %{table | entries: Map.put(table.entries, plugin_id, entry)}
end
