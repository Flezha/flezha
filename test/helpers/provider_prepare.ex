defmodule FlezhaTest.Helper.ProviderPrepare do
  @moduledoc """
  Preparation and activation as `Flezha.Plugin.Extension.Table` performs them,
  for a test that drives one provider rather than a whole relay.

  The table does two things before a provider's `init_provider/1` runs: it
  merges the host's identity and it normalises the options through the
  provider's declaration. A test that skips either is testing a shape the
  system never produces, so both live here rather than in each test file.

  A sink provider's test also stands in for the writer its outputs confirm to:
  `stub_outbox_owner/1`, `guaranteed_metadata/3` and `assert_confirmed/2`.
  This file compiles alone, so a plugin package names it in its test paths.
  """

  import ExUnit.Assertions, only: [assert_receive: 2, flunk: 1, refute_received: 1]
  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Flezha.Plugin.Extension.Table.Entry
  alias Flezha.Plugin.Extension.Table.Workers
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions
  alias Flezha.State.Outbox
  alias Flezha.State.Paths
  alias Flezha.State.Supervisor, as: StateSupervisor

  @doc """
  Prepare `opts` for `provider`: normalise through the declaration, run
  `init_provider/1`, then key it. Answers what the table would store, or the
  refusal.
  """
  @spec prepare(module(), map()) :: {:ok, map()} | {:error, term()}
  def prepare(provider, opts) do
    with {:ok, normalised} <- ProviderOptions.normalise_opts(provider, with_host_identity(opts)),
         {:ok, prepared} <- provider.init_provider(normalised),
         do: {:ok, WorkerSup.keyed(prepared)}
  end

  # What `Table.with_host_identity/3` merges: the caller stands in for the host
  # relay, which is what a provider's worker is handed. The subscription
  # list is one of the five, and a provider that allocates only for the
  # directions it is bound in reads it — an absent key is a shape the table
  # never produces.
  defp with_host_identity(opts) do
    Map.merge(
      %{
        overwatcher_pid: self(),
        source_namespace: :default,
        source_relay_id: "test-relay",
        source_plugin_id: Map.get(opts, :plugin_id),
        subscribed_extensions: []
      },
      opts
    )
  end

  @doc """
  Start `provider`'s declared workers from prepared `opts`, exactly as
  activation does: through `Table.Workers`, which is the production path, so a
  test cannot drift from what the table actually starts. Answers the first
  worker's pid, which is the one the table tracks as the plugin's own.
  """
  @spec activate(module(), map()) :: {:ok, pid()} | {:error, term()}
  def activate(provider, opts) do
    plugin_id = Map.get(opts, :plugin_id) || "provider-prepare"
    table = %{entries: %{}, monitors: %{}}

    case Workers.start(table, plugin_id, provider, opts, & &1.()) do
      {started, :ok} -> {:ok, Entry.owned_pid(started.entries[plugin_id])}
      {_table, {:error, _} = err} -> err
    end
  end

  @doc "Prepare and activate in one call, answering the prepared opts and the worker."
  @spec prepare_and_activate(module(), map()) :: {:ok, map(), pid()} | {:error, term()}
  def prepare_and_activate(provider, opts) do
    with {:ok, prepared} <- prepare(provider, opts),
         {:ok, pid} <- activate(provider, prepared),
         do: {:ok, prepared, pid}
  end

  @doc """
  Answer the next confirming ingest a source's worker made of the calling test,
  which stands in for the host relay, with `verdict`: anything a real
  Overwatch could answer, the refusals included. Answers what was injected.
  """
  @spec take_ingest(term(), timeout()) :: {binary(), map()}
  def take_ingest(verdict \\ :ok, timeout \\ 2_000) do
    receive do
      {:"$gen_call", from, {:ingest, body, metadata}} ->
        GenServer.reply(from, verdict)
        {body, metadata}
    after
      timeout -> flunk("no ingest call within #{timeout}ms")
    end
  end

  @doc "No confirming ingest is waiting in the calling test's mailbox."
  def refute_ingest(), do: refute_received({:"$gen_call", _from, {:ingest, _, _}})

  @doc """
  Stand in for `provider`'s worker of the plugin `prepare/2` readies under
  `plugin_id`, registered under that plugin's worker key. It answers every
  `{:sink_frame, message, metadata}` ask `:ok` and forwards it to the calling
  test as `{:shipped, message, metadata}`.

  A negative assertion needs this as much as a positive one. With no worker
  registered at all a provider refuses the ask, so "nothing was shipped" would
  hold for a plugin that is bound in a direction and one that is not alike.
  """
  @spec stub_sink_worker(module(), binary()) :: pid()
  def stub_sink_worker(provider, plugin_id) do
    test = self()
    key = WorkerSup.key(with_host_identity(%{plugin_id: plugin_id}))

    worker =
      spawn_link(fn ->
        Registry.register(WorkerSup.registry_name(WorkerSup.family(provider)), key, nil)
        send(test, {:registered, plugin_id})
        answer_asks(test)
      end)

    assert_receive {:registered, ^plugin_id}, 1_000
    worker
  end

  defp answer_asks(test) do
    receive do
      {{:sink_frame, message, metadata}, from, ref} ->
        send(from, {ref, :ok})
        send(test, {:shipped, message, metadata})
        answer_asks(test)
    end
  end

  @doc """
  Stop `provider`'s `worker` the way a disable does: through its own supervisor.
  A worker that is no longer registered answers `:ok`.
  """
  @spec stop(module(), pid()) :: :ok
  def stop(provider, worker) do
    family = WorkerSup.family(provider)
    registry = WorkerSup.registry_name(family)

    case Enum.find(WorkerSup.keys(family), fn key ->
           match?([{^worker, _}], Registry.lookup(registry, key))
         end) do
      nil -> :ok
      key -> WorkerSup.stop_worker(family, key)
    end
  end

  @doc """
  Stop `worker` through its supervisor when the test ends, for a provider whose
  worker outlives its host.

  A kill may not be used, and neither may a wait: a supervisor reports an
  abnormally exiting child at error level, `on_exit` runs outside the test's log
  capture, and a live socket worker spends any wait here reconnecting to a
  listener the test has already closed. A worker that stops itself takes
  `await_stop_on_exit/1` instead, which is the other half of this pair.
  """
  @spec stop_on_exit(module(), pid()) :: :ok
  def stop_on_exit(provider, worker) do
    on_exit(fn -> stop(provider, worker) end)
  end

  @doc """
  Settle the exit `worker` takes on its own when the test ends, for a provider
  whose worker monitors its host: the host is the test process, so the exit is
  already in flight by the time `on_exit` runs.

  Stopping such a worker a second time is a race, not a teardown, and the losing
  side is a supervisor reporting a child that is already gone at error level.
  """
  @spec await_stop_on_exit(pid()) :: :ok
  def await_stop_on_exit(worker) do
    on_exit(fn ->
      ref = Process.monitor(worker)
      assert_receive {:DOWN, ^ref, :process, ^worker, _}, 2_000
    end)
  end

  @doc """
  Stand in as the WalWriter that owns `cell_id`'s outbox cursor: the
  delivery-confirmation seam (`Flezha.State.Outbox`) resolves the owning writer
  through the state registry, so a registered test process receives the sink's
  `hold` / `settled` / `confirm` reports as plain casts.

  Used by the sink-plugin suites, which exercise the confirmation seam without a
  real persistence subsystem behind it.
  """
  @spec stub_outbox_owner(Paths.cell_id()) :: :ok
  def stub_outbox_owner(cell_id) do
    # Unlinked, so the test that started it cannot take it down, with every
    # test linked to it since, when it exits with `:shutdown`.
    case Registry.start_link(keys: :unique, name: StateSupervisor.registry_name()) do
      {:ok, registry} -> Process.unlink(registry)
      {:error, {:already_started, _}} -> :ok
    end

    {:ok, _} = Registry.register(StateSupervisor.registry_name(), cell_id, nil)
    :ok
  end

  @doc """
  Metadata a durable outbox flush would stamp on one output of `cell_id`'s
  entry `seq` — identity plus the confirmation token the owning writer waits
  on. `extra` merges over it for the fields a given sink reads.
  """
  @spec guaranteed_metadata(Paths.cell_id(), non_neg_integer(), map()) :: map()
  def guaranteed_metadata({rc, sh, pk} = cell_id, seq, extra \\ %{}) do
    Map.merge(
      %{
        "channel" => "out",
        "flz.cell" => "#{rc}/#{sh}/#{pk}",
        "flz.seq" => "#{seq}.0",
        "flz.epoch" => "0",
        Outbox.ack_key() => Outbox.token(cell_id, {:cursor, seq})
      },
      extra
    )
  end

  @doc """
  Every named entry is confirmed to its owning writer. A sink confirms a
  completed batch in one report and parks one record at a time, and how many
  records a linger window gathered into a batch is timing — so this asserts on
  the entries confirmed, never on how they were reported.
  """
  def assert_confirmed(keys, timeout \\ 2_000), do: await_confirmed(MapSet.new(keys), timeout)

  defp await_confirmed(remaining, timeout) do
    if MapSet.size(remaining) == 0 do
      :ok
    else
      receive do
        {:"$gen_cast", {:outbox_confirmed_many, keys}} ->
          await_confirmed(Enum.reduce(keys, remaining, &MapSet.delete(&2, &1)), timeout)

        {:"$gen_cast", {:outbox_confirmed, key}} ->
          await_confirmed(MapSet.delete(remaining, key), timeout)
      after
        timeout ->
          flunk("never confirmed: #{inspect(MapSet.to_list(remaining))}")
      end
    end
  end
end
