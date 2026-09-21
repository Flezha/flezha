defmodule Flezha.Plugin.Provider.OperativeForward do
  @moduledoc """
  Operative-to-operative delivery (`:io_binding`): the sink whose destination
  is another operative in the same namespace rather than an external endpoint.

  The callback enqueues and returns; a per-`{plugin_id, target}` worker does
  the confirming delivery and retries until the target confirms. Nothing ever
  blocks one Overwatch on another, so a cycle of operatives feeding each other
  is safe by construction.

  The delivery guarantee is DERIVED, never configured: a source with a
  transactional outbox already stamps identity and a durable receiver already
  absorbs duplicates, so retry-until-confirmed across the two is
  effectively-once. Without an outbox it is at-least-once.

  Required opt `:target_operative_id`; rest: `docs/plugins/operative_forward.md`.
  """

  alias Flezha.Dlq.Entry, as: DlqEntry
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.OperativeForward.ForwardWorker
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.SDK.KeyGroup
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Outbox

  require Logger

  use ProviderBehaviour,
    options: [
      target_operative_id: [
        type: :string,
        required: true,
        doc: "The operative in this namespace the payload is delivered to"
      ],
      plugin_id: [type: :string, doc: "Plugin id; defaults to `<source>-><target>`"],
      queue_max: [type: :int, default: 10_000, min: 1, doc: "Records held per target member"],
      retry_backoff_ms: [type: :int, default: 250, min: 1, doc: "First delay after a refusal"],
      retry_max_backoff_ms: [type: :int, default: 5_000, min: 1, doc: "Ceiling for that delay"],
      call_timeout_ms: [type: :int, default: 5_000, min: 1, doc: "Budget for one confirming call"],
      # The target's member set, resolved at every prepare. It carries no pid:
      # a route names the member, and the worker for it is addressed by name.
      routes: [type: :any, internal: true]
    ],
    worker: [module: ForwardWorker, restart: :transient]

  @impl true
  def init_provider(opts) do
    target = opts.target_operative_id
    # A re-prepare carries the member set this plugin last resolved; a first
    # prepare carries none.
    previous_routes = opts.routes || []

    with :ok <-
           validate_target(target, opts.source_namespace, opts.source_operative_id) do
      plugin_id = opts.plugin_id || default_plugin_id(opts.source_operative_id, target)
      routes = target_routes(target, opts.source_namespace)
      opts = Map.merge(opts, %{plugin_id: plugin_id, routes: routes})

      rehome_vanished_members(opts, previous_routes, routes)

      {:ok, opts}
    end
  end

  @doc """
  One worker per target member, keyed `{plugin_id, member_id}`. Per-member
  workers rather than one router because a worker is one queue with one
  in-flight slot: a shared one would head-of-line-block every partition behind
  the slowest, and per-key ordering only needs per-member FIFO.
  """
  @impl true
  def worker_specs(opts) do
    for %{id: member_id} <- opts.routes do
      {key(opts.plugin_id, member_id),
       %{
         plugin_id: opts.plugin_id,
         source_namespace: opts.source_namespace,
         source_operative_id: opts.source_operative_id,
         target_operative_id: member_id,
         queue_max: opts.queue_max,
         retry_backoff_ms: opts.retry_backoff_ms,
         retry_max_backoff_ms: opts.retry_max_backoff_ms,
         call_timeout_ms: opts.call_timeout_ms
       }}
    end
  end

  # A re-run of `init_provider/1` re-reads the target's member set, which is how
  # a rescaled target stops being routed by yesterday's ranges. Members that
  # survive keep their worker — `ensure_worker/1` is keyed `{plugin_id, member}`
  # — and therefore keep everything queued in it. A member that is GONE would
  # otherwise leave a worker retrying forever against an operative that no
  # longer exists, so its payloads are taken back and re-routed to whoever owns
  # their key now.
  #
  # Synchronous on purpose: no emit can be routed while `init_provider/1` runs,
  # so a re-homed payload lands ahead of anything emitted after the re-resolve
  # and per-key order survives the rescale.
  defp rehome_vanished_members(opts, previous, current) do
    plugin_id = Map.fetch!(opts, :plugin_id)
    surviving = MapSet.new(current, & &1.id)

    for %{id: member_id} <- previous, not MapSet.member?(surviving, member_id) do
      payloads = take_over(plugin_id, member_id)

      if payloads != [] do
        Logger.info(
          "[Plugin.Provider.OperativeForward] #{member_id} is no longer a member of " <>
            "#{opts.target_operative_id}; re-routing #{length(payloads)} " <>
            "queued payload(s) to their new owner"
        )
      end

      Enum.each(payloads, &rehome_payload(&1, current, opts))
    end

    :ok
  end

  defp rehome_payload({message, metadata}, current, opts) do
    case route(current, metadata) do
      {:ok, %{id: member_id}} ->
        ForwardWorker.absorb(worker(opts.plugin_id, member_id), {message, metadata})

      {:error, reason} ->
        park_unroutable(opts, message, metadata, reason)
    end
  end

  # Take what a worker still owes. The table stops the worker itself, because a
  # key the declaration does not name is one it drops at the next activation;
  # a worker that could not be reached, or is already dead, owes nothing.
  defp take_over(plugin_id, member_id) do
    ForwardWorker.drain(worker(plugin_id, member_id))
  catch
    :exit, _ -> []
  end

  @impl true
  def on_input(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)
  @impl true
  def on_output(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)

  defp forward(%Ctx.IO{message: message, metadata: metadata} = ctx, opts) do
    case route(opts.routes, metadata) do
      {:ok, %{id: member_id}} ->
        # Take ownership of a delivery-guaranteed output before handing it over:
        # the worker answers the hold when the target confirms the ingest. An
        # unroutable payload takes no hold, it parks below, which is a durable
        # outcome the outbox cursor may advance past.
        Outbox.hold(metadata)
        :ok = ForwardWorker.enqueue(worker(opts.plugin_id, member_id), message, metadata)

      {:error, reason} ->
        park_unroutable(opts, message, metadata, reason)
    end

    {:ok, ctx}
  end

  # One member: everything goes there, keyed or not — today's behaviour.
  defp route([route], _metadata), do: {:ok, route}

  # A partitioned target: the key decides, and only the key. Round-robin or a
  # payload hash would scatter one key across partitions and detonate
  # downstream, where the cause is invisible.
  defp route(routes, metadata) do
    case Map.get(metadata || %{}, "flz.key") do
      key when is_binary(key) and key != "" ->
        case Partition.owner_of(routes, key) do
          nil ->
            {:error,
             "key #{inspect(key)} hashes to group #{KeyGroup.group_of(key)}, " <>
               "owned by no member"}

          route ->
            {:ok, route}
        end

      _ ->
        {:error, "partitioned target requires keyed emit"}
    end
  end

  # Never guessed, never dropped: an unroutable message is parked under the
  # source's DLQ identity with the contract it violated.
  defp park_unroutable(opts, message, metadata, reason) do
    source = Map.get(opts, :source_operative_id)
    target = Map.get(opts, :target_operative_id)

    Logger.warning(
      "[Plugin.Provider.OperativeForward] parking undeliverable payload " <>
        "source=#{inspect(source)} target=#{inspect(target)}: #{reason}"
    )

    _ =
      DlqStore.append(
        DlqEntry.new!(%{
          operative_class: source,
          tasklet_id: "operative_forward",
          cell_key: target,
          channel: Map.get(metadata || %{}, "channel", "operative_forward"),
          error:
            "#{reason}: forwarding from #{source} to partitioned target #{target}; " <>
              "emit with a key (`ctx.emit_keyed`) so the owning partition can be resolved",
          attempts: 1,
          payload: message
        })
      )

    :ok
  end

  @doc """
  Health verdict: every worker alive. Delivery failure is not unhealthiness —
  an unreachable peer is retried, and convergence is already healing it — but a
  worker that is gone is, and with a fan-out the worst verdict is the one that
  matters.
  """
  # A forward resolves its target's member set once, at `init_provider/1`, so
  # a group that changed shape underneath it routes by yesterday's ranges:
  # payloads for a member absent from the group queue behind a worker that
  # will never deliver. The verdict names it, the convergence loop restarts
  # the plugin, and the re-prepare re-routes what the vanished member held.
  @impl true
  @spec health(map()) :: ProviderBehaviour.health_verdict()
  def health(opts) do
    case opts[:routes] do
      routes when is_list(routes) and routes != [] ->
        routes
        |> Enum.map(&member_verdict(opts, &1.id))
        |> Enum.find(:healthy, &(&1 != :healthy))
        |> stale_members(opts, routes)

      _ ->
        worker_verdict(opts)
    end
  end

  defp member_verdict(opts, member_id) do
    opts[:workers][key(opts.plugin_id, member_id)]
    |> ProviderBehaviour.pid_health([absent: :no_worker, dead: :worker_dead], fn _pid ->
      :healthy
    end)
  end

  defp stale_members(:healthy, opts, routes) do
    resolved = Enum.map(routes, & &1.id)
    live = target_members(opts.target_operative_id, opts.source_namespace)

    if Enum.sort(resolved) == Enum.sort(Enum.map(live, &elem(&1, 0))),
      do: :healthy,
      else: {:unhealthy, :stale_members}
  end

  defp stale_members(verdict, _opts, _routes), do: verdict

  defp worker_verdict(opts) do
    ProviderBehaviour.worker_health(
      opts,
      [absent: :no_worker, dead: :worker_dead],
      fn _pid -> :healthy end
    )
  end

  # The routes a target resolves to: one row per member, each carrying the key
  # range it owns. No process is touched here, so a re-prepare can re-resolve a
  # rescaled target without starting or stopping anything.
  defp target_routes(target, namespace) do
    for {member_id, range} <- target_members(target, namespace),
        do: %{id: member_id, range: range}
  end

  # The key one member's worker is registered under, named once and read by both
  # the declaration and the dispatch path.
  defp key(plugin_id, member_id), do: {plugin_id, member_id}

  defp worker(plugin_id, member_id), do: WorkerSup.worker(__MODULE__, key(plugin_id, member_id))

  # `{member_id, range}` per target member. An ordinary operative — including a
  # target that is not started yet — is one member owning the whole space, which
  # is what keeps a single-member target byte-identical to before partitioning.
  defp target_members(target, namespace) do
    case group_members(namespace, target) do
      [] -> [{target, {0, KeyGroup.key_groups() - 1}}]
      members -> Enum.map(members, &{&1.id, &1.partition.range})
    end
  end

  defp group_members(namespace, target) when is_binary(namespace),
    do: PartitionGroup.members(namespace, target)

  defp group_members(_namespace, _target), do: []

  # ---------- enable-time refusals ----------

  defp validate_target(target, _ns, source) when target == source,
    do: {:error, "operative_forward target_operative_id #{target} is the operative itself"}

  # A target absent from this namespace is NOT refused — it may simply not be
  # started yet, and the worker retries. It is refused only when it is found
  # somewhere else: namespaces are the AuthZ and quota boundary, and crossing one
  # is a policy decision, not a plugin option.
  defp validate_target(target, namespace, _source) do
    if resolves_in?(namespace, target) do
      :ok
    else
      refuse_if_foreign(target, foreign_namespace(namespace, target))
    end
  end

  defp refuse_if_foreign(_target, nil), do: :ok

  defp refuse_if_foreign(target, foreign) do
    {:error,
     "operative_forward target_operative_id #{target} resolves in namespace " <>
       "#{foreign}, across the namespace boundary"}
  end

  defp resolves_in?(nil, _target), do: false

  defp resolves_in?(namespace, target) do
    match?({:ok, _pid}, HubManager.get_pid(namespace, target))
  end

  defp foreign_namespace(namespace, target) do
    case OperativeManager.namespace_of(target, except: namespace) do
      {:ok, foreign} -> foreign
      :error -> nil
    end
  end

  defp default_plugin_id(source, target), do: "#{source}->#{target}"
end
