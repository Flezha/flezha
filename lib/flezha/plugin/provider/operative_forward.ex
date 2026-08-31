defmodule Flezha.Plugin.Provider.OperativeForward do
  @moduledoc """
  Operative-to-operative delivery (`:io_binding`): the member of the sink set
  whose destination is another operative in the same namespace rather than an
  external endpoint. Subscribe it to `:on_output` to feed a downstream
  operative, or to `:on_input` for a pass-through tap.

  The callback enqueues and returns; a per-`{plugin_id, target}` worker performs
  the confirming delivery and retries until the target confirms. Nothing ever
  blocks one Overwatch on another, so a cycle of operatives feeding each other
  is safe by construction.

  The delivery guarantee is derived, never configured: a source with a
  transactional outbox (effectively-once stream, or durable workflow) already
  stamps `flz.seq` / `flz.key` / `flz.epoch` identity, and a durable receiver
  already absorbs duplicates — retry-until-confirmed across those two halves is
  effectively-once. A source with no outbox gets retry-until-confirmed without
  deduplication. See `docs/resilience/data_durability.md`.

  Required opts:
    * `:target_operative_id` — the operative to deliver into

  Optional opts (defaults below):
    * `:queue_max`            — int, default `10_000` (then callers park)
    * `:retry_backoff_ms`     — int, default `250`
    * `:retry_max_backoff_ms` — int, default `5_000`
    * `:call_timeout_ms`      — int, default `5_000` (per delivery attempt)
    * `:plugin_id`            — string, defaults to the enabling plugin's id
  """

  alias Flezha.Operative.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.OperativeForward.{ForwardSup, ForwardWorker}
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.State.Outbox
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager

  use ProviderBehaviour

  import ProviderBehaviour, only: [coerce_int: 1, parse_int: 2]

  require Logger

  # The one default table. `defstruct`, the `build/1` allow-list, and the worker
  # opts all derive from it, so a new tunable is added in exactly one place.
  @int_opts [
    queue_max: 10_000,
    retry_backoff_ms: 250,
    retry_max_backoff_ms: 5_000,
    call_timeout_ms: 5_000
  ]

  # Resolved by the host at prepare time, never settable by config.
  @injected_opts [:worker_pid, :routes, :source_namespace, :source_operative_id]

  @enforce_keys [:target_operative_id]
  defstruct @enforce_keys ++ [:plugin_id] ++ @injected_opts ++ @int_opts

  # Declaring `@known_opts` is what makes this provider strict: `use
  # ProviderBehaviour` generates the `build/1` refusal from it, so a config
  # asking for a delivery guarantee this provider deliberately does not offer
  # fails loudly instead of reading as accepted.
  @known_opts [:target_operative_id, :plugin_id | Keyword.keys(@int_opts)]

  @impl true
  def plugin_type(), do: :io_binding

  def init_provider(opts) do
    source_namespace = Map.get(opts, :source_namespace)
    source_operative_id = Map.get(opts, :source_operative_id)
    target = Map.get(opts, :target_operative_id)
    # A re-prepare carries the member set this plugin last resolved; a first
    # prepare carries none.
    previous_routes = Map.get(opts, :routes, [])

    with :ok <- validate_target(target, source_namespace, source_operative_id),
         plugin_id = Map.get(opts, :plugin_id) || default_plugin_id(source_operative_id, target),
         {:ok, routes} <-
           ensure_workers(plugin_id, source_namespace, source_operative_id, target, opts) do
      opts =
        opts
        |> Map.put(:plugin_id, plugin_id)
        |> Map.put(:routes, routes)
        |> Map.put(:worker_pid, hd(routes).pid)

      rehome_vanished_members(opts, previous_routes, routes)

      {:ok, opts, hd(routes).pid}
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

    for %{id: member_id, pid: pid} <- previous, not MapSet.member?(surviving, member_id) do
      payloads = take_over(plugin_id, member_id, pid)

      if payloads != [] do
        Logger.info(
          "[Plugin.Provider.OperativeForward] #{member_id} is no longer a member of " <>
            "#{Map.get(opts, :target_operative_id)}; re-routing #{length(payloads)} " <>
            "queued payload(s) to their new owner"
        )
      end

      Enum.each(payloads, fn {message, metadata} ->
        case route(current, metadata) do
          {:ok, %{pid: new_pid}} -> ForwardWorker.absorb(new_pid, {message, metadata})
          {:error, reason} -> park_unroutable(opts, message, metadata, reason)
        end
      end)
    end

    :ok
  end

  # Take what a worker still owes and leave it stopped. A successful drain stops
  # it as it replies, which is what guarantees nothing is delivered from a queue
  # already handed over; only a worker that could not be reached needs the
  # supervisor to take it down, and a worker already dead owes nothing at all.
  defp take_over(plugin_id, member_id, pid) do
    if Process.alive?(pid), do: ForwardWorker.drain(pid), else: []
  catch
    :exit, _ ->
      ForwardSup.stop_worker({plugin_id, member_id})
      []
  end

  @doc """
  Stop every worker this plugin owns. The host tracks one `:owned_pid`; a
  partitioned target has one worker per member, and this is what keeps the
  other N−1 from outliving the plugin.
  """
  @impl true
  @spec terminate_provider(map()) :: :ok
  def terminate_provider(opts) do
    plugin_id = Map.get(opts, :plugin_id)

    for %{id: member_id} <- Map.get(opts, :routes, []), is_binary(plugin_id) do
      ForwardSup.stop_worker({plugin_id, member_id})
    end

    :ok
  end

  def on_input(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)
  def on_output(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)

  defp forward(%Ctx.IO{message: message, metadata: metadata} = ctx, opts) do
    case route(Map.fetch!(opts, :routes), metadata) do
      {:ok, %{pid: pid}} ->
        # Take ownership of a delivery-guaranteed output before handing it over:
        # the worker answers the hold when the target confirms the ingest. An
        # unroutable payload takes no hold — it parks below, which is a durable
        # outcome the outbox cursor may advance past.
        Outbox.hold(metadata)
        :ok = ForwardWorker.enqueue(pid, message, metadata)

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
             "key #{inspect(key)} hashes to group #{Partition.group_of(key)}, " <>
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
      Flezha.Dlq.Store.append(%{
        operative_class: source,
        procedure_id: "operative_forward",
        entity_key: target,
        channel: Map.get(metadata || %{}, "channel", "operative_forward"),
        origin_offset: nil,
        error:
          "#{reason} — forwarding from #{source} to partitioned target #{target}; " <>
            "emit with a key (`ctx.emit_keyed`) so the owning partition can be resolved",
        attempts: 1,
        payload: message
      })

    :ok
  end

  @doc """
  Health verdict: every worker alive. Delivery failure is not unhealthiness —
  an unreachable peer is retried, and convergence is already healing it — but a
  worker that is gone is, and with a fan-out the worst verdict is the one that
  matters.
  """
  @impl true
  @spec health(map()) :: ProviderBehaviour.health_verdict()
  def health(opts) do
    case Map.get(opts, :routes) do
      routes when is_list(routes) and length(routes) > 1 ->
        routes
        |> Enum.map(&worker_verdict(Map.put(opts, :worker_pid, &1.pid)))
        |> Enum.find(:healthy, &(&1 != :healthy))

      _ ->
        worker_verdict(opts)
    end
  end

  defp worker_verdict(opts) do
    ProviderBehaviour.worker_health(
      opts,
      [pid: [:worker_pid, :owned_pid], absent: :no_worker, dead: :worker_dead],
      fn _pid -> :healthy end
    )
  end

  # One worker per target member — the existing `{plugin_id, target}` keying
  # already distinguishes them. Per-member workers rather than one router
  # because a worker is one queue with one in-flight slot: a shared one would
  # head-of-line-block every partition behind the slowest, and per-key ordering
  # only needs per-member FIFO.
  defp ensure_workers(plugin_id, source_namespace, source_operative_id, target, opts) do
    target
    |> target_members(source_namespace)
    |> Enum.reduce_while({:ok, []}, fn {member_id, range}, {:ok, acc} ->
      case ForwardSup.ensure_worker(
             [
               plugin_id: plugin_id,
               source_namespace: source_namespace,
               source_operative_id: source_operative_id,
               target_operative_id: member_id
             ] ++ tunables(opts)
           ) do
        {:ok, pid} -> {:cont, {:ok, [%{id: member_id, range: range, pid: pid} | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, routes} -> {:ok, Enum.reverse(routes)}
      err -> err
    end
  end

  # `{member_id, range}` per target member. An ordinary operative — including a
  # target that is not started yet — is one member owning the whole space, which
  # is what keeps a single-member target byte-identical to before partitioning.
  defp target_members(target, namespace) do
    case group_members(namespace, target) do
      [] -> [{target, {0, Partition.key_groups() - 1}}]
      members -> Enum.map(members, &{&1.id, &1.partition.range})
    end
  end

  defp group_members(namespace, target) when is_binary(namespace),
    do: PartitionGroup.members(namespace, target)

  defp group_members(_namespace, _target), do: []

  # ---------- enable-time refusals ----------

  defp validate_target(nil, _ns, _source),
    do: {:error, "operative_forward requires :target_operative_id"}

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

  defp tunables(opts) do
    Enum.map(@int_opts, fn {field, default} ->
      {field, coerce_int(Map.get(opts, field, default))}
    end)
  end

  @int_fields Keyword.keys(@int_opts)

  defp convert_value(field, value) when field in @int_fields, do: parse_int(value, field)

  # Every event carries the source operative, the target operative and the
  # plugin id, so a saturating cycle between two operatives is visible to an
  # operator without parsing log lines.
  @prefix [:flezha, :plugin, :operative_forward]

  @doc "One payload reached the target. `outcome` is the verdict's terminal tag."
  @spec delivered(atom(), map()) :: :ok
  def delivered(outcome, labels) do
    :telemetry.execute(@prefix ++ [:delivered], %{count: 1}, Map.put(labels, :outcome, outcome))
  end

  @doc "A delivery attempt failed and the payload is retained for retry."
  @spec retry(pos_integer(), non_neg_integer(), term(), map()) :: :ok
  def retry(attempt, backoff_ms, reason, labels) do
    :telemetry.execute(
      @prefix ++ [:retry],
      %{attempt: attempt, backoff_ms: backoff_ms},
      Map.put(labels, :reason, reason)
    )
  end

  @doc "Current queue depth, emitted on every enqueue."
  @spec queue_depth(non_neg_integer(), map()) :: :ok
  def queue_depth(depth, labels) do
    :telemetry.execute(@prefix ++ [:queue_depth], %{depth: depth}, labels)
  end

  @doc "The queue reached `:queue_max` and the enqueueing caller is parked."
  @spec saturated(non_neg_integer(), map()) :: :ok
  def saturated(depth, labels) do
    :telemetry.execute(@prefix ++ [:saturated], %{depth: depth}, labels)
  end
end
