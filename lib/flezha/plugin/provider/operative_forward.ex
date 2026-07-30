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
  transactional outbox (effectively-once unbounded, or durable bounded) already
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

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.OperativeForward.{ForwardSup, ForwardWorker}
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Service.HubManager

  use ProviderBehaviour

  import ProviderBehaviour, only: [coerce_int: 1, parse_int: 2]

  # The one default table. `defstruct`, the `build/1` allow-list, and the worker
  # opts all derive from it, so a new tunable is added in exactly one place.
  @int_opts [
    queue_max: 10_000,
    retry_backoff_ms: 250,
    retry_max_backoff_ms: 5_000,
    call_timeout_ms: 5_000
  ]

  # Resolved by the host at prepare time, never settable by config.
  @injected_opts [:worker_pid, :source_namespace, :source_operative_id]

  @enforce_keys [:target_operative_id]
  defstruct @enforce_keys ++ [:plugin_id] ++ @injected_opts ++ @int_opts

  @known_opts Enum.map([:target_operative_id, :plugin_id | Keyword.keys(@int_opts)], &to_string/1)

  @impl true
  def plugin_type(), do: :io_binding

  defoverridable build: 1

  @doc """
  Reject unknown options rather than ignoring them, so a config asking for a
  delivery guarantee this provider deliberately does not offer fails loudly
  instead of reading as accepted.
  """
  @impl true
  def build(string_map) when is_map(string_map) do
    case ProviderBehaviour.reject_unknown(Map.keys(string_map), @known_opts, "operative_forward") do
      :ok -> super(string_map)
      err -> err
    end
  end

  def init_provider(opts) do
    source_namespace = Map.get(opts, :source_namespace)
    source_operative_id = Map.get(opts, :source_operative_id)
    target = Map.get(opts, :target_operative_id)

    with :ok <- validate_target(target, source_namespace, source_operative_id),
         plugin_id = Map.get(opts, :plugin_id) || default_plugin_id(source_operative_id, target),
         {:ok, worker_pid} <-
           ForwardSup.ensure_worker(
             [
               plugin_id: plugin_id,
               source_namespace: source_namespace,
               source_operative_id: source_operative_id,
               target_operative_id: target
             ] ++ tunables(opts)
           ) do
      opts =
        opts
        |> Map.put(:plugin_id, plugin_id)
        |> Map.put(:worker_pid, worker_pid)

      {:ok, opts, worker_pid}
    end
  end

  def on_input(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)
  def on_output(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)

  defp forward(%Ctx.IO{message: message, metadata: metadata} = ctx, opts) do
    :ok = ForwardWorker.enqueue(Map.fetch!(opts, :worker_pid), message, metadata)
    {:ok, ctx}
  end

  @doc """
  Health verdict: the worker being alive. Delivery failure is not unhealthiness
  — an unreachable peer is retried, and convergence is already healing it.
  """
  @impl true
  @spec health(map()) :: ProviderBehaviour.health_verdict()
  def health(opts) do
    ProviderBehaviour.worker_health(
      opts,
      [pid: [:worker_pid, :owned_pid], absent: :no_worker, dead: :worker_dead],
      fn _pid -> :healthy end
    )
  end

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
    HubManager.get_running_namespaces()
    |> Enum.map(&to_string/1)
    |> Enum.reject(&(&1 == to_string(namespace)))
    |> Enum.find(&resolves_in?(&1, target))
  end

  defp default_plugin_id(source, target), do: "#{source}->#{target}"

  defp tunables(opts) do
    Enum.map(@int_opts, fn {field, default} ->
      {field, coerce_int(Map.get(opts, field, default))}
    end)
  end

  @int_fields Keyword.keys(@int_opts)

  defp convert_value(field, value) when field in @int_fields, do: parse_int(value, field)
end
