defmodule Flezha.Service.RelayManager.Admission do
  @moduledoc """
  The checks a start must clear before a relay is created: the factory's
  placement validity and run, the refusals a partition group's base id earns,
  and the `:start` steps of the shutdown plan that take what the start holds
  (its quota, the disk check, the namespace claim) before the declared start.
  """

  @behaviour Flezha.Shutdown.Step

  alias Flezha.AuthZ.Quotas
  alias Flezha.Identificator
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.Validator
  alias Flezha.Shutdown
  alias Flezha.State.Paths

  @doc """
  Why no runner on any node can place a start of `factory_id`, as the reason
  the start is refused with.
  """
  def cluster_wide_refusal(namespace, factory_id) when is_binary(factory_id),
    do:
      placement_reason(
        FleetManager.placement_refusal(namespace, factory_id),
        namespace,
        factory_id
      )

  def cluster_wide_refusal(namespace, _factory_id), do: {:no_runners, namespace}

  @doc """
  The reason a start of `factory_id` is refused with, for a
  `FleetManager.placement_refusal/2` answer other than `:none`.

  A factory that exists in another language or run the other way is not a
  factory nobody has: reporting it as missing sends the user looking for a
  runner that is already connected. Every refusal site reads this, so they
  cannot drift.
  """
  @spec placement_reason(term(), String.t(), String.t()) :: tuple()
  def placement_reason({:language, established}, namespace, factory_id),
    do: {:factory_language_unserved, namespace, factory_id, established}

  def placement_reason(:no_language_declared, namespace, factory_id),
    do: {:factory_language_undeclared, namespace, factory_id}

  def placement_reason(:ambiguous_language, namespace, factory_id),
    do: {:factory_language_ambiguous, namespace, factory_id}

  def placement_reason({:run, established}, namespace, factory_id),
    do: {:factory_run_unserved, namespace, factory_id, established}

  def placement_reason(:no_run_declared, namespace, factory_id),
    do: {:factory_run_undeclared, namespace, factory_id}

  def placement_reason(:ambiguous_run, namespace, factory_id),
    do: {:factory_run_ambiguous, namespace, factory_id}

  def placement_reason(_absent, namespace, factory_id),
    do: {:factory_unknown, namespace, factory_id}

  @doc """
  Admit the start's factory and take its run: a runner here must be able to
  place it, and the run is the one its runners announced, which no caller
  names. A run a caller asserts (a spawn or a schedule fire, both
  steps-only) is judged against it rather than taken.
  """
  @spec resolve_factory(String.t(), map()) :: {:ok, map()} | {:error, tuple()}
  def resolve_factory(namespace, %{factory_id: factory_id, run: asserted} = relay)
      when is_binary(factory_id) do
    case FleetManager.factory_run(namespace, factory_id) do
      {:ok, run} when asserted in [nil, run] ->
        {:ok, %{relay | run: run}}

      {:ok, run} ->
        {:error, {:factory_run_mismatch, asserted, namespace, factory_id, run}}

      {:error, refusal} ->
        {:error, placement_reason(refusal, namespace, factory_id)}
    end
  end

  def resolve_factory(_namespace, relay), do: {:ok, relay}

  @doc """
  Whether a `generated?` id names nothing yet: no partition group and no state
  directory in the namespace. A finished steps relay keeps its directory for its
  retention window, and a start over it would read the old journal as its own.
  A caller's own id passes, because its directory is the ordinary restore.
  """
  @spec check_generated_id_free(String.t(), String.t(), boolean()) :: :ok | {:error, :id_taken}
  def check_generated_id_free(_namespace, _relay_id, false), do: :ok

  def check_generated_id_free(namespace, relay_id, true) do
    if PartitionGroup.members(namespace, relay_id) == [] and
         not Paths.class_on_disk?(relay_id, namespace),
       do: :ok,
       else: {:error, :id_taken}
  end

  # A group's base id is never claimed — only member ids are — so without this
  # a plain start of the base mints a singleton beside the group. Member starts
  # carry partition identity in their opts and skip the registry walk.
  def refuse_group_base_claim(namespace, relay) do
    case Flezha.Partition.identity(relay.opts) do
      nil -> refuse_live_group(namespace, relay.id)
      _member_identity -> :ok
    end
  end

  defp refuse_live_group(namespace, relay_id) do
    case PartitionGroup.members(namespace, relay_id) do
      [] ->
        :ok

      members ->
        {:error, {:group_base_claim, relay_id, length(members), hd(members).partition.count}}
    end
  end

  @impl Flezha.Shutdown.Step
  def shutdown_steps do
    [
      %{scope: :start, order: 10, name: :reserve, run: &reserve_step/2},
      %{scope: :start, order: 20, name: :disk, run: &disk_step/2},
      %{scope: :start, order: 30, name: :namespace_claim, run: &namespace_claim_step/2}
    ]
  end

  @doc """
  Whether `namespace` has room for `needed` more relays, counted from the
  cluster's live relays; `admit` is the check, or the take a start makes.
  """
  def check_relay_quota(namespace, needed \\ 1, admit \\ &Quotas.check_allocation/3) do
    if Flezha.AuthZ.enabled?(),
      do: _ = Quotas.refresh_relays(namespace, fn -> live_relays(namespace) end)

    check_quota(namespace, :max_relays, needed, admit)
  end

  defp live_relays(namespace), do: Enum.map(HubManager.get_all_relays(namespace), &elem(&1, 0))

  @doc "Close a start's quota take: kept once its relay runs, else given back."
  @spec settle_start(String.t(), boolean()) :: :ok
  def settle_start(namespace, kept?) do
    Enum.each(
      [:max_relays, :max_cells_per_relay],
      &Quotas.settle_allocation(namespace, &1, kept?)
    )
  end

  # The relay and its Cells, each checked and counted in one step, so two
  # concurrent starts cannot both fit in the last unit.
  defp reserve_step(%{namespace: namespace, relay: relay}, _budget) do
    with :ok <- check_relay_quota(namespace, [relay.id], &Quotas.record_allocation/3) do
      case cell_count(relay) do
        0 -> :ok
        cells -> check_quota(namespace, :max_cells_per_relay, cells, &Quotas.record_allocation/3)
      end
    end
  end

  # A namespace over its disk quota refuses NEW allocation — running
  # relays and their in-flight durable writes are never interrupted
  # (the Watchdog raises the alarm for those).
  defp disk_step(%{namespace: namespace}, _budget) do
    if Flezha.AuthZ.enabled?() do
      _ = Quotas.refresh_disk_bytes(namespace)
      check_quota(namespace, :max_state_bytes, 0, &Quotas.check_allocation/3)
    else
      :ok
    end
  end

  defp namespace_claim_step(%{namespace: namespace, relay: relay}, _budget),
    do: Paths.claim_namespace(relay.id, namespace)

  # An unregistered namespace streams: quotas gate registered tenants, and
  # refusing every legacy/implicit namespace here would brick starts the
  # moment enforcement flips on.
  defp check_quota(namespace, kind, requested, admit) do
    case admit.(namespace, kind, requested) do
      :ok -> :ok
      {:error, {:namespace_unknown, _}} -> :ok
      {:error, _} = err -> err
    end
  end

  def check_not_shutting_down(namespace), do: Shutdown.admit(:namespace, namespace)

  def validate_relay(namespace, relay) do
    with :ok <- relay_id_within_limit(relay.id),
         :ok <- Validator.valid_namespace(namespace) do
      Validator.relay_unique(namespace, relay.id)
    end
  end

  defp relay_id_within_limit(id) do
    case Identificator.validate(:relay, id) do
      :ok -> :ok
      {:error, _refusal} -> {:error, {:id_too_long, id, Identificator.limit(:relay)}}
    end
  end

  def cell_count(relay) do
    case Map.get(relay, :cells) do
      ps when is_list(ps) -> length(ps)
      n when is_integer(n) -> n
      _ -> 0
    end
  end
end
