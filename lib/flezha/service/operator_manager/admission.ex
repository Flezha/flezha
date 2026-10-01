defmodule Flezha.Service.OperatorManager.Admission do
  @moduledoc """
  The checks a start must clear before an operator is created: the three
  quotas, the factory's placement validity and run, and the refusals a
  partition group's base id earns.
  """

  alias Flezha.AuthZ.Quotas
  alias Flezha.Identificator
  alias Flezha.Operator.PartitionGroup
  alias Flezha.Runner.FleetManager
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

  # A factory that exists in another language or run the other way is not a
  # factory nobody has: reporting it as missing sends the user looking for a
  # runner that is already connected. Both refusal sites read this, so the two
  # cannot drift.
  defp placement_reason({:language, established}, namespace, factory_id),
    do: {:factory_language_unserved, namespace, factory_id, established}

  defp placement_reason(:no_language_declared, namespace, factory_id),
    do: {:factory_language_undeclared, namespace, factory_id}

  defp placement_reason(:ambiguous_language, namespace, factory_id),
    do: {:factory_language_ambiguous, namespace, factory_id}

  defp placement_reason({:run, established}, namespace, factory_id),
    do: {:factory_run_unserved, namespace, factory_id, established}

  defp placement_reason(:no_run_declared, namespace, factory_id),
    do: {:factory_run_undeclared, namespace, factory_id}

  defp placement_reason(:ambiguous_run, namespace, factory_id),
    do: {:factory_run_ambiguous, namespace, factory_id}

  defp placement_reason(_absent, namespace, factory_id),
    do: {:factory_unknown, namespace, factory_id}

  @doc """
  Admit the start's factory and take its run: a runner here must be able to
  place it, and the run is the one its runners announced, which no caller
  names. A run a caller asserts (a spawn or a schedule fire, both
  steps-only) is judged against it rather than taken.
  """
  @spec resolve_factory(String.t(), map()) :: {:ok, map()} | {:error, tuple()}
  def resolve_factory(namespace, %{factory_id: factory_id, run: asserted} = operator)
      when is_binary(factory_id) do
    case FleetManager.factory_run(namespace, factory_id) do
      {:ok, run} when asserted in [nil, run] ->
        {:ok, %{operator | run: run}}

      {:ok, run} ->
        {:error, {:factory_run_mismatch, asserted, namespace, factory_id, run}}

      {:error, refusal} ->
        {:error, placement_reason(refusal, namespace, factory_id)}
    end
  end

  def resolve_factory(_namespace, operator), do: {:ok, operator}

  @doc """
  Whether a `generated?` id names nothing yet: no partition group and no state
  directory in the namespace. A finished steps operator keeps its directory for its
  retention window, and a start over it would read the old journal as its own.
  A caller's own id passes, because its directory is the ordinary restore.
  """
  @spec check_generated_id_free(String.t(), String.t(), boolean()) :: :ok | {:error, :id_taken}
  def check_generated_id_free(_namespace, _operator_id, false), do: :ok

  def check_generated_id_free(namespace, operator_id, true) do
    if PartitionGroup.members(namespace, operator_id) == [] and
         not Paths.class_on_disk?(operator_id, namespace),
       do: :ok,
       else: {:error, :id_taken}
  end

  # A group's base id is never claimed — only member ids are — so without this
  # a plain start of the base mints a singleton beside the group. Member starts
  # carry partition identity in their opts and skip the registry walk.
  def refuse_group_base_claim(namespace, operator) do
    case Flezha.Partition.identity(operator.opts) do
      nil -> refuse_live_group(namespace, operator.id)
      _member_identity -> :ok
    end
  end

  defp refuse_live_group(namespace, operator_id) do
    case PartitionGroup.members(namespace, operator_id) do
      [] ->
        :ok

      members ->
        {:error, {:group_base_claim, operator_id, length(members), hd(members).partition.count}}
    end
  end

  def check_operator_quota(namespace) do
    check_quota(namespace, :max_operators, 1)
  end

  # A namespace over its disk quota refuses NEW allocation — running
  # operators and their in-flight durable writes are never interrupted
  # (the Watchdog raises the alarm for those).
  def check_disk_quota(namespace) do
    if Flezha.AuthZ.enabled?() do
      _ = Quotas.refresh_disk_bytes(namespace)
      check_quota(namespace, :max_state_bytes, 0)
    else
      :ok
    end
  end

  def check_cell_quota(namespace, operator) do
    case cell_count(operator) do
      0 -> :ok
      requested -> check_quota(namespace, :max_cells_per_operator, requested)
    end
  end

  # An unregistered namespace streams: quotas gate registered tenants, and
  # refusing every legacy/implicit namespace here would brick starts the
  # moment enforcement flips on.
  defp check_quota(namespace, kind, requested) do
    case Quotas.check_allocation(namespace, kind, requested) do
      :ok -> :ok
      {:error, :namespace_unknown} -> :ok
      {:error, _} = err -> err
    end
  end

  def check_not_shutting_down(namespace), do: Shutdown.admit(:namespace, namespace)

  def validate_operator(namespace, operator) do
    with :ok <- operator_id_within_limit(operator.id),
         :ok <- Validator.valid_namespace(namespace) do
      Validator.operator_unique(namespace, operator.id)
    end
  end

  defp operator_id_within_limit(id) do
    case Identificator.validate(:operator, id) do
      :ok -> :ok
      {:error, _refusal} -> {:error, {:id_too_long, id, Identificator.limit(:operator)}}
    end
  end

  def cell_count(operator) do
    case Map.get(operator, :cells) do
      ps when is_list(ps) -> length(ps)
      n when is_integer(n) -> n
      _ -> 0
    end
  end
end
