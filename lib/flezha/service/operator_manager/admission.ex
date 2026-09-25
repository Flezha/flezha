defmodule Flezha.Service.OperatorManager.Admission do
  @moduledoc """
  The checks a start must clear before an operator is created: the three
  quotas, the factory's placement validity and kind, and the refusals a
  partition group's base id earns.
  """

  alias Flezha.AuthZ.Quotas
  alias Flezha.Identificator
  alias Flezha.Operator.PartitionGroup
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.Validator
  alias Flezha.Shutdown
  alias Flezha.State.Paths

  def cluster_wide_refusal(namespace, factory_id) when is_binary(factory_id) do
    factory_refusal(
      namespace,
      factory_id,
      "no runners for factory #{factory_id} in namespace #{namespace} on any cluster node"
    )
  end

  def cluster_wide_refusal(namespace, _factory_id),
    do: "no runners in namespace #{namespace} on any cluster node"

  # A factory that exists in another language or as the other kind is not a
  # factory nobody has: reporting it as missing sends the user looking for a
  # runner that is already connected. Both refusal sites read this, so the two
  # cannot drift.
  defp factory_refusal(namespace, factory_id, absent),
    do:
      refusal_text(
        FleetManager.placement_refusal(namespace, factory_id),
        namespace,
        factory_id,
        absent
      )

  defp refusal_text(refusal, namespace, factory_id, absent) do
    case refusal do
      {:language, established} ->
        "factory #{factory_id} in namespace #{namespace} is established at sdk_lang " <>
          "#{inspect(established)}; every runner advertising it is of another SDK and " <>
          "cannot read its state — start a runner of that language, or clear the " <>
          "factory's durable state so it re-establishes"

      :no_language_declared ->
        "factory #{factory_id} in namespace #{namespace} is advertised without an sdk_lang " <>
          "declaration; a runner that does not declare its language is never a placement " <>
          "candidate, because durable state is per-SDK — declare sdk_lang in the runner's " <>
          "factory advertisement"

      :ambiguous_language ->
        "factory #{factory_id} in namespace #{namespace} is advertised at more than one " <>
          "sdk_lang with none established; the controller cannot tell which language owns " <>
          "its state — run one language's runners, or clear the factory's durable state"

      {:kind, established} ->
        "factory #{factory_id} in namespace #{namespace} is established as " <>
          "#{established}, and every runner advertising it announces the other kind; run " <>
          "runners whose class of that id is #{established}"

      :no_kind_declared ->
        "factory #{factory_id} in namespace #{namespace} is advertised without a kind " <>
          "declaration; a runner that does not announce its class's kind is never a " <>
          "placement candidate, since the class declares it and its runner announces it"

      :ambiguous_kind ->
        "factory #{factory_id} in namespace #{namespace} is announced as both unbounded and " <>
          "bounded with neither established; run runners of one kind for it"

      _ ->
        absent
    end
  end

  @doc """
  Admit the start's factory and take its kind: a runner here must be able to
  place it, and the kind is the one its runners announced, which no caller
  names. A kind a caller asserts (a spawn or a schedule fire, both
  bounded-only) is judged against it rather than taken.
  """
  @spec resolve_factory(String.t(), map()) :: {:ok, map()} | {:error, String.t()}
  def resolve_factory(namespace, %{factory_id: factory_id, kind: asserted} = operator)
      when is_binary(factory_id) do
    case FleetManager.factory_kind(namespace, factory_id) do
      {:ok, kind} when asserted in [nil, kind] ->
        {:ok, %{operator | kind: kind}}

      {:ok, kind} ->
        {:error,
         "factory #{factory_id} in namespace #{namespace} is #{kind}, and only " <>
           "#{asserted} is started here"}

      {:error, refusal} ->
        absent = "Factory with id #{factory_id} does not exist"
        {:error, refusal_text(refusal, namespace, factory_id, absent)}
    end
  end

  def resolve_factory(_namespace, operator), do: {:ok, operator}

  @doc """
  Whether a `generated?` id names nothing yet: no partition group and no state
  directory in the namespace. A finished bounded operator keeps its directory for its
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
        {:error,
         "#{operator_id} names a partition group of #{length(members)} live member(s); " <>
           "re-run with --partitions #{hd(members).partition.count} to converge it, or stop " <>
           "the group first"}
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
         :ok <- Validator.valid_namespace(namespace),
         :ok <- Validator.operator_unique(namespace, operator.id) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # The rule is the `:operator` row; the wording is this surface's, unchanged.
  defp operator_id_within_limit(id) do
    case Identificator.validate(:operator, id) do
      :ok ->
        :ok

      {:error, _refusal} ->
        {:error, "operator id exceeds #{Identificator.limit(:operator)} bytes"}
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
