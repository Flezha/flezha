defmodule Flezha.Service.OperativeManager.Admission do
  @moduledoc """
  The checks a start must clear before an operative is created: the three
  quotas, the factory's placement validity, and the refusals a partition group's
  base id earns.
  """

  alias Flezha.AuthZ.Quotas
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Runner.FleetManager
  alias Flezha.Shutdown.Coordinator, as: ShutdownCoordinator

  def cluster_wide_refusal(namespace, factory_id) when is_binary(factory_id) do
    factory_refusal(
      namespace,
      factory_id,
      "no runners for factory #{factory_id} in namespace #{namespace} on any cluster node"
    )
  end

  def cluster_wide_refusal(namespace, _factory_id),
    do: "no runners in namespace #{namespace} on any cluster node"

  # A factory that exists in another language is not a factory nobody has:
  # reporting it as missing sends the operator looking for a runner that is
  # already connected. Both refusal sites read this, so the two cannot drift.
  defp factory_refusal(namespace, factory_id, absent) do
    case FleetManager.placement_refusal(namespace, factory_id) do
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

      _ ->
        absent
    end
  end

  # A group's base id is never claimed — only member ids are — so without this
  # a plain start of the base mints a singleton beside the group. Member starts
  # carry partition identity in their opts and skip the registry walk.
  def refuse_group_base_claim(namespace, operative) do
    case Flezha.Partition.identity(operative.opts) do
      nil -> refuse_live_group(namespace, operative.id)
      _member_identity -> :ok
    end
  end

  defp refuse_live_group(namespace, operative_id) do
    case PartitionGroup.members(namespace, operative_id) do
      [] ->
        :ok

      members ->
        {:error,
         "#{operative_id} names a partition group of #{length(members)} live member(s); " <>
           "re-run with --partitions #{hd(members).partition.count} to converge it, or stop " <>
           "the group first"}
    end
  end

  def check_operative_quota(namespace) do
    check_quota(namespace, :max_operatives, 1)
  end

  # A namespace over its disk quota refuses NEW allocation — running
  # operatives and their in-flight durable writes are never interrupted
  # (the Watchdog raises the alarm for those).
  def check_disk_quota(namespace) do
    if Flezha.AuthZ.enabled?() do
      _ = Quotas.refresh_disk_bytes(namespace)
      check_quota(namespace, :max_state_bytes, 0)
    else
      :ok
    end
  end

  def check_entity_quota(namespace, operative) do
    case entity_count(operative) do
      0 -> :ok
      requested -> check_quota(namespace, :max_entities_per_operative, requested)
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

  def check_not_shutting_down() do
    if ShutdownCoordinator.shutting_down?(), do: {:error, :shutting_down}, else: :ok
  end

  def validate_factory(namespace, factory_id) when is_binary(factory_id) do
    case FleetManager.runners_for_factory(namespace, factory_id) do
      [_ | _] ->
        :ok

      [] ->
        {:error,
         factory_refusal(namespace, factory_id, "Factory with id #{factory_id} does not exist")}
    end
  end

  def validate_factory(_namespace, _factory_id), do: :ok

  def entity_count(operative) do
    case Map.get(operative, :entities) do
      ps when is_list(ps) -> length(ps)
      n when is_integer(n) -> n
      _ -> 0
    end
  end
end
