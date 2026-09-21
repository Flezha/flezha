defmodule Flezha.Runner.FleetManager.Factories do
  @moduledoc """
  The per-namespace factory table and the SDK language each factory is
  established at.

  A factory is advertised by one or more runners; the language is established by
  the first advertisement that names one and never changes afterwards.
  Invariant: a runner advertising a factory at a language other than the
  established one is not a placement candidate for it — durable State/Change
  bodies are per-SDK and are never decoded across languages.
  """

  alias Flezha.Operative.ClassRegistry
  alias Flezha.Runner.FleetManager.Roster
  alias Flezha.Service.HubManager

  require Logger

  # An advertisement carrying no language. Never places: the runner most likely
  # to be untagged is the one most likely to be mismatched.
  @sdk_lang_unspecified 0

  def store_factories(state, namespace, runner_id, factories) do
    ns_factories = Map.get(state.namespace_factories, namespace, %{})

    ns_factories =
      Enum.reduce(factories, ns_factories, fn factory, acc ->
        factory_id = factory["id"]

        existing = factory_entry(Map.get(acc, factory_id))
        description = factory["description"] || existing.description
        advertised = advertised_lang(factory)

        # Stored even when it conflicts, so operator tooling can see the
        # runner that is advertising the wrong language; `placeable_runners/3`
        # is what refuses it.
        _ = establish_lang(namespace, runner_id, factory_id, advertised)

        Map.put(acc, factory_id, %{
          description: description,
          ce_ids: MapSet.put(existing.ce_ids, runner_id),
          langs: Map.put(existing.langs, runner_id, advertised)
        })
      end)

    put_in(state, [:namespace_factories, namespace], ns_factories)
  end

  def remove_ce_factories(state, namespace, runner_id) do
    ns_factories = Map.get(state.namespace_factories, namespace, %{})

    ns_factories =
      ns_factories
      |> Map.new(fn {factory_id, info} ->
        info = factory_entry(info)

        {factory_id,
         %{
           info
           | ce_ids: MapSet.delete(info.ce_ids, runner_id),
             langs: Map.delete(info.langs, runner_id)
         }}
      end)

    put_in(state, [:namespace_factories, namespace], ns_factories)
  end

  # Drop the factories no runner advertises any more, releasing the language
  # each established. A pass of its own, run AFTER any store: a full-set resync
  # removes the runner's whole list before re-adding it, and releasing inside
  # the removal would discard a language the durable header had corrected.
  def prune_empty_factories(state, namespace) do
    {empty, kept} =
      state.namespace_factories
      |> Map.get(namespace, %{})
      |> Map.split_with(fn {_factory_id, info} -> MapSet.size(info.ce_ids) == 0 end)

    Enum.each(Map.keys(empty), &ClassRegistry.delete_sdk_lang(namespace, &1))
    put_in(state, [:namespace_factories, namespace], kept)
  end

  # The one constructor for a stored factory entry. Total on an older entry: a
  # `KeyError` here would take the whole FleetManager down over a shape
  # question, and every placement lookup with it.
  def factory_entry(nil), do: %{description: nil, ce_ids: MapSet.new(), langs: %{}}

  def factory_entry(%{} = info) do
    info
    |> Map.put_new(:description, nil)
    |> Map.put_new(:ce_ids, MapSet.new())
    |> Map.put_new(:langs, %{})
  end

  # The language a factory entry advertises. Absent / `0` stays `0`: an
  # unspecified language is refused at the placement filter, never guessed.
  def advertised_lang(factory) do
    case factory["sdk_lang"] do
      lang when is_integer(lang) and lang > 0 -> lang
      _ -> @sdk_lang_unspecified
    end
  end

  # An advertisement establishes the factory's language if nothing has yet, and
  # is reported when it disagrees. The one place a language is RECORDED — the
  # record is a durable sidecar, so writing it belongs on this path and never
  # in a query.
  #
  # The durable half of the resolution is read at the BIND
  # (`Flezha.Operative.Lifecycle`): a disk walk here would stall every
  # concurrent placement lookup behind the connect path.
  def establish_lang(_ns, _runner_id, _factory_id, @sdk_lang_unspecified), do: :ok

  def establish_lang(namespace, runner_id, factory_id, advertised) do
    case ClassRegistry.sdk_lang(namespace, factory_id) do
      nil ->
        ClassRegistry.put_sdk_lang(namespace, factory_id, advertised)

      established when established != advertised ->
        Logger.warning(
          "[fleet_manager] runner #{runner_id} on #{namespace} advertises factory " <>
            "#{factory_id} at sdk_lang #{advertised}, but the factory is established " <>
            "at #{established}; it will not be a placement candidate for it — durable " <>
            "State/Change bodies are per-SDK and are never decoded across languages"
        )

        :telemetry.execute(
          [:flezha, :runner, :factory_language_conflict],
          %{count: 1},
          %{
            namespace: namespace,
            runner: runner_id,
            factory: factory_id,
            established: established,
            advertised: advertised
          }
        )

      _agreed ->
        :ok
    end
  end

  # Factory ids this runner advertised before a sync that are absent from the
  # incoming full set.
  def dropped_factory_ids(state, namespace, runner_id, factories) do
    new_ids = MapSet.new(factories, & &1["id"])

    state.namespace_factories
    |> Map.get(namespace, %{})
    |> Enum.filter(fn {factory_id, info} ->
      MapSet.member?(info.ce_ids, runner_id) and not MapSet.member?(new_ids, factory_id)
    end)
    |> Enum.map(fn {factory_id, _info} -> factory_id end)
  end

  # Tripwire for the suspended-instance removal hole: a dropped factory with
  # controller-known operatives still bound to this runner — or suspended
  # (runner tag nil) with no other active runner advertising the factory —
  # escaped the SDK-side live-instance check, and its rehydration will fail
  # placement. Warn only; a blocking policy is deferred.
  def warn_dropped_factories(state, namespace, runner_id, dropped) do
    Enum.each(dropped, fn factory_id ->
      case affected_operatives(state, namespace, runner_id, factory_id) do
        [] ->
          :ok

        affected ->
          Logger.warning(
            "[fleet_manager] runner #{runner_id} on #{namespace} dropped factory " <>
              "#{factory_id} that still has controller-known operatives " <>
              "#{inspect(affected)}; their (re)placement will fail until a " <>
              "runner re-defines the factory"
          )
      end
    end)
  end

  def affected_operatives(state, namespace, runner_id, factory_id) do
    still_placeable = MapSet.size(placeable_runners(state, namespace, factory_id)) > 0

    namespace
    |> HubManager.operative_rows()
    |> Enum.filter(fn row ->
      row.factory == factory_id and
        (row.runner == runner_id or (is_nil(row.runner) and not still_placeable))
    end)
    |> Enum.map(& &1.id)
  end

  @doc """
  The factories of `namespace` this node can actually place on, which is what
  makes its controller an eligible host for their operatives.

  `placeable_runners/3` over every factory the namespace holds, so this set
  cannot disagree with the runner choice: a factory advertised here only at a
  language other than the established one is in neither.
  """
  @spec placeable_factories(map(), String.t()) :: MapSet.t()
  def placeable_factories(state, namespace) do
    state.namespace_factories
    |> Map.get(namespace, %{})
    |> Map.keys()
    |> Enum.filter(&(MapSet.size(placeable_runners(state, namespace, &1)) > 0))
    |> MapSet.new()
  end

  # ACTIVE runners advertising `factory_id` **in that factory's established SDK
  # language** — the one filter every placement path reads (initial start, the
  # convergence rebind, the reroute off a dead runner), so a future caller
  # inherits the guard rather than having to remember it.
  #
  # The homogeneous unit is the FACTORY, not the namespace: a restore ships one
  # SDK's bytes verbatim to whichever runner bound the Cell, and only one
  # factory id's implementations must never mix.
  def placeable_runners(state, namespace, factory_id) do
    {info, candidates} = advertising_runners(state, namespace, factory_id)

    case established_lang(namespace, factory_id, info, candidates) do
      # An entry carrying no language never makes its runner a candidate, and
      # disagreeing advertisements with nothing recorded leave the controller
      # unable to say which language owns the state. Neither places: durable
      # State/Change bodies are per-SDK and are never decoded across languages.
      reason when reason in [:none, :ambiguous] ->
        MapSet.new()

      {:ok, established} ->
        MapSet.filter(
          candidates,
          &(Map.get(info.langs, &1, @sdk_lang_unspecified) == established)
        )
    end
  end

  # The factories `runner_id` advertises in `namespace`, at any language. The
  # inverse read of `advertising_runners/3` over the same table: the release
  # path asks what one runner can take, where a placement asks who can take one
  # operative. Unfiltered by language on purpose — a mismatched advertisement
  # must not place, but it is not evidence that the runner is irrelevant, and
  # the bind itself is where the language guard belongs.
  def runner_factories(state, namespace, runner_id) do
    state.namespace_factories
    |> Map.get(namespace, %{})
    |> Enum.filter(fn {_factory_id, info} ->
      MapSet.member?(factory_entry(info).ce_ids, runner_id)
    end)
    |> MapSet.new(fn {factory_id, _info} -> factory_id end)
  end

  # The stored entry and the ACTIVE runners advertising `factory_id` at any
  # language — what separates "the factory exists in another language" from "no
  # runner has this factory at all". `placeable_runners/3` is this set narrowed
  # to one language, so the two never disagree on who is even a candidate.
  def advertising_runners(state, namespace, factory_id) do
    active_ids = MapSet.new(Roster.get_runner_ids(state, namespace, :active))

    info =
      state.namespace_factories
      |> Map.get(namespace, %{})
      |> Map.get(factory_id)
      |> factory_entry()

    {info, MapSet.intersection(info.ce_ids, active_ids)}
  end

  # A factory's established language: the record if there is one, else what its
  # own live advertisements say. Both halves of the record are losable (the ETS
  # table is recreated empty if its owner restarts, the sidecar goes with the
  # factory's state dir), and a missing record is no evidence of a conflict —
  # so it must not read as one. Derives, never records: this is a query.
  #
  # Unanimity required: disagreeing advertisements with nothing recorded mean
  # the controller cannot tell which language owns the factory.
  def established_lang(namespace, factory_id, info, candidates) do
    case ClassRegistry.sdk_lang(namespace, factory_id) do
      nil ->
        case advertised_langs(info, candidates) do
          [] -> :none
          [lang] -> {:ok, lang}
          _conflicting -> :ambiguous
        end

      recorded ->
        {:ok, recorded}
    end
  end

  # The distinct non-zero languages the live candidates advertise. An
  # unspecified advertisement contributes nothing: it can neither establish a
  # factory nor make one ambiguous.
  def advertised_langs(info, candidates) do
    candidates
    |> Enum.map(&Map.get(info.langs, &1, @sdk_lang_unspecified))
    |> Enum.reject(&(&1 == @sdk_lang_unspecified))
    |> Enum.uniq()
  end
end
