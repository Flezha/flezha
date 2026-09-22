defmodule Flezha.Runner.FleetManager.Factories do
  @moduledoc """
  The per-namespace factory table, and the SDK language and kind each factory
  is established at.

  A factory is advertised by one or more runners; its language and its kind are
  each established by the first advertisement that names one and never change
  afterwards. Invariant: a runner advertising a factory at a language or kind
  other than the established one is not a placement candidate for it — durable
  State/Change bodies are per-SDK and are never decoded across languages, and
  every instance of a factory is of the one kind its class declares.
  """

  alias Flezha.Operative.ClassRegistry
  alias Flezha.Runner.FleetManager.Roster
  alias Flezha.Service.HubManager

  require Logger

  # The value of each established attribute that says nothing: an
  # advertisement carrying it never places, since the runner most likely to be
  # untagged is the one most likely to be mismatched.
  @unspecified %{sdk_lang: 0, kind: nil}

  def store_factories(state, namespace, runner_id, factories) do
    ns_factories = Map.get(state.namespace_factories, namespace, %{})

    ns_factories =
      Enum.reduce(factories, ns_factories, fn factory, acc ->
        factory_id = factory["id"]

        existing = factory_entry(Map.get(acc, factory_id))
        advert = %{sdk_lang: advertised_lang(factory), kind: advertised_kind(factory)}

        # Stored even when it conflicts, so operator tooling can see the
        # runner that is advertising the wrong language or kind;
        # `placeable_runners/3` is what refuses it.
        Enum.each(advert, fn {attribute, value} ->
          establish(namespace, runner_id, factory_id, attribute, value)
        end)

        Map.put(acc, factory_id, %{
          description: factory["description"] || existing.description,
          adverts: Map.put(existing.adverts, runner_id, advert)
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

        {factory_id, %{info | adverts: Map.delete(info.adverts, runner_id)}}
      end)

    put_in(state, [:namespace_factories, namespace], ns_factories)
  end

  # Drop the factories no runner advertises any more, releasing what each
  # established. A pass of its own, run AFTER any store: a full-set resync
  # removes the runner's whole list before re-adding it, and releasing inside
  # the removal would discard a language the durable header had corrected.
  def prune_empty_factories(state, namespace) do
    {empty, kept} =
      state.namespace_factories
      |> Map.get(namespace, %{})
      |> Map.split_with(fn {_factory_id, info} -> map_size(factory_entry(info).adverts) == 0 end)

    Enum.each(Map.keys(empty), &ClassRegistry.delete_established(namespace, &1))
    put_in(state, [:namespace_factories, namespace], kept)
  end

  # The one constructor for a stored factory entry: its description and, per
  # advertising runner, what that runner announced (`%{sdk_lang:, kind:}`).
  # Total on an older entry: a `KeyError` here would take the whole
  # FleetManager down over a shape question, and every placement lookup with it.
  def factory_entry(nil), do: %{description: nil, adverts: %{}}

  def factory_entry(%{} = info) do
    info
    |> Map.put_new(:description, nil)
    |> Map.put_new(:adverts, %{})
  end

  # The language a factory entry advertises. Absent / `0` stays `0`: an
  # unspecified language is refused at the placement filter, never guessed.
  defp advertised_lang(factory) do
    case factory["sdk_lang"] do
      lang when is_integer(lang) and lang > 0 -> lang
      _ -> @unspecified.sdk_lang
    end
  end

  # The kind a factory entry advertises. Absent, empty or unknown stays
  # unspecified and is refused at the placement filter, as a missing language
  # is; only a kind this controller knows becomes an atom.
  defp advertised_kind(factory) do
    case factory["kind"] do
      "stream" -> :stream
      "workflow" -> :workflow
      _ -> @unspecified.kind
    end
  end

  # An advertisement establishes the factory's `attribute` if nothing has yet,
  # and is reported when it disagrees. The one place either is RECORDED, so
  # writing it belongs on this path and never in a query.
  #
  # The durable half of the language's resolution is read at the BIND
  # (`Flezha.Operative.Lifecycle`): a disk walk here would stall every
  # concurrent placement lookup behind the connect path.
  defp establish(namespace, runner_id, factory_id, attribute, advertised) do
    if advertised == Map.fetch!(@unspecified, attribute),
      do: :ok,
      else: record(namespace, runner_id, factory_id, attribute, advertised)
  end

  defp record(namespace, runner_id, factory_id, attribute, advertised) do
    case ClassRegistry.established(namespace, factory_id, attribute) do
      nil ->
        ClassRegistry.put_established(namespace, factory_id, attribute, advertised)

      established when established != advertised ->
        report_conflict(attribute, %{
          namespace: namespace,
          runner: runner_id,
          factory: factory_id,
          established: established,
          advertised: advertised
        })

      _agreed ->
        :ok
    end
  end

  # How a disagreeing advertisement of each attribute is reported. Each event
  # name is a literal here so the telemetry export seam can read it.
  defp report_conflict(:sdk_lang, meta),
    do:
      conflict(
        :factory_language_conflict,
        "at sdk_lang",
        "durable State/Change bodies are per-SDK and are never decoded across languages",
        meta
      )

  defp report_conflict(:kind, meta),
    do:
      conflict(
        :factory_kind_conflict,
        "as kind",
        "every instance of a factory is of the one kind its class declares",
        meta
      )

  defp conflict(event, phrase, reason, meta) do
    Logger.warning(
      "[fleet_manager] runner #{meta.runner} on #{meta.namespace} advertises factory " <>
        "#{meta.factory} #{phrase} #{meta.advertised}, but the factory is established " <>
        "#{phrase} #{meta.established}; it will not be a placement candidate for it — " <>
        reason
    )

    :telemetry.execute([:flezha, :runner, event], %{count: 1}, meta)
  end

  # Factory ids this runner advertised before a sync that are absent from the
  # incoming full set.
  def dropped_factory_ids(state, namespace, runner_id, factories) do
    new_ids = MapSet.new(factories, & &1["id"])

    state.namespace_factories
    |> Map.get(namespace, %{})
    |> Enum.filter(fn {factory_id, info} ->
      Map.has_key?(factory_entry(info).adverts, runner_id) and
        not MapSet.member?(new_ids, factory_id)
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

  # ACTIVE runners advertising `factory_id` **at that factory's established SDK
  # language and kind** — the one filter every placement path reads (initial
  # start, the convergence rebind, the reroute off a dead runner), so a future
  # caller inherits the guard rather than having to remember it.
  #
  # The homogeneous unit is the FACTORY, not the namespace: a restore ships one
  # SDK's bytes verbatim to whichever runner bound the Cell, and only one
  # factory id's implementations must never mix.
  def placeable_runners(state, namespace, factory_id) do
    case placement(state, namespace, factory_id) do
      {:ok, runners} -> runners
      {:refused, _attribute, _verdict} -> MapSet.new()
    end
  end

  # The placement filter with its reason: the runners it leaves, or the first
  # established attribute that leaves none, with what the factory is
  # established at (`{:ok, value}`), or `:none` / `:ambiguous` when nothing
  # decides it. An entry carrying no value never makes its runner a candidate,
  # and disagreeing advertisements with nothing recorded leave the controller
  # unable to say which value owns the factory. Neither places.
  def placement(state, namespace, factory_id) do
    {info, candidates} = advertising_runners(state, namespace, factory_id)

    Enum.reduce_while([:sdk_lang, :kind], {:ok, candidates}, fn attribute, {:ok, runners} ->
      verdict = established(namespace, factory_id, attribute, info, candidates)
      narrowed = narrow(runners, info, attribute, verdict)

      if Enum.empty?(narrowed) and not Enum.empty?(runners),
        do: {:halt, {:refused, attribute, verdict}},
        else: {:cont, {:ok, narrowed}}
    end)
  end

  defp narrow(runners, info, attribute, {:ok, established}),
    do: MapSet.filter(runners, &(get_in(info.adverts, [&1, attribute]) == established))

  defp narrow(_runners, _info, _attribute, _undecided), do: MapSet.new()

  # The factories `runner_id` advertises in `namespace`, at any language or
  # kind. The inverse read of `advertising_runners/3` over the same table: the
  # release path asks what one runner can take, where a placement asks who can
  # take one operative. Unfiltered on purpose — a mismatched advertisement must
  # not place, but it is not evidence that the runner is irrelevant, and the bind
  # itself is where the language guard belongs.
  def runner_factories(state, namespace, runner_id) do
    state.namespace_factories
    |> Map.get(namespace, %{})
    |> Enum.filter(fn {_factory_id, info} ->
      Map.has_key?(factory_entry(info).adverts, runner_id)
    end)
    |> MapSet.new(fn {factory_id, _info} -> factory_id end)
  end

  # The stored entry and the ACTIVE runners advertising `factory_id` at any
  # language or kind — what separates "the factory exists in another language"
  # from "no runner has this factory at all". `placeable_runners/3` is this set
  # narrowed to the established language and kind, so the two never disagree on
  # who is even a candidate.
  def advertising_runners(state, namespace, factory_id) do
    active_ids = MapSet.new(Roster.get_runner_ids(state, namespace, :active))

    info =
      state.namespace_factories
      |> Map.get(namespace, %{})
      |> Map.get(factory_id)
      |> factory_entry()

    {info, MapSet.intersection(MapSet.new(Map.keys(info.adverts)), active_ids)}
  end

  # A factory's established `attribute`: the record if there is one, else what
  # its own live advertisements say. Both halves of the language's record are
  # losable (the ETS table is recreated empty if its owner restarts, the sidecar
  # goes with the factory's state dir), and a missing record is no evidence of a
  # conflict — so it must not read as one. Derives, never records: this is a
  # query.
  #
  # Unanimity required: disagreeing advertisements with nothing recorded mean
  # the controller cannot tell which value owns the factory.
  def established(namespace, factory_id, attribute, info, candidates) do
    case ClassRegistry.established(namespace, factory_id, attribute) do
      nil ->
        case advertised_values(info, candidates, attribute) do
          [] -> :none
          [value] -> {:ok, value}
          _conflicting -> :ambiguous
        end

      recorded ->
        {:ok, recorded}
    end
  end

  # The distinct specified values the live candidates advertise for
  # `attribute`. An unspecified advertisement contributes nothing: it can neither
  # establish a factory nor make one ambiguous.
  defp advertised_values(info, candidates, attribute) do
    unspecified = Map.fetch!(@unspecified, attribute)

    candidates
    |> Enum.map(&get_in(info.adverts, [&1, attribute]))
    |> Enum.reject(&(&1 in [nil, unspecified]))
    |> Enum.uniq()
  end
end
