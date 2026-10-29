defmodule Flezha.Runner.FleetManager.Factories do
  @moduledoc """
  The per-namespace factory table, and the SDK language and run each factory
  is established at.

  A factory is advertised by one or more runners; its language, its run
  (`flow` or `steps`) and a steps run's shape (`each`, `on_failure`) are each
  established by the first advertisement that names one and never change
  afterwards. Invariant: a runner advertising a
  factory at a language or run other than the established one is not a
  placement candidate for it — durable State/Change bodies are per-SDK and are
  never decoded across languages, and every instance of a factory runs the one
  way its class declares.
  """

  alias Flezha.Relay.ClassRegistry
  alias Flezha.Runner.FleetManager.Roster
  alias Flezha.Service.HubManager

  require Logger

  # The value of each established attribute that says nothing: an
  # advertisement carrying it never places, since the runner most likely to be
  # untagged is the one most likely to be mismatched.
  @unspecified %{sdk_lang: 0, run: nil, each: nil, on_failure: nil}

  def store_factories(state, namespace, runner_id, factories) do
    ns_factories = Map.get(state.namespace_factories, namespace, %{})

    ns_factories =
      Enum.reduce(factories, ns_factories, fn factory, acc ->
        factory_id = factory["id"]

        existing = factory_entry(Map.get(acc, factory_id))

        advert = %{
          sdk_lang: advertised_lang(factory),
          run: advertised_run(namespace, runner_id, factory),
          each: advertised_step(factory, "each", [:once, :per_input, :chained]),
          on_failure: advertised_step(factory, "on_failure", [:undo_all, :keep_progress])
        }

        # Stored even when it conflicts, so user tooling can see the
        # runner that is advertising the wrong language or run;
        # `placeable_runners/3` is what refuses it.
        Enum.each(advert, fn {attribute, value} ->
          establish(namespace, runner_id, factory_id, attribute, value)
        end)

        # The API, the graph and the declarations ride beside the placement
        # attributes, never among them: they describe the factory and decide
        # no placement.
        advert =
          Map.merge(advert, %{
            api: api(factory["api"]),
            graph: graph(factory["graph"]),
            declarations: declarations(factory["declarations"], advert),
            announced_at: System.os_time()
          })

        Map.put(acc, factory_id, %{
          description: blank_nil(factory["description"]) || existing.description,
          adverts: Map.put(existing.adverts, runner_id, advert)
        })
      end)

    put_in(state, [:namespace_factories, namespace], ns_factories)
  end

  @doc """
  Decoded `UpdateFactories` entries in the form the connect frame carries
  them, the same fields under the same string keys, so both frames reach
  `store_factories/4` alike.
  """
  @spec announced(term()) :: term()
  def announced(%_{} = message),
    do:
      Map.new(Map.from_struct(message), fn {key, value} -> {to_string(key), announced(value)} end)

  def announced(values) when is_list(values), do: Enum.map(values, &announced/1)
  def announced(value), do: value

  def remove_ce_factories(state, namespace, runner_id) do
    ns_factories =
      Map.new(Map.get(state.namespace_factories, namespace, %{}), fn {factory_id, info} ->
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
  # advertising runner, what that runner announced (its placement attributes,
  # API and graph).
  # Total on an older entry: a `KeyError` here would take the whole
  # FleetManager down over a shape question, and every placement lookup with it.
  def factory_entry(nil), do: %{description: nil, adverts: %{}}

  def factory_entry(%{} = info) do
    info
    |> Map.put_new(:description, nil)
    |> Map.put_new(:adverts, %{})
  end

  @doc """
  What a factory row reads from its announcement: the API and the
  declarations `runner`'s advertisement of a factory entry announced, the
  latest advertisement's when `runner` has none here, and whether its
  advertising runners announce different ones. `declarations` is `nil` for a
  runner that announced none.
  """
  @spec announced_row(map(), String.t() | nil) :: %{
          api: [map()],
          api_at: integer() | nil,
          api_disagree: boolean(),
          declarations: map() | nil,
          declarations_disagree: boolean()
        }
  def announced_row(info, runner) do
    {api, at, disagree} = announced(info, runner, :api, [])
    {declarations, _at, declarations_disagree} = announced(info, runner, :declarations, nil)

    %{
      api: api,
      api_at: at,
      api_disagree: disagree,
      declarations: declarations,
      declarations_disagree: declarations_disagree
    }
  end

  @doc """
  The graph a factory entry's advertisement announced (`nil` when it announced
  none), chosen as `announced_row/2` chooses it, and whether its advertising
  runners announce different ones. Read on its own, so no factory row carries it.
  """
  @spec announced_graph(map(), String.t() | nil) :: %{
          graph: map() | nil,
          disagree: boolean()
        }
  def announced_graph(info, runner) do
    {graph, _at, disagree} = announced(info, runner, :graph, nil)
    %{graph: graph, disagree: disagree}
  end

  # `key` of `runner`'s advertisement, or of the newest, when it was announced,
  # and whether the advertisements disagree on it.
  defp announced(info, runner, key, none) do
    adverts = factory_entry(info).adverts
    all = Map.values(adverts)
    newest = fn -> Enum.max_by(all, &Map.get(&1, :announced_at, 0), fn -> %{} end) end
    advert = Map.get_lazy(adverts, runner, newest)
    disagree = all |> Enum.map(&Map.get(&1, key, none)) |> Enum.uniq() |> length() > 1

    {Map.get(advert, key, none), Map.get(advert, :announced_at), disagree}
  end

  # The API entries a factory's announcement carries, whether from the connect
  # frame's JSON or an `UpdateFactories` frame. Absent is none: a runner
  # older than the relay API announces nothing.
  defp api(entries) when is_list(entries), do: Enum.map(entries, &api_entry/1)
  defp api(_absent), do: []

  defp api_entry(entry) do
    %{
      channel: entry["channel"],
      codec: entry["codec"],
      schema: schema(entry["schema_json"]),
      description: blank_nil(entry["description"]),
      answer_codec: blank_nil(entry["answer_codec"]),
      answer_schema: schema(entry["answer_schema_json"])
    }
  end

  # The graph a factory's announcement carries, in either form. Absent is
  # `nil`: a runner older than the graph announces none.
  defp graph(%{"channels" => channels, "tasklets" => tasklets})
       when is_list(channels) and is_list(tasklets) do
    %{
      channels: for(%{"name" => name} = c when is_binary(name) <- channels, do: graph_channel(c)),
      tasklets: for(%{"name" => name} = t when is_binary(name) <- tasklets, do: graph_tasklet(t))
    }
  end

  defp graph(_absent), do: nil

  # A runner's frame is read as text, lists of text, integers and booleans,
  # so a malformed one stores nothing a reader of the graph would trip over.
  # A field a runner did not announce is `nil`.
  defp graph_channel(channel) do
    %{
      name: channel["name"],
      kind: text(channel["kind"]),
      producer: blank_nil(text(channel["producer"])),
      consumers: texts(channel["consumers"]),
      plugin_end: text(channel["plugin_end"]),
      call_target: blank_nil(text(channel["call_target"])),
      overflow: text(channel["overflow"]),
      retry_max: integer(channel["retry_max"]),
      retry_backoff_ms: integer(channel["retry_backoff_ms"]),
      call_deadline_ms: integer(channel["call_deadline_ms"]),
      call_create: boolean(channel["call_create"])
    }
  end

  defp graph_tasklet(tasklet) do
    %{
      name: tasklet["name"],
      mode: text(tasklet["mode"]),
      cell_keys: texts(tasklet["cell_keys"]),
      mailbox_capacity: integer(tasklet["mailbox_capacity"]),
      compensates: boolean(tasklet["compensates"]),
      idle_ttl_ms: integer(tasklet["idle_ttl_ms"]),
      max_cells: integer(tasklet["max_cells"])
    }
  end

  # The declarations a factory's announcement carries, decoded as an
  # `AddRelay` answer's metadata is, in the names an operator reads. Empty or
  # absent is `nil`: a current runner always declares its durability. A flow's
  # duplicate rule, acknowledgement, correction window and keying are `nil` for
  # a steps run, which declares none of them.
  defp declarations(metadata, advert) when is_map(metadata) and map_size(metadata) > 0 do
    decoded = ClassRegistry.declarations_of(metadata, advert)
    flow? = decoded.run == :flow

    %{
      durable: decoded.durable,
      sleep_after_ms: decoded.sleep_after_ms,
      max_duration_ms: decoded.max_duration_ms,
      effectively_once: if(flow?, do: decoded.effectively_once),
      ack: if(flow?, do: ack(decoded.ack_checkpoint)),
      correction_ms: decoded.correction_window_ms,
      keyed: if(flow?, do: not decoded.keyless)
    }
  end

  defp declarations(_absent, _advert), do: nil

  defp ack(true = _checkpoint), do: :checkpoint
  defp ack(false), do: :per_input

  defp text(value) when is_binary(value), do: value
  defp text(_other), do: nil

  defp integer(value) when is_integer(value), do: value
  defp integer(_other), do: nil

  defp boolean(value) when is_boolean(value), do: value
  defp boolean(_other), do: nil

  defp texts(values) when is_list(values), do: Enum.filter(values, &is_binary/1)
  defp texts(_other), do: []

  defp schema(json) when is_binary(json) and json != "" do
    case Jason.decode(json) do
      {:ok, schema} when is_map(schema) -> schema
      _unreadable -> nil
    end
  end

  defp schema(_none), do: nil

  defp blank_nil(value) when value in [nil, ""], do: nil
  defp blank_nil(value), do: value

  # The language a factory entry advertises. Absent / `0` stays `0`: an
  # unspecified language is refused at the placement filter, never guessed.
  defp advertised_lang(factory) do
    case factory["sdk_lang"] do
      lang when is_integer(lang) and lang > 0 -> lang
      _ -> @unspecified.sdk_lang
    end
  end

  # The run a factory entry advertises. Absent, empty or anything but `flow`
  # and `steps` stays unspecified and is refused at the placement filter, as a
  # missing language is; a value that says something else is logged by name,
  # so a runner announcing anything else is visible.
  defp advertised_run(namespace, runner_id, factory) do
    case factory["run"] do
      "flow" ->
        :flow

      "steps" ->
        :steps

      unknown when unknown in [nil, ""] ->
        @unspecified.run

      unknown ->
        Logger.warning(
          "[fleet_manager] runner #{runner_id} on #{namespace} announces factory " <>
            "#{factory["id"]} with run #{inspect(unknown)}, which is neither flow nor " <>
            "steps; it will not be a placement candidate for it"
        )

        @unspecified.run
    end
  end

  # A steps run's `key` (its loop shape or failure mode) among the `known`
  # values; nothing for a flow, or for a value the controller does not know.
  defp advertised_step(%{"run" => "steps"} = factory, key, known),
    do: Enum.find(known, &(Atom.to_string(&1) == factory[key]))

  defp advertised_step(_factory, _key, _known), do: nil

  # An advertisement establishes the factory's `attribute` if nothing has yet,
  # and is reported when it disagrees. The one place either is RECORDED, so
  # writing it belongs on this path and never in a query.
  #
  # The durable half of the language's resolution is read at the BIND
  # (`Flezha.Relay.Lifecycle`): a disk walk here would stall every
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
          attribute: attribute,
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

  # How a disagreeing advertisement of each attribute is reported: durable
  # bodies are per-SDK, and every instance of a factory runs the one way its
  # class declares. Each event name is a literal so the export seam reads it.
  defp report_conflict(:sdk_lang, meta),
    do: :telemetry.execute([:flezha, :runner, :factory_language_conflict], %{count: 1}, meta)

  defp report_conflict(_attribute, meta),
    do: :telemetry.execute([:flezha, :runner, :factory_run_conflict], %{count: 1}, meta)

  # Factory ids this runner advertised before a sync that are absent from the
  # incoming full set.
  def dropped_factory_ids(state, namespace, runner_id, factories) do
    new_ids = MapSet.new(factories, & &1["id"])

    for {factory_id, info} <- Map.get(state.namespace_factories, namespace, %{}),
        Map.has_key?(factory_entry(info).adverts, runner_id),
        not MapSet.member?(new_ids, factory_id),
        do: factory_id
  end

  # Tripwire for the suspended-instance removal hole: a dropped factory with
  # controller-known relays still bound to this runner — or suspended
  # (runner tag nil) with no other active runner advertising the factory —
  # escaped the SDK-side live-instance check, and its rehydration will fail
  # placement. Warn only; a blocking policy is deferred.
  def warn_dropped_factories(state, namespace, runner_id, dropped) do
    Enum.each(dropped, fn factory_id ->
      case affected_relays(state, namespace, runner_id, factory_id) do
        [] ->
          :ok

        affected ->
          Logger.warning(
            "[fleet_manager] runner #{runner_id} on #{namespace} dropped factory " <>
              "#{factory_id} that still has controller-known relays " <>
              "#{inspect(affected)}; their (re)placement will fail until a " <>
              "runner re-defines the factory"
          )
      end
    end)
  end

  def affected_relays(state, namespace, runner_id, factory_id) do
    still_placeable = MapSet.size(placeable_runners(state, namespace, factory_id)) > 0

    namespace
    |> HubManager.relay_rows()
    |> Enum.filter(fn row ->
      row.factory == factory_id and
        (row.runner == runner_id or (is_nil(row.runner) and not still_placeable))
    end)
    |> Enum.map(& &1.id)
  end

  @doc """
  The factories of `namespace` this node can actually place on, which is what
  makes its controller an eligible host for their relays.

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
  # language and run** — the one filter every placement path reads (initial
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

    Enum.reduce_while([:sdk_lang, :run, :each, :on_failure], {:ok, candidates}, fn attribute,
                                                                                   {:ok, runners} ->
      verdict = established(namespace, factory_id, attribute, info, candidates)
      narrowed = narrow(runners, info, attribute, verdict)

      if Enum.empty?(narrowed) and not Enum.empty?(runners),
        do: {:halt, {:refused, attribute, verdict}},
        else: {:cont, {:ok, narrowed}}
    end)
  end

  defp narrow(runners, info, attribute, {:ok, established}),
    do: MapSet.filter(runners, &(get_in(info.adverts, [&1, attribute]) == established))

  # A flow, or a steps run that named no shape, decides nothing about it.
  defp narrow(runners, _info, attribute, :none) when attribute in [:each, :on_failure],
    do: runners

  defp narrow(_runners, _info, _attribute, _undecided), do: MapSet.new()

  # The factories `runner_id` advertises in `namespace`, at any language or
  # kind. The inverse read of `advertising_runners/3` over the same table: the
  # release path asks what one runner can take, where a placement asks who can
  # take one relay. Unfiltered on purpose — a mismatched advertisement must
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
  # language or run — what separates "the factory exists in another language"
  # from "no runner has this factory at all". `placeable_runners/3` is this set
  # narrowed to the established language and run, so the two never disagree on
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
