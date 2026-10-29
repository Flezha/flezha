defmodule FlezhaTest.Helper.FleetStateHelper do
  @moduledoc """
  Synthetic fleet state for tests that drive the live `Flezha.Runner.FleetManager`
  with `:sys.replace_state` — one definition of "this namespace holds these
  runners advertising these factories".
  """

  alias Flezha.Relay.ClassRegistry
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Factories

  # Every synthetic runner speaks this language and advertises this run unless
  # a test says otherwise. A factory whose established language or run no
  # runner matches is unplaceable by design, so a helper that seeded neither
  # would make every fleet test a test of the refusal path.
  @default_lang 1
  @default_kind :flow

  @doc """
  Tell the live `FleetManager` that `runner` closed its control channel, as
  `ControlChannel` does, and return once it has been handled.
  """
  def disconnected(namespace, runner) do
    GenServer.cast(FleetManager, {:runner_disconnected, namespace, runner})
    _ = :sys.get_state(FleetManager)
    :ok
  end

  @doc """
  Seed `namespace` with `%{runner_id => [factory_id]}`, all `:active`.

  Factory ids may be given as `id` (the default language and run),
  `{id, sdk_lang}` to place runners of different languages in one namespace, or
  `{id, sdk_lang, run}` to name the run too. The first runner to advertise a
  factory establishes it, exactly as a real connect does.
  """
  def seed(namespace, runner_factories) do
    ts = System.monotonic_time(:millisecond)

    :sys.replace_state(FleetManager, fn state ->
      runners = runner_factories |> Map.keys() |> Map.new(&{&1, {:active, ts}})

      ns_factories =
        Enum.reduce(runner_factories, %{}, fn {runner, factories}, acc ->
          Enum.reduce(factories, acc, &record_factory(&2, &1, runner, namespace))
        end)

      state
      |> put_in([:runner_namespaces, namespace], runners)
      |> put_in([:namespace_factories, namespace], ns_factories)
    end)
  end

  @doc """
  Deliver a `sync_factories` cast of the factory entries `runner` announces, as
  a connect frame carries them, then a same-sender call so an assertion
  following this runs after the cast is handled.
  """
  def announce(namespace, runner, factories) do
    GenServer.cast(FleetManager, {:sync_factories, namespace, runner, factories})
    {:ok, _} = FleetManager.list_factories(namespace)
    :ok
  end

  @doc """
  One graph channel as the connect frame carries it, `fields` (string keys)
  over a channel with no call target that drops nothing.
  """
  def graph_channel(name, kind, producer, consumers, plugin_end, fields \\ %{}) do
    Map.merge(
      %{
        "name" => name,
        "kind" => kind,
        "producer" => producer,
        "consumers" => consumers,
        "plugin_end" => plugin_end,
        "call_target" => "",
        "overflow" => "drop_nothing"
      },
      fields
    )
  end

  @doc """
  The `UpdateFactories` form of a factory entry given in the connect frame's
  form, so a test announces one factory both ways.
  """
  def factory_decl(factory) do
    decl = factory |> Map.delete("graph") |> struct_of(RunnerCommands.FactoryDecl)
    %{decl | graph: relay_graph(factory["graph"])}
  end

  defp relay_graph(nil), do: nil

  defp relay_graph(graph),
    do: %RunnerCommands.RelayGraph{
      channels: Enum.map(graph["channels"], &struct_of(&1, RunnerCommands.GraphChannel)),
      tasklets: Enum.map(graph["tasklets"], &struct_of(&1, RunnerCommands.GraphTasklet))
    }

  defp struct_of(fields, module),
    do:
      struct(
        module,
        Map.new(fields, fn {key, value} -> {String.to_existing_atom(key), value} end)
      )

  @doc """
  `announce/3` of bare factory ids, which take the same shapes `seed/2` accepts.
  """
  def sync(namespace, runner, factory_ids) do
    factories =
      Enum.map(factory_ids, fn factory ->
        {id, lang, kind} = split(factory)
        %{"id" => id, "description" => nil, "sdk_lang" => lang, "run" => to_string(kind)}
      end)

    announce(namespace, runner, factories)
  end

  @doc """
  The per-runner relay count `FleetManager` last handed placements out of.

  Read from its state rather than through an API: nothing in the controller asks
  the fleet for this — the user-facing count is the observability source's,
  read straight from the hub registry.
  """
  def loads(namespace) do
    case :sys.get_state(FleetManager).runner_loads[namespace] do
      %{counts: counts} -> counts
      nil -> %{}
    end
  end

  @doc "Forget what every factory established — `on_exit` for a seeding test."
  def forget_established(namespace, factory_ids),
    do: Enum.each(factory_ids, &ClassRegistry.delete_established(namespace, &1))

  # Fold one advertised factory into the namespace's factory map, establishing
  # its language and run the first time a runner names each.
  defp record_factory(acc, factory, runner, namespace) do
    {id, lang, kind} = split(factory)

    # An unspecified value establishes nothing — the same rule
    # `Factories.store_factories/4` applies to a real advertisement.
    if lang > 0 and is_nil(ClassRegistry.established(namespace, id, :sdk_lang)) do
      ClassRegistry.put_established(namespace, id, :sdk_lang, lang)
    end

    if kind != nil and is_nil(ClassRegistry.established(namespace, id, :run)) do
      ClassRegistry.put_established(namespace, id, :run, kind)
    end

    existing = Factories.factory_entry(Map.get(acc, id))
    advert = %{sdk_lang: lang, run: kind}
    Map.put(acc, id, %{existing | adverts: Map.put(existing.adverts, runner, advert)})
  end

  defp split({id, lang, kind}), do: {id, lang, kind}
  defp split({id, lang}), do: {id, lang, @default_kind}
  defp split(id), do: {id, @default_lang, @default_kind}
end
