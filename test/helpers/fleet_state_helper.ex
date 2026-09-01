defmodule FlezhaTest.Helper.FleetStateHelper do
  @moduledoc """
  Synthetic fleet state for tests that drive the live `Flezha.Runner.FleetManager`
  with `:sys.replace_state` — one definition of "this namespace holds these
  runners advertising these factories".
  """

  alias Flezha.Operative.ClassRegistry
  alias Flezha.Runner.FleetManager

  # Every synthetic runner speaks this language unless a test says otherwise.
  # A factory whose established language no runner matches is unplaceable by
  # design, so a helper that seeded no language would make every fleet test a
  # test of the refusal path.
  @default_lang 1

  @doc """
  Seed `namespace` with `%{runner_id => [factory_id]}`, all `:active`.

  Factory ids may be given as `id` (the default language) or `{id, sdk_lang}`
  to place runners of different languages in one namespace. The first runner to
  advertise a factory establishes it, exactly as a real connect does.
  """
  def seed(namespace, runner_factories) do
    ts = System.system_time(:millisecond)

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
  Deliver a `sync_factories` cast for `runner`, then a same-sender call so an
  assertion following this runs after the cast is handled.

  Factory ids take the same `id` / `{id, sdk_lang}` shape `seed/2` accepts.
  """
  def sync(namespace, runner, factory_ids) do
    factories =
      Enum.map(factory_ids, fn factory ->
        {id, lang} = split(factory)
        %{"id" => id, "description" => nil, "sdk_lang" => lang}
      end)

    GenServer.cast(FleetManager, {:sync_factories, namespace, runner, factories})
    {:ok, _} = FleetManager.list_factories(namespace)
    :ok
  end

  @doc "Forget every factory's established language — `on_exit` for a seeding test."
  def forget_langs(namespace, factory_ids),
    do: Enum.each(factory_ids, &ClassRegistry.delete_sdk_lang(namespace, &1))

  # Fold one advertised factory into the namespace's factory map, establishing
  # its language the first time a runner names one.
  defp record_factory(acc, factory, runner, namespace) do
    {id, lang} = split(factory)

    # An unspecified language establishes nothing — the same rule
    # `establish_lang/4` applies to a real advertisement.
    if lang > 0 and is_nil(ClassRegistry.sdk_lang(namespace, id)) do
      ClassRegistry.put_sdk_lang(namespace, id, lang)
    end

    existing = Map.get(acc, id, %{description: nil, ce_ids: MapSet.new(), langs: %{}})

    Map.put(acc, id, %{
      existing
      | ce_ids: MapSet.put(existing.ce_ids, runner),
        langs: Map.put(existing.langs, runner, lang)
    })
  end

  defp split({id, lang}), do: {id, lang}
  defp split(id), do: {id, @default_lang}
end
