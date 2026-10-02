defmodule Flezha.Api.ConsumerSurfaceTest do
  @moduledoc """
  `flezha_ui` is this platform's console and reaches the controller by runtime
  name over `:erpc`. Its `mix.exs` does not depend on this project, so no
  compiler sees both sides and a rename here compiles clean in both trees,
  failing only on a user's machine — which is how six names came to be
  called that this tree does not answer.

  `docs/api/consumers/flezha_ui.txt` records what the console calls, and this
  test resolves every line of it against this tree's real exports. The break is
  caused here, so it is reported here, in the run that causes it.

  Two things keep the gate honest. A vendored manifest is a second source of
  truth, so when `FLEZHA_UI_PATH` names a consumer checkout the manifest is
  regenerated from it and any difference in the call set fails. And the entries
  that do not resolve *today* are listed below rather than tolerated: a new
  break fails the run, and a repaired one fails it too, until the line is
  removed from the list.
  """
  use ExUnit.Case, async: true

  alias FlezhaTest.Helper.RpcSurface

  @manifest "docs/api/consumers/flezha_ui.txt"
  @published "docs/api/consumers/published.txt"
  @generator "scripts/consumer_surface.py"

  # The only two published names callable before a caller holds a context,
  # because their answers are what one is built from.
  @context_free [{Flezha.AuthZ.TokenStore, :verify, 1}, {Flezha.AuthZ, :enforcement_posture, 0}]

  # What the console calls today and this tree does not answer. Phase 3 of
  # `publish-a-controller-consumer-api` moves the console onto the published
  # surface, in the sibling repository, and empties this list. The console's own
  # tasklet/cell rename takes off the entries that still use the old names.
  #
  # module-naming-record:begin
  @unresolved_today %{
    {Flezha.AuthZ.RoleStore, :list_for_namespace, 1} => :withdrawn,
    {Flezha.Dlq.Store, :procedures, 1} => :withdrawn,
    {Flezha.Operative.Kind, :capabilities, 1} => :withdrawn,
    {Flezha.Operative.Kind, :known?, 1} => :moved,
    {Flezha.Operative.Lifecycle, :rewind, 2} => :moved,
    {Flezha.Operative.PartitionGroup, :rescale, 4} => :moved,
    {Flezha.Service.CliManager, :cluster_status_recovery, 2} => :withdrawn,
    {Flezha.Service.ConfigManager, :get_provider_info, 0} => :withdrawn,
    {Flezha.Service.HubManager, :operative_plugins, 1} => :withdrawn,
    {Flezha.Service.HubManager, :operative_rows, 1} => :withdrawn,
    {Flezha.Service.Identificator, :hub, 1} => :moved,
    {Flezha.Service.OperativeManager, :get_entity_state, 3} => :withdrawn,
    {Flezha.Service.OperativeManager, :get_operative_pid, 2} => :withdrawn,
    {Flezha.Service.OperativeManager, :get_overwatch_state, 2} => :moved,
    {Flezha.Service.PluginHelper, :enable_io_listener, 4} => :withdrawn,
    {Flezha.Service.PluginManager, :get_plugins, 0} => :withdrawn,
    {Flezha.State.PersistenceResolver, :workflow_completed_retention_hours, 1} => :withdrawn,
    {Flezha.State.ProcInbox, :last_offset, 1} => :moved,
    {Flezha.State.ProcInbox, :undelivered, 1} => :moved,
    {Flezha.State.ProcJournal, :committed_iterations, 1} => :moved,
    {Flezha.State.ProcJournal, :consumed_offset, 1} => :moved,
    {Flezha.State.ProcJournal, :read, 1} => :moved,
    {Flezha.State.Registry, :operative_classes, 0} => :withdrawn,
    {Flezha, :disable_plugin, 1} => :withdrawn,
    {Flezha, :list_operatives, 1} => :withdrawn,
    {Flezha, :start_operative, 2} => :withdrawn,
    {Flezha, :stop_operative, 2} => :withdrawn
  }

  # module-naming-record:end

  setup_all do
    entries = manifest_entries(@manifest, 80)

    published = manifest_entries(@published, 60)

    # Resolved once: an unresolved entry sends the resolver through the whole
    # tree's source, and four tests below ask the same question.
    %{entries: entries, published: published, unresolved: RpcSurface.unresolved(entries)}
  end

  test "every controller function the console calls resolves against this tree", ctx do
    report = ratchet(ctx.unresolved, @unresolved_today)

    assert report.new == [],
           "the console calls names this tree does not answer:\n  " <>
             Enum.map_join(report.new, "\n  ", &RpcSurface.explain/1) <>
             "\n\nEither restore the name, or move the console and regenerate #{@manifest}."

    assert report.repaired == [],
           "these entries resolve again and must come off @unresolved_today: " <>
             Enum.map_join(report.repaired, ", ", &inspect/1)

    assert report.misclassified == [],
           "the class of a recorded break changed: " <>
             Enum.map_join(report.misclassified, ", ", fn {mfa, was, now} ->
               "#{inspect(mfa)} was #{was}, is now #{now}"
             end)
  end

  test "the ratchet refuses a new break, a repaired one, and a changed class" do
    known = %{
      {Flezha.Gone, :a, 0} => :withdrawn,
      {Flezha.Moved, :b, 1} => :moved,
      {Flezha.Fixed, :c, 2} => :withdrawn
    }

    unresolved = [
      {{Flezha.Gone, :a, 0, "ui:1"}, :withdrawn},
      {{Flezha.Moved, :b, 1, "ui:2"}, {:privatised, Flezha.Somewhere}},
      {{Flezha.Fresh, :d, 3, "ui:4"}, :withdrawn}
    ]

    report = ratchet(unresolved, known)

    assert [{{Flezha.Fresh, :d, 3, "ui:4"}, :withdrawn}] = report.new
    assert [{Flezha.Fixed, :c, 2}] = report.repaired
    assert [{{Flezha.Moved, :b, 1}, :moved, :privatised}] = report.misclassified
  end

  test "the report names the class and the successor, not just the name", ctx do
    [{_entry, _verdict} = pair | _] =
      Enum.filter(ctx.unresolved, fn {{m, f, a, _}, _} ->
        # module-naming-record:begin
        {m, f, a} == {Flezha.Service.OperativeManager, :get_overwatch_state, 2}
        # module-naming-record:end
      end)

    line = RpcSurface.explain(pair)
    assert line =~ "MOVED"
    assert line =~ "Flezha.Service.OperatorManager"
    assert line =~ "node_manager.ex:"
  end

  test "a manifest that describes a consumer which has moved on is refused", ctx do
    checkout = System.get_env("FLEZHA_UI_PATH")

    if checkout in [nil, ""] do
      # CI has no consumer checkout; the always-on half above is the gate there.
      assert File.exists?(@generator)
    else
      live = regenerate(checkout)
      recorded = call_set(ctx.entries)

      drift =
        Enum.map_join(
          Enum.map(MapSet.difference(live, recorded), &{"added", &1}) ++
            Enum.map(MapSet.difference(recorded, live), &{"removed", &1}),
          "\n  ",
          fn {side, {m, f, a}} -> "#{side}: #{inspect(m)}.#{f}/#{a}" end
        )

      assert drift == "",
             "#{@manifest} does not describe #{checkout}:\n  " <>
               drift <>
               "\n\nRegenerate it:\n  python3 #{@generator} #{checkout} --out #{@manifest}"
    end
  end

  # Three ways a recorded break drifts, each with its own remedy: a name the
  # console calls that nothing here answers and nothing recorded, a recorded one
  # that resolves again, and one whose class changed under it.
  describe "the published surface" do
    test "every name on it resolves against this tree", ctx do
      failures = RpcSurface.unresolved(ctx.published)

      assert failures == [],
             "a published name was retired without the manifest that spells it:\n  " <>
               Enum.map_join(failures, "\n  ", &RpcSurface.explain/1)
    end

    test "every name on it is on the front door or a service module", ctx do
      strays =
        for {module, fun, arity, _where} <- ctx.published,
            module != Flezha,
            not String.starts_with?(inspect(module), "Flezha.Service."),
            do: "#{inspect(module)}.#{fun}/#{arity}"

      assert strays == [],
             "the published surface may name only `Flezha` or `Flezha.Service.*`: " <>
               Enum.join(strays, ", ")
    end

    test "the two context-free identity reads exist and are the only ones named" do
      entries = for {module, fun, arity} <- @context_free, do: {module, fun, arity, @published}
      assert RpcSurface.unresolved(entries) == []

      body = File.read!(@published)

      for {module, fun, arity} <- @context_free do
        assert body =~ "#{inspect(module)}.#{fun}/#{arity}",
               "#{inspect(module)}.#{fun}/#{arity} must be recorded as context-free"
      end
    end

    test "the consumer's own manifest is not on it yet, and the gap is phase 3", ctx do
      published = call_set(ctx.published)
      consumer = call_set(ctx.entries)
      on_surface = MapSet.intersection(consumer, published)

      # Phase 3 moves the console; when it lands, this is every line it calls.
      assert MapSet.size(on_surface) < MapSet.size(consumer),
             "the console is fully on the published surface — remove this test and " <>
               "assert the manifests are equal instead"

      assert MapSet.size(on_surface) > 0,
             "not one call the console makes is on the published surface; the two " <>
               "manifests describe unrelated things"
    end
  end

  defp ratchet(unresolved, known) do
    reported =
      Map.new(unresolved, fn {{module, fun, arity, _where}, verdict} ->
        {{module, fun, arity}, class(verdict)}
      end)

    %{
      new: Enum.reject(unresolved, &Map.has_key?(known, mfa(&1))),
      repaired: Map.keys(known) -- Map.keys(reported),
      misclassified: misclassified(known, reported)
    }
  end

  defp misclassified(known, reported) do
    for {entry, was} <- Enum.sort(known),
        reported[entry] not in [nil, was],
        do: {entry, was, reported[entry]}
  end

  defp mfa({{module, fun, arity, _where}, _verdict}), do: {module, fun, arity}

  # The generator writes the same format the committed manifest is in, so the
  # comparison reads both through one parser.
  defp regenerate(checkout) do
    {output, 0} = System.cmd("python3", [@generator, checkout], stderr_to_stdout: true)
    path = Path.join(System.tmp_dir!(), "flz_consumer_#{System.unique_integer([:positive])}.txt")
    File.write!(path, output)
    on_exit(fn -> File.rm(path) end)
    path |> RpcSurface.manifest() |> call_set()
  end

  defp call_set(entries), do: MapSet.new(entries, fn {m, f, a, _where} -> {m, f, a} end)

  defp manifest_entries(path, floor) do
    assert File.exists?(path),
           "#{path} is missing; generate it with `python3 #{@generator} <checkout>`"

    entries = RpcSurface.manifest(path)

    # A gate that finds nothing passes for the wrong reason.
    assert length(entries) >= floor,
           "#{path} records only #{length(entries)} entries; it is truncated"

    entries
  end

  defp class({:moved, _candidates}), do: :moved
  defp class({:privatised, _holder}), do: :privatised
  defp class(:withdrawn), do: :withdrawn
end
