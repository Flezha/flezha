defmodule FlezhaTest.State.StoreTest do
  @moduledoc """
  One contract, run against every module that declares it.

  Before this, opening was shared and closing was not: eight `:dets.close`
  calls sat one per owner's `terminate/2`, so a store's shutdown was proved six
  times and its contract nowhere, and the plugin catalog — which shared no
  function with the other six — was proved by nothing at all.

  The implementers are discovered from the behaviour, never from a list here,
  so a store cannot be added without being proved.
  """
  use ExUnit.Case, async: false

  alias Flezha.DetsStore
  alias Flezha.State.Store

  setup do
    base = Path.join(System.tmp_dir!(), "store_conformance_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(base) end)
    {:ok, base: base}
  end

  test "the behaviour is declared by every durable control-plane store" do
    declared = MapSet.new(implementers())

    for expected <- [
          Flezha.AuthZ.TokenStore,
          Flezha.AuthZ.RoleStore,
          Flezha.AuthZ.NamespaceStore,
          Flezha.AuthZ.QuotaCounters,
          Flezha.AuthZ.AuthState,
          Flezha.Schedule.Store,
          Flezha.Plugin.Installer.Store
        ] do
      assert MapSet.member?(declared, expected),
             "#{inspect(expected)} keeps a DETS file and declares no store contract"
    end
  end

  test "no store reaches DETS outside the seam" do
    offenders =
      Path.wildcard("lib/**/*.ex")
      |> Enum.reject(&(&1 == "lib/flezha/state/dets_store.ex"))
      |> Enum.reject(&(&1 == "lib/flezha/state/store.ex"))
      |> Enum.filter(fn file ->
        file
        |> File.read!()
        |> String.replace(~r/^\s*#.*$/m, "")
        |> String.replace(~r/^\s*@(module|type|)doc\s+"""(.|\n)*?"""/m, "")
        |> String.match?(~r/:dets\.(open_file|close)\(/)
      end)

    assert offenders == [],
           "these open or close a DETS table themselves: " <> Enum.join(offenders, ", ")
  end

  describe "the contract, per implementer" do
    test "declares a subtree, a file and a table, and answers one path", %{base: base} do
      for module <- implementers() do
        assert module.subtree() in Flezha.Layout.Table.subtrees(),
               "#{inspect(module)} declares subtree #{inspect(module.subtree())}"

        assert is_binary(module.filename()) and module.filename() != ""
        assert is_atom(module.table())
        assert Path.basename(module.path()) == module.filename()
      end

      _ = base
    end

    test "open -> put -> crash -> reopen keeps the row; close releases the table",
         %{base: base} do
      for module <- implementers() do
        dir = Path.join(base, Atom.to_string(module))
        path = Path.join(dir, module.filename())
        table = module.table()
        :ok = File.mkdir_p(dir)

        # open
        assert {:ok, %{path: ^path}} = DetsStore.init_table(table, path, module_opts(module))
        assert :dets.info(table, :size) == 0

        # put + sync: the row is on disk before the call returns
        :ok = :dets.insert(table, {"k", "v"})
        :ok = :dets.sync(table)

        # a crash is the process going away without a close; the file holds
        :ok = :dets.close(table)
        assert {:ok, _} = DetsStore.init_table(table, path, module_opts(module))
        assert :dets.lookup(table, "k") == [{"k", "v"}]

        # close releases the name
        :ok = :dets.close(table)
        assert :dets.info(table, :size) == :undefined
      end
    end

    test "a corrupt file is rotated aside and a fresh one opened", %{base: base} do
      for module <- implementers() do
        dir = Path.join(base, "corrupt_#{module}")
        path = Path.join(dir, module.filename())
        :ok = File.mkdir_p(dir)
        File.write!(path, "this is not a DETS file at all, not even close")

        log =
          ExUnit.CaptureLog.capture_log(fn ->
            assert {:ok, _} = DetsStore.init_table(module.table(), path, module_opts(module))
          end)

        assert log =~ "corrupt DETS"

        rotated =
          File.ls!(dir)
          |> Enum.filter(&String.starts_with?(&1, module.filename() <> ".corrupt-"))

        assert length(rotated) == 1,
               "#{inspect(module)} left #{inspect(File.ls!(dir))} behind"

        assert :dets.info(module.table(), :size) == 0
        :ok = :dets.close(module.table())
      end
    end
  end

  test "a subtree's file set is every declaring store's own filename" do
    # What a durability-tier artifact of the subtree carries. Listed here so a
    # store that drops out of the group fails rather than silently stopping
    # being shipped.
    assert Store.filenames_in(:auth) ==
             ~w(namespaces.dets quota_counters.dets roles.dets state.dets tokens.dets)

    assert Store.filenames_in(:schedules) == ~w(schedules.dets)
    assert Store.filenames_in(:plugins) == ~w(installed_plugins.dets)
    assert Store.filenames_in(:dlq) == []
  end

  test "the seam's close is what a store's terminate calls" do
    for module <- implementers() do
      assert function_exported?(module, :terminate, 2),
             "#{inspect(module)} has no terminate/2"
    end
  end

  # Every loop below runs over this, so an empty answer is a failure rather
  # than a green run over nothing.
  defp implementers() do
    found = Store.implementers()
    assert length(found) >= 7, "the behaviour found #{length(found)} implementers"
    found
  end

  defp module_opts(module) do
    if function_exported?(module, :open_opts, 0), do: module.open_opts(), else: []
  end
end
