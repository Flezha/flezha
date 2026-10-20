defmodule Flezha.Service.Cli.CatalogueTest do
  @moduledoc """
  The catalogue of published operations is built from each binding's own
  declaration, so these tests hold the declarations to the functions they
  describe: every positional argument declared, every function described, and
  every row saying whether it may be reached over the network.
  """
  use ExUnit.Case, async: false

  import FlezhaTest.Case, only: [refute_minted: 2]

  @doc false
  # Every `{module, function, declared, positional}` whose declared arguments
  # do not number the arguments it takes before its context.
  def arity_mismatches(modules) do
    for module <- modules,
        {function, bindings} <- Enum.group_by(module.__verbs__(), &elem(&1, 0)),
        {_function, arity, _verb, described} = Enum.max_by(bindings, &elem(&1, 1)),
        declared = length(described[:args] || []),
        declared != arity - 1,
        do: {module, function, declared, arity - 1}
  end

  @doc false
  def undescribed(modules) do
    for module <- modules,
        {function, _arity, _verb, described} <- module.__verbs__(),
        described[:doc] in [nil, ""],
        uniq: true,
        do: {module, function}
  end

  describe "every published binding" do
    test "declares exactly the arguments it takes before its context" do
      assert arity_mismatches(Flezha.Service.modules()) == []
    end

    test "is described by the first sentence of its @doc" do
      assert undescribed(Flezha.Service.modules()) == []
    end
  end

  describe "a surface" do
    test "binds no row: only a published module does, so a request is judged once" do
      {:ok, modules} = :application.get_key(:flezha, :modules)

      surfaces =
        for module <- modules,
            module not in Flezha.Service.modules(),
            Code.ensure_loaded?(module) and function_exported?(module, :__verbs__, 0),
            do: {module, module.__verbs__() |> Enum.map(&elem(&1, 2)) |> Enum.uniq()}

      assert surfaces == [], "modules off the published surface bind rows: #{inspect(surfaces)}"
    end
  end

  describe "a planted mismatch" do
    defmodule Planted do
      @moduledoc false
      use Flezha.AuthZ.Verb

      @doc "Three arguments, two declared."
      verb mismatched(a, b, c, caller_context),
        verb: :plugin_info,
        args: [a: [type: :string, doc: "A."], b: [type: :string, doc: "B."]] do
        {:ok, {a, b, c, caller_context}}
      end

      verb undocumented(caller_context), verb: :plugin_info do
        {:ok, caller_context}
      end
    end

    test "is named with the module, the function and both counts" do
      assert arity_mismatches([Planted]) == [{Planted, :mismatched, 2, 3}]
    end

    test "a function without a @doc is named" do
      assert undescribed([Planted]) == [{Planted, :undocumented}]
    end
  end

  describe "the catalogue" do
    test "says which operations may be reached over the network" do
      entries = Map.new(Flezha.Service.catalogue(), &{&1.name, &1})

      assert entries["Controller.stop"].remote == false
      assert entries["Dlq.list"].remote == true
    end

    test "carries each operation's name, arguments, description, action and flags" do
      for entry <- Flezha.Service.catalogue() do
        assert entry.name =~ ~r/^[A-Z][A-Za-z]+\.[a-z_?]+$/
        assert is_list(entry.args)
        assert is_binary(entry.doc), "#{entry.name} has no description"
        assert is_atom(entry.action)
        assert is_boolean(entry.stream) and is_boolean(entry.remote)
      end
    end

    test "resolves a name without minting an atom" do
      names = ["no_such_operation_anywhere", "Elixir.Flezha.Service.NoSuchModuleAnywhere"]

      refute_minted(names, fn ->
        assert :error = Flezha.Service.lookup("Dlq.no_such_operation_anywhere")
        assert :error = Flezha.Service.lookup("NoSuchModuleAnywhere.list")
      end)

      assert {:ok, %{module: Flezha.Service.Dlq, function: :list}} =
               Flezha.Service.lookup("Dlq.list")
    end
  end

  describe "cli/catalogue.json" do
    test "is the catalogue the code declares, named entry by entry when it is not" do
      committed = operations(File.read!(Mix.Tasks.Flezha.Catalogue.path()))
      fresh = operations(Mix.Tasks.Flezha.Catalogue.render())

      stale =
        for name <- Enum.uniq(Map.keys(committed) ++ Map.keys(fresh)),
            committed[name] != fresh[name],
            do: name

      assert stale == [],
             "cli/catalogue.json is stale for #{inspect(Enum.sort(stale))}: run `mix flezha.catalogue`"

      assert File.read!(Mix.Tasks.Flezha.Catalogue.path()) == Mix.Tasks.Flezha.Catalogue.render()
    end
  end

  defp operations(json),
    do: json |> Jason.decode!() |> Map.fetch!("operations") |> Map.new(&{&1["name"], &1})
end
