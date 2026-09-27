defmodule FlezhaTest.Helper.DeclarationSnapshot do
  @moduledoc """
  The declaration a runtime sends, compared to its committed golden.

  Two callers read this: `test/system/topology_refusal_e2e_test.exs`, which pins
  every declaration golden against every runtime with a manifest, and the smoke
  tier, which pins only the identity stamps a keyed sink's output carries
  (`keyed_output_stamps`).

  The mechanism: bind the factory with `ClassRegistry.decode_declarations/1`
  intercepted and compare the raw metadata map the controller decoded. The stub
  delegates, so the bind itself is unchanged and the assertion is about what
  crossed the wire, not about what a test rebuilt.
  """

  import ExUnit.Assertions

  alias Flezha.Operator.Builder
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PortPilot
  alias FlezhaTest.Helper.MimicGlobal
  alias FlezhaTest.Helper.NodeTestHelper

  @golden_dir "test/fixtures/declarations"

  @doc "The directory the goldens live in."
  def golden_dir, do: @golden_dir

  @doc "Every committed golden's factory id."
  def factories do
    @golden_dir
    |> Path.join("*.json")
    |> Path.wildcard()
    |> Enum.map(&Path.basename(&1, ".json"))
    |> Enum.sort()
  end

  @doc """
  The committed goldens grouped by the capability the reference manifest files
  them under. A drill generating one describe per capability reads this, so a
  golden added under a new capability brings its scenario with it — and a
  runtime that defers that capability skips the row by the same rule as
  everything else.
  """
  def by_capability do
    fixtures =
      PortPilot.manifest(NodeTestHelper.reference_runtime())["fixtures"]

    goldens = MapSet.new(factories())

    for {capability, ids} <- fixtures,
        carried = Enum.filter(ids, &MapSet.member?(goldens, &1)),
        carried != [],
        do: {String.to_atom(capability), carried}
  end

  @doc """
  Bind `factory` in `namespace` and assert its declaration equals its golden.
  `unique` mints the operator id, so each caller keeps its own naming rule.
  """
  def assert_matches_golden(namespace, factory, kind, unique) do
    parent = self()

    Mimic.stub(ClassRegistry, :decode_declarations, fn raw ->
      send(parent, {:declaration_metadata, decoded_metadata(raw)})
      Mimic.call_original(ClassRegistry, :decode_declarations, [raw])
    end)

    MimicGlobal.set_mimic_global_until_exit()

    assert {:ok, _} =
             OperatorManager.start_operator(namespace, %Builder{
               id: unique.("d_#{factory}"),
               factory_id: factory,
               kind: kind
             })

    assert_receive {:declaration_metadata, metadata}, 60_000
    assert metadata == golden(factory, metadata)
  end

  @doc """
  The committed golden for `name`, or `actual` written back when
  `FLEZHA_E2E_SNAPSHOT=update` is set under the reference runtime.
  """
  def golden(name, actual) do
    path = Path.join(@golden_dir, "#{name}.json")

    cond do
      System.get_env("FLEZHA_E2E_SNAPSHOT") != "update" ->
        assert File.exists?(path),
               "no golden at #{path}; regenerate with " <>
                 "`FLEZHA_E2E_SNAPSHOT=update " <>
                 "FLEZHA_E2E_RUNTIME=#{NodeTestHelper.reference_runtime()} " <>
                 "mix test --include sdk_smoke`"

        path |> File.read!() |> Jason.decode!()

      NodeTestHelper.runtime() != NodeTestHelper.reference_runtime() ->
        flunk(
          "a golden is regenerated only under the reference runtime " <>
            "(#{NodeTestHelper.reference_runtime()}); this run is " <>
            "#{NodeTestHelper.runtime()}"
        )

      true ->
        File.mkdir_p!(@golden_dir)
        File.write!(path, Jason.encode!(actual, pretty: true) <> "\n")
        actual
    end
  end

  # The same wire form `ClassRegistry.decode_declarations/1` reads, kept raw so
  # a key an SDK adds, drops or spells differently is a diff.
  defp decoded_metadata(raw) when is_binary(raw) and raw != "" do
    case RunnerCommands.AddOperatorResponse.decode(raw) do
      {:ok, %{metadata: metadata}} -> metadata
      {:error, _} -> %{}
    end
  end

  defp decoded_metadata(_raw), do: %{}
end
