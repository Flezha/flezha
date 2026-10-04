defmodule Flezha.Api.RpcSurfaceTest do
  @moduledoc """
  The negative control for `FlezhaTest.Helper.RpcSurface`. A resolver that never
  refused anything would let both consumer gates pass vacuously, so the six
  names `flezha_ui` calls today and this tree does not answer are asserted
  here, each in its own class.
  """
  use ExUnit.Case, async: true

  alias FlezhaTest.Helper.RpcSurface

  describe "a name this tree still answers" do
    test "resolves" do
      entries = [
        {Flezha.State.Paths, :operator_classes, 0, "control"},
        {Flezha.Service.Cli, :call, 3, "control"}
      ]

      assert Enum.map(RpcSurface.resolve(entries), &elem(&1, 1)) == [:ok, :ok]
      assert RpcSurface.unresolved(entries) == []
    end
  end

  describe "a name this tree does not answer" do
    test "a module that moved is reported as moved, naming the successor first" do
      # The console still calls the retired path, which is the break this gate
      # reports; the name is data here, not a use.
      # module-naming-record:begin
      assert {:moved, [Flezha.State.Paths | _]} =
               verdict({Flezha.State.Registry, :operator_classes, 0, "ui:910"})

      # module-naming-record:end

      assert {:moved, [Flezha.Identificator | _]} =
               verdict({Flezha.Service.Identificator, :hub, 1, "ui:1560"})

      assert {:moved, [Flezha.Operator.Migration | _]} =
               verdict({Flezha.Operator.Lifecycle, :rewind, 2, "ui:990"})
    end

    test "a function made private is reported as privatised, not as present" do
      # `Flezha.AuthZ.authorize/3` is the policy point and `do_authorize/3` is
      # deliberately behind it, so this one is private for an architectural
      # reason rather than by accident.
      assert {:privatised, Flezha.AuthZ} =
               verdict({Flezha.AuthZ, :do_authorize, 3, "a caller that reached past the gate"})
    end

    test "a name with no successor is reported as withdrawn" do
      # Renamed, not moved: `completed_retention_hours/2` carries the answer now,
      # and no search by name and arity can reach a name that changed.
      assert :withdrawn =
               verdict(
                 {Flezha.State.PersistenceResolver, :operator_completed_retention_hours, 1,
                  "ui:926"}
               )

      assert :withdrawn = verdict({Flezha.AuthZ.RoleStore, :list_for_namespace, 1, "ui:1116"})

      # Publishing the per-node recovery read removed the `defp` the console was
      # reaching past, so this name has no successor of the same spelling at all.
      assert :withdrawn =
               verdict({Flezha.Service.CliManager, :cluster_status_recovery, 2, "ui:1685"})

      assert :withdrawn =
               verdict({Flezha.Service.ConfigManager, :get_provider_info, 0, "ui:1354"})

      assert :withdrawn =
               verdict({Flezha.Service.PluginHelper, :enable_io_listener, 4, "ui:1309"})
    end

    test "the explanation names the class and what to do about it" do
      entry = {Flezha.AuthZ, :do_authorize, 3, "ui:1685"}
      [pair] = RpcSurface.unresolved([entry])

      line = RpcSurface.explain(pair)
      assert line =~ "ui:1685"
      assert line =~ "PRIVATISED"
      assert line =~ "Flezha.AuthZ"
    end
  end

  defp verdict(entry) do
    [{^entry, verdict}] = RpcSurface.resolve([entry])
    verdict
  end
end
