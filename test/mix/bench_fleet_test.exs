defmodule Mix.Tasks.Bench.FleetTest do
  @moduledoc """
  `--runtime` resolution. The accepted set is a property of the SDK test-app
  manifests, never of a clause list here: a runtime that defers the capability
  a bench measures has no fixture, and its manifest is where that is declared.
  """

  use ExUnit.Case, async: true

  alias Flezha.Service.PortPilot
  alias Mix.Tasks.Bench.Fleet

  describe "parse_runtime/3" do
    test "no argument is the default fleet" do
      assert Fleet.parse_runtime(nil) == :rust
      assert Fleet.parse_runtime(nil, :workflow) == :rust
    end

    test "every runtime with a manifest is accepted when no capability is needed" do
      for runtime <- PortPilot.runtimes() do
        assert Fleet.parse_runtime(Atom.to_string(runtime)) == runtime
      end
    end

    test "an unknown runtime is refused and the accepted set is named" do
      %Mix.Error{message: message} =
        assert_raise(Mix.Error, fn -> Fleet.parse_runtime("kotlin") end)

      assert message =~ ~s(unknown --runtime "kotlin")

      for runtime <- PortPilot.runtimes() do
        assert message =~ Atom.to_string(runtime)
      end
    end

    test ":workflow accepts exactly the runtimes whose manifest does not defer it" do
      {accepted, refused} =
        Enum.split_with(PortPilot.runtimes(), fn runtime ->
          "workflow" not in (PortPilot.manifest(runtime)["deferred"] || [])
        end)

      assert Enum.sort(accepted) == [:java, :node, :rust]

      for runtime <- accepted do
        assert Fleet.parse_runtime(Atom.to_string(runtime), :workflow) == runtime
      end

      for runtime <- refused do
        %Mix.Error{message: message} =
          assert_raise(Mix.Error, fn ->
            Fleet.parse_runtime(Atom.to_string(runtime), :workflow)
          end)

        assert message =~ "defers workflow"
        assert message =~ PortPilot.manifest_path(runtime)
      end
    end

    test "the caller names what asked for the capability" do
      %Mix.Error{message: message} =
        assert_raise(Mix.Error, fn ->
          Fleet.parse_runtime("elixir", :workflow, "--durability on")
        end)

      assert message =~ "carries no --durability on"
      assert message =~ "defers workflow"
    end
  end
end
