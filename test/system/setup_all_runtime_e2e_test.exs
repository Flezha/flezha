defmodule FlezhaTest.System.SetupAllRuntimeE2ETest do
  @moduledoc """
  The harness invariant every `setup_all` fleet rests on: a runner spawned in
  `setup_all` is a runner of the SDK the run selected.

  `setup_all` runs before any `setup`, and in its own process, so a runtime
  published per test cannot reach a module-wide fleet: the fleet takes the
  reference SDK while the run reports the selected one. Charter §9 mandates one
  fleet per module, so this guards exactly the shape the charter asks for.

  It reads the SDK language the fleet advertised, because nothing else
  disagrees: the run is green, the conformance banner names the selected SDK,
  and the scenarios pass for the reference's reasons. The launch line is no
  witness either — a runtime launched through a build tool (`exec mix run`)
  replaces its own argv, so only the wire says which SDK connected.

  Tagged `:system` (boots a real subprocess); run with
  `mix test --include system test/system/setup_all_runtime_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  import FlezhaTest.Helper.NodeTestHelper, only: [ensure_port_dead: 1]

  alias Flezha.Operative.ClassRegistry
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.PortPilot
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "setup_all_runtime"

  # A `runner_lifecycle` fixture every test app registers, so the language the
  # controller records against it is the language that connected.
  @factory "do_nothing"

  # One fleet for the module, spawned exactly as a module-wide fleet is:
  # `start_runner_port/2` with no runtime named, so the default decides.
  setup_all do
    # A record left by an earlier run would answer for this run's fleet.
    ClassRegistry.delete_sdk_lang(@namespace, @factory)

    {port, _runner_id} = NodeTestHelper.start_runner_port(@namespace)

    on_exit(fn ->
      try do
        FleetManager.stop_namespace(@namespace)
      catch
        _, _ -> :ok
      end

      ClassRegistry.delete_sdk_lang(@namespace, @factory)
      ensure_port_dead(port)
    end)

    %{advertised: ClassRegistry.sdk_lang(@namespace, @factory)}
  end

  test "a fleet spawned in setup_all runs the selected SDK's runner", %{advertised: advertised} do
    selected = NodeTestHelper.runtime()
    expected = PortPilot.sdk_lang(selected)

    assert advertised == expected,
           "setup_all's fleet advertised sdk_lang #{inspect(advertised)}, but the run selected " <>
             "#{selected} (sdk_lang #{expected})"
  end
end
