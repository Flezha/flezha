defmodule FlezhaTest.System.CrossSdkPlacementE2ETest do
  @moduledoc """
  The cross-SDK placement guard, proved against two live runtimes
  (`guard-cross-sdk-placement`).

  This is the one scenario that deliberately places more than one SDK in a
  single run. Everywhere else the one-runtime-per-run rule stands
  (`FLEZHA_E2E_RUNTIME` selects it); here two runners of different languages
  advertise the SAME factory id on purpose, because the conformance fleet gives
  them the same ids so one workload can be measured on each runtime — 49 ids
  are claimed by both `rust_runner` and `java_runner`. Those shared ids are
  safe only because of the guard this scenario asserts.

  Durable `State` and `Change` bodies are per-SDK below the shared 16-byte
  envelope (serde-postcard in Rust, Kryo in Java), and a restore ships them
  verbatim to whichever runner bound the Cell. So: a relay of a factory
  established at one language is placed only on a runner of that language, and
  killing that runner leaves it unbound with a language reason rather than
  rerouting onto the other runtime.

  Tagged `:interop` as well as `:system`; it needs BOTH test apps built.
  Run with `mix test --include system --include interop`.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  @moduletag interop: true

  # Both runners below name their language, so this module never follows
  # `FLEZHA_E2E_RUNTIME` — it proves a controller-side placement filter, and its
  # two runtimes ARE the scenario. The module tag says so rather than leaving a
  # future runner spawn to pick up whichever SDK the run happened to select.
  @moduletag reference_runtime: true

  @moduletag skip: FlezhaTest.Tiers.skip_unless_included(:interop)

  alias Flezha.Relay.Builder
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PortPilot
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "cross_sdk_placement"

  # A `runner_lifecycle` fixture both reference apps register under the same id.
  @shared_factory "bench_echo"

  @rust PortPilot.sdk_lang(:rust)

  setup_all do
    # The factory must start unestablished: a leftover record from another run
    # would decide the outcome before either runner connects.
    ClassRegistry.delete_established(@namespace, @shared_factory)

    # The Rust runner connects FIRST and establishes the factory's language.
    {rust_port, rust_runner} = NodeTestHelper.start_runner_port(@namespace, runtime: :rust)
    {java_port, java_runner} = NodeTestHelper.start_runner_port(@namespace, runtime: :java)

    on_exit(fn ->
      # Order matters: the namespace must leave the fleet while its hub is
      # still alive. Closing the ports first tears the hub down and leaves the
      # FleetManager holding a namespace whose ETS table is gone — which the
      # next cleanup tick walks and dies on.
      for step <- [
            fn -> RelayManager.stop_all_relays(@namespace) end,
            fn -> FleetManager.stop_namespace(@namespace) end,
            fn -> Enum.each([rust_port, java_port], &Port.close/1) end,
            fn -> ClassRegistry.delete_established(@namespace, @shared_factory) end
          ] do
        try do
          step.()
        catch
          _, _ -> :ok
        end
      end
    end)

    %{
      rust_port: rust_port,
      rust_runner: rust_runner,
      java_runner: java_runner
    }
  end

  # ONE test, because the phases are one narrative and the last of them kills a
  # runner the earlier ones need: three tests over shared `setup_all` runners
  # would pass or fail on the seed's ordering.
  test "placement never crosses languages, and a dead runner does not hand its factory over",
       ctx do
    assert ClassRegistry.established(@namespace, @shared_factory, :sdk_lang) == @rust,
           "the Rust runner connected first, so it establishes #{@shared_factory}"

    # -- both advertise the id; only the established language is a candidate --
    candidates = FleetManager.runners_for_factory(@namespace, @shared_factory)
    assert candidates == [ctx.rust_runner]

    refute ctx.java_runner in candidates,
           "a Java runner advertising a Rust-established factory is not a candidate: " <>
             "its Kryo codec cannot read the factory's postcard state"

    # -- placement lands on the established language --
    placed = %Builder{
      id: "cross_sdk_placed_#{System.unique_integer([:positive])}",
      factory_id: @shared_factory,
      opts: []
    }

    assert {:ok, _} = RelayManager.start_relay(@namespace, placed)

    assert RelayManager.assign_runner(@namespace, @shared_factory, placed.id) ==
             ctx.rust_runner

    # -- the reroute off a dead runner reads the same filter --
    {:os_pid, os_pid} = :erlang.port_info(ctx.rust_port, :os_pid)
    System.cmd("kill", ["-KILL", Integer.to_string(os_pid)], stderr_to_stdout: true)

    assert NodeTestHelper.wait_until(
             fn ->
               FleetManager.runners_for_factory(@namespace, @shared_factory) == []
             end,
             10_000
           ),
           "the Java runner became a candidate after the Rust runner died"

    assert FleetManager.placement_refusal(@namespace, @shared_factory) == {:language, @rust}

    # -- and the refusal names the language, never "does not exist" --
    {:error, reason} =
      RelayManager.start_relay(@namespace, %Builder{
        id: "cross_sdk_refused_#{System.unique_integer([:positive])}",
        factory_id: @shared_factory,
        opts: []
      })

    assert {:factory_language_unserved, @namespace, @shared_factory, @rust} = reason
  end
end
