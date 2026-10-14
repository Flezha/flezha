defmodule FlezhaTest.System.EmbeddedRunnerE2ETest do
  @moduledoc """
  A runner in the controller's own BEAM. The Elixir SDK started in this VM
  connects to this controller over UDS like any other runner: the socket
  directory is handed to the core from the application env rather than read
  from the process environment, and under enforcement it presents a runner
  credential. It then serves a keyed relay and answers the relay's activity
  read with every tasklet's totals. Closing it ends its session, and the core
  it ran on serves no second runner in this VM.

  The runner is this VM whatever `FLEZHA_E2E_RUNTIME` names, so the scenario
  proves the Elixir SDK's embedding and no other runtime's.

  Tagged `:system`; run with
  `mix test --include system test/system/embedded_runner_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  alias Flezha.AuthZ
  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.Relay.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.{EventManager, RelayManager}
  alias Flezha.Socket.Support
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.{AuthTestHelper, NodeTestHelper}

  @ns "embedded-runner"

  defmodule Tally do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "embedded_tally"

    @impl true
    def init_state(_key), do: 0

    defchange counted(n) do
      n + 1
    end

    @impl true
    def run(input, n, ctx) do
      n = counted(n, ctx)
      emit(ctx, "#{input}=#{n}")
      {:ok, n}
    end
  end

  defmodule Embedded do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    import Flezha.SDK.Channels

    def factory_id, do: "embedded_tally"

    def channels(c),
      do: c |> input("in", codec: :text) |> key_by(& &1) |> into(Tally) |> output("out")
  end

  setup do
    _ = AuthTestHelper.setup_auth_stores(prefix: "flz_embedded_runner", claim: true)
    assert AuthZ.enforcement_posture() == :enforced
    {:ok, _} = NamespaceStore.create(@ns, quotas: %{})
    {secret, _hash} = AuthTestHelper.issue_runner_credential(@ns)

    previous = Application.get_env(:flezha_sdk, :socket_files_path)
    Application.put_env(:flezha_sdk, :socket_files_path, Support.socket_dir())

    on_exit(fn ->
      if previous,
        do: Application.put_env(:flezha_sdk, :socket_files_path, previous),
        else: Application.delete_env(:flezha_sdk, :socket_files_path)

      try do
        RelayManager.stop_all_relays(@ns)
        FleetManager.stop_namespace(@ns)
      catch
        _kind, _reason -> :ok
      end
    end)

    %{secret: secret}
  end

  test "a refused start leaves nothing behind, the next one connects, serves and answers, and a close ends it",
       %{
         secret: secret
       } do
    # Without a credential the controller refuses it; the start fails at the
    # call and no runner of this process keeps knocking.
    assert {:error, %Flezha.SDK.Error{code: "runner.namespace_not_proven"}} =
             @ns
             |> Flezha.SDK.create_runner()
             |> Flezha.SDK.register_factory(Embedded)
             |> Flezha.SDK.start()

    EventManager.add_handler(:runner_connected_event)
    on_exit(fn -> EventManager.rem_handler(:runner_connected_event) end)

    runner =
      @ns
      |> Flezha.SDK.create_runner(namespace_secret: secret)
      |> Flezha.SDK.register_factory(Embedded)
      |> Flezha.SDK.start()

    assert %Flezha.SDK{} = runner
    on_exit(fn -> Flezha.SDK.close(runner) end)

    assert_receive {:runner_connected_event, {@ns, runner_id}}, 30_000
    assert FleetManager.active_runners(@ns) == [runner_id]

    relay = start_relay()
    for key <- ~w(a b a), do: RelayManager.send_io(relay, @ns, key, %{"input_type" => "2"})

    assert Enum.sort(for _ <- 1..3, do: output()) == ~w(a=1 a=2 b=1)

    {:ok, raw, ^runner_id} = RelayManager.get_tasklet_activity(@ns, relay, %{})
    %{rows: [row]} = RunnerCommands.TaskletActivityResponse.decode!(raw)
    assert %{tasklet: "embedded_tally", cells: 2, admitted_total: 3} = row

    :ok = Flezha.SDK.close(runner)

    assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(@ns) == [] end, 15_000),
           "the closed runner's session is still open"

    assert {:error, %Flezha.SDK.Error{code: "runtime.core_shut_down"}} =
             @ns
             |> Flezha.SDK.create_runner(namespace_secret: secret)
             |> Flezha.SDK.register_factory(Embedded)
             |> Flezha.SDK.start()
  end

  defp start_relay do
    id = "embedded_#{System.unique_integer([:positive])}"

    sink = %Flezha.Plugin{
      id: "embedded_sink",
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: false},
      hooks: [{:on_output, []}]
    }

    {:ok, _} =
      RelayManager.start_relay(@ns, %Builder{
        id: id,
        factory_id: "embedded_tally",
        opts: [bindings: RelayFixture.bindings(@ns, id, [sink])]
      })

    id
  end

  defp output do
    receive do
      {:message_forward, payload} -> to_string(payload)
    after
      30_000 -> flunk("the embedded runner released no output")
    end
  end
end
