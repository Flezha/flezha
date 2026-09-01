defmodule Flezha.Convergence.PluginActivationRecoveryDrillTest do
  @moduledoc """
  The end-to-end case the unit tests cannot cover: an operative with a declared
  source whose upstream is DOWN at bind time recovers on its own once upstream
  returns, with no operator action.

  Everything here is real except the hub lookup: a real `%Overwatch{}` served by
  a real GenServer through `Overwatch.handle_call/3`, a real `Extension.Table`,
  a real provider whose `activate/1` opens a real TCP connection, a real
  `Diff` → `Planner` → `Heal.execute/1` round. "Upstream down" is a port with
  nothing listening; "upstream up" is a listener on that same port.

  What makes the drill writable at all is that the phase is the authority on
  "running": a failed activation leaves the plugin `:prepared`, and `:prepared`
  reads healthy, so a Probe counting every subscribed plugin as running would
  make the Diff empty on this tick and every later one.
  """

  # async: false: Mimic global mode; Mimic stubs; the primary Logger.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Convergence.{Backoff, Diff, Heal, Intent, Planner}
  alias Flezha.Operative.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Service.HubManager

  # Two activations fail against a refused socket by design; capture so a green
  # run stays quiet.
  @moduletag :capture_log

  setup :set_mimic_global_until_exit

  @namespace "plugin_activation_recovery_drill"
  @operative_id "detector"
  @plugin_id "src"

  # A source whose `activate/1` acquires a real upstream resource: a TCP
  # connection. Refused while nothing listens, granted once something does —
  # the same shape as the websocket provider's connect-at-activate, without
  # needing a websocket server to prove it.
  defmodule SocketSource do
    def init_provider(opts), do: {:ok, opts}

    def activate(%{port: port}) do
      case :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 200) do
        {:ok, socket} -> {:ok, spawn(fn -> hold(socket) end)}
        {:error, reason} -> {:error, reason}
      end
    end

    defp hold(socket) do
      receive do
        :stop -> :gen_tcp.close(socket)
      end
    end

    def on_input(ctx, _opts), do: {:ok, ctx}
  end

  # The operative, served by the production callback.
  defmodule HostedOperative do
    use GenServer

    def start_link(ow), do: GenServer.start_link(__MODULE__, ow)

    @impl true
    def init(ow), do: {:ok, ow}

    @impl true
    def handle_call(request, from, ow), do: Overwatch.handle_call(request, from, ow)
  end

  defp free_port() do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    :gen_tcp.close(listen)
    port
  end

  defp plugin(port),
    do: %Plugin{
      id: @plugin_id,
      provider: SocketSource,
      opts: %{port: port},
      subscriptions: [on_input: []],
      namespace: @namespace,
      operative_id: @operative_id
    }

  defp intent(port),
    do: %Intent{
      operatives: [
        %{
          namespace: @namespace,
          operative_id: @operative_id,
          factory_id: nil,
          plugins: [{@plugin_id, plugin(port)}]
        }
      ]
    }

  # The shape `Convergence.Probe` reports, built from the operative's own live
  # table through the same two accessors the Probe calls.
  defp reality(pid) do
    ow = GenServer.call(pid, :get_overwatch_state)

    %{
      active_runners: %{@namespace => MapSet.new(["e_live"])},
      operatives: %{
        {@namespace, @operative_id} => %{
          bound_runner: "e_live",
          pid: pid,
          parked?: false,
          suspended?: false,
          running_plugins: Table.running_ids(ow.extensions),
          subscribed_plugins: Table.subscribed_ids(ow.extensions),
          plugin_health: Table.health(ow.extensions)
        }
      },
      stalled_operatives: MapSet.new()
    }
  end

  test "a source whose upstream is down at bind heals itself when upstream returns" do
    port = free_port()

    {:ok, pid} =
      HostedOperative.start_link(%Overwatch{
        id: @operative_id,
        namespace: @namespace,
        runner_id: "e_live",
        factory_id: nil,
        channel: nil,
        extensions: Table.new(Overwatch.__extensions_catalog__()),
        builder: nil
      })

    stub(HubManager, :get_pid, fn @namespace, @operative_id -> {:ok, pid} end)

    # ---- bind with upstream DOWN -------------------------------------------
    assert {:error, {:activation_failed, :econnrefused}} =
             GenServer.call(pid, {:plugin_subscribe, @plugin_id, plugin(port)})

    ow = GenServer.call(pid, :get_overwatch_state)
    assert Table.phase(ow.extensions, @plugin_id) == :prepared
    assert Table.running_ids(ow.extensions) == []

    # ---- the loop SEES it (this is the whole defect) ------------------------
    delta = Diff.compute(intent(port), reality(pid))

    assert [{{@namespace, @operative_id}, @plugin_id, _}] = delta.missing_plugins
    refute Diff.empty?(delta)

    [op] = Planner.plan(delta, Backoff.new())
    assert op.primitive == :start_plugin

    # ---- the heal reports what it achieved, which is nothing yet ------------
    assert Heal.execute(op) == :error,
           "a heal that did not bring the plugin up must not report success"

    assert Diff.compute(intent(port), reality(pid)).missing_plugins != [],
           "a failed heal must not erase the divergence it failed to fix"

    # ---- upstream returns. No operator action. -----------------------------
    {:ok, listen} =
      :gen_tcp.listen(port, [:binary, active: false, reuseaddr: true, backlog: 4])

    on_exit(fn -> :gen_tcp.close(listen) end)

    assert Heal.execute(op) == :ok

    ow = GenServer.call(pid, :get_overwatch_state)
    assert Table.phase(ow.extensions, @plugin_id) == :running
    assert Table.running_ids(ow.extensions) == [@plugin_id]

    # ---- and the loop goes quiet again --------------------------------------
    healed = Diff.compute(intent(port), reality(pid))
    assert healed.missing_plugins == []
    assert healed.unhealthy_plugins == []
    assert Planner.plan(healed, Backoff.new()) == []
  end
end
