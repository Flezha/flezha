defmodule FlezhaTest.System.KnobDrillTest do
  @moduledoc """
  Every knob an operator can OBSERVE, set and then read on the surface its row
  names.

  The runtime drill (`test/flezha/config/runtime_drill_test.exs`) proves the
  variable reaches the key. This one proves the key reaches the world: a state
  root is the absolute directory `flz controller status` names and the WAL
  lands in, a socket directory is where the controller listens, `/healthz`
  answers on the address and port it was given, a parked payload lands under
  the DLQ directory, an audit record under the audit directory, and the
  Prometheus exporter serves on the port it was told.

  The table is `Keys.rows/0` filtered to rows that declare a `surface`, so a
  knob given one is covered here with no test edit — and a knob that claims a
  surface nothing reads fails by name.

  Tagged `:system`; run with
  `mix test --include system test/system/knob_drill_test.exs`.
  """

  # `reference_runtime: true`: controller mechanics. The scenario needs a live
  # controller and its own listeners, not a particular SDK.
  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  @moduletag reference_runtime: true
  @moduletag :capture_log

  alias Flezha.Config.Keys
  alias Flezha.Dlq.Entry
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Healthz.Server, as: Healthz
  alias Flezha.Observability.AuditLog.Writer, as: AuditWriter
  alias Flezha.Observability.Export.PrometheusHttp
  alias Flezha.Service.Cli.Controller, as: ControllerCli
  alias Flezha.Socket.Support
  alias Flezha.State.Paths
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    previous = Application.get_all_env(:flezha)

    on_exit(fn ->
      for {key, value} <- previous, do: Application.put_env(:flezha, key, value)
    end)

    :ok
  end

  test "every row that declares a surface is observed on it" do
    surfaced = for {_name, row} <- Keys.rows(), row.surface, do: row

    # A surface the drill has no reading for is a claim nothing checks.
    unread = for row <- surfaced, not Map.has_key?(readings(), row.surface), do: row.name
    assert unread == []

    assert length(surfaced) >= 6

    for row <- surfaced do
      value = scratch(row)
      configure(row.key, value)

      assert Map.fetch!(readings(), row.surface).(value),
             "#{row.name} is not observable on its surface"
    end
  end

  # A row's key may be nested, so the drill writes it the way the boot config
  # does rather than assuming a flat one.
  defp configure(key, value) when is_atom(key), do: Application.put_env(:flezha, key, value)

  defp configure({group, leaf}, value) do
    Application.put_env(
      :flezha,
      group,
      Keyword.put(Application.get_env(:flezha, group, []), leaf, value)
    )
  end

  # One reading per surface: what an operator looks at, driven for real.
  defp readings() do
    %{
      {:status, :state_root} => &status_reports_state_root/1,
      {:disk, :socket_dir} => &socket_dir_is/1,
      {:disk, :dlq_dir} => &dlq_writes_under/1,
      {:disk, :audit_dir} => &audit_writes_under/1,
      {:healthz, :port} => &healthz_answers_on_port/1,
      {:healthz, :bind} => &healthz_binds/1,
      {:observe, :prometheus} => &prometheus_serves_on/1
    }
  end

  # Two halves of one claim: `flz controller status` reports the resolved root,
  # and that is the directory the WAL actually lands in. A status field naming
  # a tree nothing writes to would pass the first half alone.
  defp status_reports_state_root(root) do
    resolved = Path.expand(root)

    printed =
      ExUnit.CaptureIO.capture_io(fn ->
        :ok = ControllerCli.controller_status("text", %{})
      end)

    Paths.state_root() == resolved and
      printed =~ "state_root:" and printed =~ resolved and
      String.starts_with?(Paths.wal_path({"knob_drill", "proc", "k1"}), resolved)
  end

  defp socket_dir_is(dir), do: Support.socket_dir() == Path.expand(dir)

  defp dlq_writes_under(dir) do
    {:ok, entry} =
      Entry.new(%{
        operative_class: "knob_drill",
        procedure_id: "proc",
        entity_key: "k1",
        channel: "input",
        error: "the drill parked this",
        payload: "bytes"
      })

    {:ok, _park_id} = DlqStore.append(entry)

    NodeTestHelper.wait_until(fn -> Path.wildcard(Path.join(dir, "**/*")) != [] end, 2_000) != nil
  end

  # The writer creates and holds `<dir>/audit-current.jsonl`, so the directory
  # an operator points at is where a record lands.
  defp audit_writes_under(dir) do
    {:ok, pid} = AuditWriter.start_link(dir: dir)
    exists? = File.exists?(Path.join(dir, "audit-current.jsonl"))
    NodeTestHelper.ensure_stopped(pid)
    exists?
  end

  defp healthz_answers_on_port(port) do
    with {:ok, pid} <- start_healthz(),
         {:ok, bound} <- Healthz.listen_port() do
      NodeTestHelper.ensure_stopped(pid)
      bound == port or port == 0
    else
      _ -> false
    end
  end

  defp healthz_binds(address) do
    case start_healthz() do
      {:ok, pid} ->
        {:ok, port} = Healthz.listen_port()
        reachable? = connectable?(address, port)
        NodeTestHelper.ensure_stopped(pid)
        reachable?

      _ ->
        false
    end
  end

  defp prometheus_serves_on(port) do
    case PrometheusHttp.start_link([]) do
      {:ok, pid} ->
        reachable? = connectable?("127.0.0.1", port)
        NodeTestHelper.ensure_stopped(pid)
        reachable?

      _ ->
        false
    end
  end

  defp start_healthz() do
    case Healthz.start_link([]) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  end

  defp connectable?(address, port) do
    host = if address == "0.0.0.0", do: ~c"127.0.0.1", else: String.to_charlist(address)

    case :gen_tcp.connect(host, port, [:binary, active: false], 1_000) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        true

      _ ->
        false
    end
  end

  # A value no default could be, so a surface that answers it answers the row.
  defp scratch(%{type: :int}), do: free_port()
  defp scratch(%{type: :string, surface: {:healthz, :bind}}), do: "127.0.0.1"

  defp scratch(%{name: name}),
    do: NodeTestHelper.claim_tmp_dir("knob_drill_#{name}")

  defp free_port() do
    {:ok, socket} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end
end
