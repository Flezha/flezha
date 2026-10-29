defmodule Flezha.Logs.LogsServiceTest do
  @moduledoc """
  `Flezha.Service.Logs` on one controller: the kinds it describes, a read's
  filters, pages and refusals, a read of the relay history that writes no audit
  record beside one of the audit kind that does, and the relay history read
  with the observability bus off.
  """

  # async: false: restarts the relay history kind; registers a sink.
  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false
  use Mimic

  alias Flezha.Logs.RelayHistory
  alias Flezha.Observability.Registry
  alias Flezha.Observability.TestSinks.Counting
  alias Flezha.Relay.Builder
  alias Flezha.Service.Logs

  @moduletag :capture_log

  @ctx %{source: :internal}

  setup %{tmp: dir} do
    env = Application.get_env(:flezha, :relay_history, [])
    Application.put_env(:flezha, :relay_history, Keyword.put(env, :dir, dir))
    restart_history()

    on_exit(fn ->
      Application.put_env(:flezha, :relay_history, env)
      restart_history()
    end)
  end

  test "kinds describes every kind with its filters and columns" do
    assert {:ok, kinds} = Logs.kinds(@ctx)
    assert Enum.map(kinds, & &1.id) == ["audit", "relay_history"]

    history = Enum.find(kinds, &(&1.id == "relay_history"))
    assert history.enabled and history.disabled_reason == nil

    assert %{type: "enum", values: outcomes} =
             Enum.find(history.filters, &(&1.field == "outcome"))

    assert outcomes == ~w(completed undone failed stopped timed_out rescaled_away cancelled)
    assert Enum.any?(history.columns, &(&1.field == "duration_ms"))

    audit = Enum.find(kinds, &(&1.id == "audit"))
    refute audit.enabled
    assert audit.disabled_reason =~ "audit log is disabled"
  end

  test "a read pages newest first with no record repeated or skipped" do
    for n <- 1..25, do: :ok = started("relay-#{n}")

    {:ok, %{records: first, next: next, unreachable: []}} = read(%{limit: 10})
    {:ok, %{records: second, next: next2}} = read(%{limit: 10, before: next})
    {:ok, %{records: third, next: nil}} = read(%{limit: 10, before: next2})

    ids = Enum.map(first ++ second ++ third, & &1["relay"])
    assert ids == for(n <- 25..1//-1, do: "relay-#{n}")
    assert Enum.all?(first, &(&1["node"] == Atom.to_string(node())))
  end

  test "a controller that does not answer is named while the others' records are read" do
    ghost = :"history_ghost@127.0.0.1"
    stub(Flezha.Cluster.Directory, :controller_nodes, fn -> [node(), ghost] end)
    :ok = started("here")

    assert {:ok, %{records: [%{"relay" => "here"}], unreachable: [^ghost]}} = read(%{})

    assert {:ok, %{controllers: [_here], unreachable: [^ghost]}} =
             Logs.stats(%{kind: "relay_history"}, @ctx)
  end

  test "filters narrow a read, and a filter the kind does not declare is refused" do
    :ok = started("keep", "fan_trade")
    :ok = started("other", "other_factory")

    assert {:ok, %{records: [%{"relay" => "keep"}]}} = read(%{filters: %{factory: "fan_trade"}})

    assert {:error, %Flezha.Error{code: "logs.unknown_filter"} = error} =
             read(%{filters: %{source: "x"}})

    assert error.message =~ "namespace, factory, relay, event, outcome"

    assert {:error, %Flezha.Error{code: "logs.unknown_kind"}} =
             Logs.read(%{kind: "nope"}, @ctx)
  end

  test "reading the relay history writes no audit record; reading the audit log does" do
    counter = :counters.new(1, [])
    name = :"logs_audit_probe_#{System.unique_integer([:positive])}"
    config = %{counter_ref: counter, parent: self(), sources: [:audit_log]}
    {:ok, _} = Registry.register_sink(%{name: name, module: Counting, config: config})
    on_exit(fn -> Registry.unregister_sink(name) end)

    assert {:ok, _} = read(%{})
    refute_receive {:counted, %{source: :audit_log}}, 200

    assert {:error, %Flezha.Error{code: "audit.disabled"}} =
             Logs.read(%{kind: "audit"}, %{source: :cli, request_id: "r-1"})

    assert_receive {:counted, %{source: :audit_log, name: :read, metadata: metadata}}, 1_000
    assert metadata.request_id == "r-1"
  end

  test "the relay history is written and read with the observability bus off" do
    obs = Application.get_env(:flezha, :observability, [])
    Application.put_env(:flezha, :observability, Keyword.put(obs, :enabled, false))

    :ok = started("quiet")
    assert {:ok, %{records: [%{"relay" => "quiet"}]}} = read(%{})

    assert {:error, %Flezha.Error{code: "observability.disabled"}} =
             Logs.stats(%{kind: "audit"}, @ctx)
  end

  test "stats, files and rotate answer one row per controller" do
    :ok = started("one")

    assert {:ok, %{rotated: [%{node: node, rotated_to: "relay_history-" <> _}]}} =
             Logs.rotate(%{kind: "relay_history"}, @ctx)

    assert node == node()

    assert {:ok, %{files: [%{node: ^node, record_count: 1}], unreachable: []}} =
             Logs.files(%{kind: "relay_history"}, @ctx)

    assert {:ok, %{controllers: [row], unreachable: []}} =
             Logs.stats(%{kind: "relay_history"}, @ctx)

    assert %{node: ^node, total_files: 2, dropped_count: 0, timeout_count: 0} = row
  end

  defp read(params), do: Logs.read(Map.put(params, :kind, "relay_history"), @ctx)

  defp started(id, factory \\ "f"),
    do: RelayHistory.started("ns", %Builder{id: id, factory_id: factory, run: :steps})

  defp restart_history() do
    _ = Supervisor.terminate_child(Flezha.Logs.Supervisor, :relay_history)
    {:ok, _} = Supervisor.restart_child(Flezha.Logs.Supervisor, :relay_history)
  end
end
