defmodule Flezha.Plugin.Provider.HttpPollNamespaceKeyTest do
  @moduledoc """
  A shareable plugin's copy is one per `(namespace, name)`, so the same name
  enabled in two namespaces on one controller runs two workers, each polling its
  own endpoint, and a disable in one namespace leaves the other's running.
  """

  use FlezhaTest.HttpPollCase, async: false

  alias Flezha.Plugin.Extension.Table.Workers
  alias Flezha.Plugin.Provider.HttpPoll
  alias FlezhaTest.Helper.ProviderPrepare

  setup do
    name = "ticker_#{System.unique_integer([:positive])}"
    origin_a = start_origin(body: "from-a")
    origin_b = start_origin(body: "from-b")

    {:ok, opts_a} = prepare(origin_a, %{source_namespace: "ns_a", source_plugin_id: name})
    {:ok, opts_b} = prepare(origin_b, %{source_namespace: "ns_b", source_plugin_id: name})

    # Activation as the extension table runs it (`Table.Workers.start/5`).
    {:ok, pid_a} = ProviderPrepare.activate(HttpPoll, opts_a)
    stop_on_exit(pid_a)
    assert {"from-a", _} = take_ingest()

    {:ok, pid_b} = ProviderPrepare.activate(HttpPoll, opts_b)
    if pid_b != pid_a, do: stop_on_exit(pid_b)

    %{opts_a: opts_a, pid_a: pid_a, pid_b: pid_b, origin_b: origin_b}
  end

  test "one name in two namespaces runs two workers", ctx do
    assert ctx.pid_b != ctx.pid_a, "namespace ns_b was handed ns_a's worker"
    assert {"from-b", _} = take_ingest()
    assert Agent.get(ctx.origin_b.agent, & &1.requests) == 1
  end

  test "a disable in one namespace leaves the other's worker running", ctx do
    table = %{entries: %{}, monitors: %{}}

    Workers.stop_all(table, ctx.opts_a.source_plugin_id, HttpPoll, ctx.opts_a, & &1.())

    refute Process.alive?(ctx.pid_a)
    assert Process.alive?(ctx.pid_b), "disabling in ns_a stopped ns_b's worker"
  end
end
