defmodule Flezha.Plugin.Provider.HttpPollNamespaceKeyTest do
  @moduledoc """
  A plugin's worker is one per `(namespace, name)`: the same name enabled in two
  namespaces, and two names in one namespace that set the same `plugin_id`, each
  run two workers polling their own endpoints, and a disable of one leaves the
  other's running.
  """

  use FlezhaTest.HttpPollCase, async: false

  alias Flezha.Plugin.Extension.Table.Workers
  alias Flezha.Plugin.Provider.HttpPoll
  alias FlezhaTest.Helper.ProviderPrepare

  # Two plugins activated as the extension table runs them (`Table.Workers.start/5`).
  defp start_pair(host_a, host_b) do
    origin_a = start_origin(body: "from-a")
    origin_b = start_origin(body: "from-b")
    {:ok, opts_a} = prepare(origin_a, host_a)
    {:ok, opts_b} = prepare(origin_b, host_b)

    {:ok, pid_a} = ProviderPrepare.activate(HttpPoll, opts_a)
    await_stop_on_exit(pid_a)
    assert {"from-a", _} = take_ingest()

    {:ok, pid_b} = ProviderPrepare.activate(HttpPoll, opts_b)
    if pid_b != pid_a, do: await_stop_on_exit(pid_b)

    %{opts_a: opts_a, pid_a: pid_a, pid_b: pid_b, origin_b: origin_b}
  end

  defp disable_a(ctx) do
    table = %{entries: %{}, monitors: %{}}
    Workers.stop_all(table, ctx.opts_a.source_plugin_id, HttpPoll, ctx.opts_a, & &1.())
    refute Process.alive?(ctx.pid_a)
  end

  defp assert_b_polled(ctx) do
    assert {"from-b", _} = take_ingest()
    assert requests(ctx.origin_b) == 1
  end

  describe "one name in two namespaces" do
    setup do
      name = "ticker_#{System.unique_integer([:positive])}"

      start_pair(
        %{source_namespace: "ns_a", source_plugin_id: name},
        %{source_namespace: "ns_b", source_plugin_id: name}
      )
    end

    test "runs two workers", ctx do
      assert ctx.pid_b != ctx.pid_a, "namespace ns_b was handed ns_a's worker"
      assert_b_polled(ctx)
    end

    test "a disable in one namespace leaves the other's worker running", ctx do
      disable_a(ctx)
      assert Process.alive?(ctx.pid_b), "disabling in ns_a stopped ns_b's worker"
    end
  end

  describe "two names with one plugin_id in one namespace" do
    setup do
      n = System.unique_integer([:positive])

      start_pair(
        %{source_namespace: "ns", source_plugin_id: "a_#{n}", plugin_id: "id_#{n}"},
        %{source_namespace: "ns", source_plugin_id: "b_#{n}", plugin_id: "id_#{n}"}
      )
    end

    test "run two workers", ctx do
      assert ctx.pid_b != ctx.pid_a, "plugin b was handed plugin a's worker"
      assert_b_polled(ctx)
    end

    test "a disable of one leaves the other's worker running", ctx do
      disable_a(ctx)
      assert Process.alive?(ctx.pid_b), "disabling plugin a stopped plugin b's worker"
    end
  end
end
