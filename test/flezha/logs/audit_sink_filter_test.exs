defmodule Flezha.Logs.AuditSinkFilterTest do
  @moduledoc """
  The audit sink's bus filter is its allow-list's sources, so the bus never
  calls it for any other source and a producer of another source never waits
  on it; `:extra_subscriptions` widens the filter.
  """

  # async: false: application env, the observability registry, the audit kind.
  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  alias Flezha.Logs.{Kinds, Retention, Rotation, Sink, Writer}
  alias Flezha.Observability.{Bus, Signal}

  @moduletag :capture_log

  setup %{tmp: dir} do
    env = Application.get_env(:flezha, :audit_log, [])
    on_exit(fn -> Flezha.Observability.Registry.unregister_sink(:audit_log) end)

    configure = fn extra ->
      Application.put_env(
        :flezha,
        :audit_log,
        Keyword.merge(env, enabled: true, dir: dir, extra_subscriptions: extra)
      )
    end

    %{configure: configure}
  end

  test "the filter names the allow-list's sources and an extra one widens it", %{
    configure: configure
  } do
    configure.([])
    assert %{kinds: :any, sources: sources} = Sink.filter()
    assert :platform_authz in sources and :audit_log in sources
    refute :flezha_relay in sources

    configure.([{:my_source, :critical}])
    assert :my_source in Sink.filter().sources
  end

  test "a signal of a source outside the allow-list never reaches the sink", %{
    configure: configure
  } do
    configure.([])
    kind = Kinds.fetch!(:audit)

    start_supervised!(%{
      id: :audit_kind,
      start: {Supervisor, :start_link, [children(kind), [strategy: :one_for_one]]},
      type: :supervisor
    })

    {:ok, %{pid: sink}} = Flezha.Observability.Registry.lookup_sink(:audit_log)
    :erlang.trace(sink, true, [:receive])

    emit(:flezha_relay, :tick)
    emit(:platform_authz, :token_issued)

    assert_receive {:trace, ^sink, :receive,
                    {:"$gen_call", _, {:signal, %{source: :platform_authz}}}},
                   1_000

    refute_received {:trace, ^sink, :receive,
                     {:"$gen_call", _, {:signal, %{source: :flezha_relay}}}}

    assert Writer.stats(kind).current_record_count == 1
  end

  defp children(kind), do: [{Writer, kind}, {Rotation, kind}, {Retention, kind}, Sink]

  defp emit(source, name),
    do: Bus.emit(Signal.new(%{kind: :event, source: source, name: name, metadata: %{}}))
end
