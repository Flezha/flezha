defmodule Flezha.Observability.PubSubSinkTest do
  use ExUnit.Case, async: false

  alias Flezha.Observability
  alias Flezha.Observability.{Bus, PubSubSink, Registry, Signal}

  setup do
    # PubSub sink is disabled by default in test config. Register it
    # here for the duration of the test, then unregister on exit.
    sink_name = :"obs_pubsub_sink_test_#{:erlang.unique_integer([:positive])}"
    src = :"obs_pubsub_src_#{:erlang.unique_integer([:positive])}"

    :ok =
      Registry.register_source(%{
        name: src,
        kinds: [:event],
        flavour: :declarative
      })

    case Registry.lookup_sink(:pubsub) do
      {:ok, _} ->
        # The default :pubsub sink is already registered (probably by
        # a prior test that wasn't cleaned up). Reuse it.
        :ok

      :error ->
        {:ok, _pid} =
          Registry.register_sink(%{
            name: sink_name,
            module: PubSubSink,
            config: %{}
          })

        on_exit(fn -> Registry.unregister_sink(sink_name) end)
    end

    on_exit(fn -> Registry.unregister_source(src) end)
    {:ok, src: src}
  end

  test "subscriber receives one message per emit", %{src: src} do
    :ok = Observability.subscribe(src)

    Bus.emit(Signal.new(%{kind: :event, source: src, name: :hello}))

    assert_receive {:flezha_signal, %Signal{source: ^src, name: :hello}}, 500

    Observability.unsubscribe(src)
  end

  test "unsubscribed process receives nothing", %{src: src} do
    Observability.subscribe(src)
    Observability.unsubscribe(src)

    Bus.emit(Signal.new(%{kind: :event, source: src, name: :ignored}))

    refute_receive {:flezha_signal, _}, 100
  end

  test "sink-disabled config: subscribe returns error when no sink running" do
    # Temporarily simulate the disabled state by unregistering the
    # PubSub sink. The subscribe helper should then report the disabled
    # error path.
    Registry.unregister_sink(:pubsub)

    # Re-fetch — should be gone.
    assert :error = Registry.lookup_sink(:pubsub)

    src = :"obs_disabled_pubsub_#{:erlang.unique_integer([:positive])}"
    assert {:error, :pubsub_sink_disabled} = Observability.subscribe(src)
  end
end
