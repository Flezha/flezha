defmodule FlezhaTest.Processor.InitStateResolverTest do
  alias Flezha.Processor.InitStatePlugin
  alias Flezha.Processor.InitStateResolver

  use ExUnit.Case, async: true

  @base_context %{
    processor_id: "proc-1",
    namespace: :default,
    factory_id: "factory-1"
  }

  defmodule StaticProvider do
    @behaviour Flezha.Processor.InitStateProvider
    @impl true
    def resolve(context), do: {:ok, Map.get(context.opts, :return, %{})}
  end

  defmodule ErrorProvider do
    @behaviour Flezha.Processor.InitStateProvider
    @impl true
    def resolve(context), do: {:error, Map.get(context.opts, :reason, :boom)}
  end

  defmodule SlowProvider do
    @behaviour Flezha.Processor.InitStateProvider
    @impl true
    def resolve(_context) do
      Process.sleep(200)
      {:ok, %{late: true}}
    end
  end

  defmodule RaiseProvider do
    @behaviour Flezha.Processor.InitStateProvider
    @impl true
    def resolve(_context), do: raise("kaboom")
  end

  defmodule ContextProbe do
    @behaviour Flezha.Processor.InitStateProvider
    @impl true
    def resolve(context) do
      pid = Map.fetch!(context.opts, :report_to)
      send(pid, {:probe, context})
      {:ok, %{probed: true}}
    end
  end

  test "empty plugin list returns {:ok, %{}}" do
    assert {:ok, %{}} == InitStateResolver.resolve([], @base_context)
  end

  test "single plugin success" do
    plugin = %InitStatePlugin{provider: StaticProvider, opts: %{return: %{"a" => 1}}}
    assert {:ok, %{"a" => 1}} == InitStateResolver.resolve([plugin], @base_context)
  end

  test "multi-plugin shallow merge with later key overriding earlier" do
    p1 = %InitStatePlugin{provider: StaticProvider, opts: %{return: %{"a" => 1, "b" => 2}}}
    p2 = %InitStatePlugin{provider: StaticProvider, opts: %{return: %{"b" => 99, "c" => 3}}}

    assert {:ok, %{"a" => 1, "b" => 99, "c" => 3}} ==
             InitStateResolver.resolve([p1, p2], @base_context)
  end

  test "context carries processor_id, namespace, factory_id, accumulated, opts" do
    p1 = %InitStatePlugin{provider: StaticProvider, opts: %{return: %{"from_p1" => true}}}

    p2 = %InitStatePlugin{
      provider: ContextProbe,
      opts: %{report_to: self(), some_opt: :x}
    }

    assert {:ok, _} = InitStateResolver.resolve([p1, p2], @base_context)

    assert_receive {:probe, context}
    assert context.processor_id == "proc-1"
    assert context.namespace == :default
    assert context.factory_id == "factory-1"
    assert context.accumulated == %{"from_p1" => true}
    assert context.opts == %{report_to: self(), some_opt: :x}
  end

  test "first plugin in chain receives empty accumulator" do
    plugin = %InitStatePlugin{
      provider: ContextProbe,
      opts: %{report_to: self()}
    }

    assert {:ok, _} = InitStateResolver.resolve([plugin], @base_context)
    assert_receive {:probe, context}
    assert context.accumulated == %{}
  end

  test ":fail on_error aborts chain with {:error, reason} and skips later plugins" do
    p1 = %InitStatePlugin{
      provider: ErrorProvider,
      opts: %{reason: :db_unreachable},
      on_error: :fail
    }

    p2 = %InitStatePlugin{
      provider: ContextProbe,
      opts: %{report_to: self()}
    }

    assert {:error, :db_unreachable} == InitStateResolver.resolve([p1, p2], @base_context)
    refute_receive {:probe, _}, 50
  end

  test ":empty on_error skips failing plugin and continues" do
    p1 = %InitStatePlugin{
      provider: ErrorProvider,
      opts: %{reason: :nope},
      on_error: :empty
    }

    p2 = %InitStatePlugin{provider: StaticProvider, opts: %{return: %{"after" => true}}}

    assert {:ok, %{"after" => true}} == InitStateResolver.resolve([p1, p2], @base_context)
  end

  test "timeout: slow provider is killed and treated as error per on_error" do
    fail_plugin = %InitStatePlugin{
      provider: SlowProvider,
      timeout_ms: 20,
      on_error: :fail
    }

    assert {:error, :timeout} == InitStateResolver.resolve([fail_plugin], @base_context)

    empty_plugin = %InitStatePlugin{
      provider: SlowProvider,
      timeout_ms: 20,
      on_error: :empty
    }

    assert {:ok, %{}} == InitStateResolver.resolve([empty_plugin], @base_context)
  end

  test "exception: provider that raises is captured, not propagated" do
    fail_plugin = %InitStatePlugin{provider: RaiseProvider, on_error: :fail}

    assert {:error, {:exception, :error, %RuntimeError{}, _}} =
             InitStateResolver.resolve([fail_plugin], @base_context)

    empty_plugin = %InitStatePlugin{provider: RaiseProvider, on_error: :empty}
    assert {:ok, %{}} == InitStateResolver.resolve([empty_plugin], @base_context)
  end

  test "invalid entry: non-struct → :invalid_init_state_plugin" do
    assert {:error, {:invalid_init_state_plugin, {:not_a_struct, _}}} =
             InitStateResolver.resolve([%{foo: :bar}], @base_context)
  end

  test "invalid entry: missing module → :invalid_init_state_plugin" do
    plugin = %InitStatePlugin{provider: DoesNotExist}

    assert {:error, {:invalid_init_state_plugin, {:module_not_loaded, DoesNotExist}}} =
             InitStateResolver.resolve([plugin], @base_context)
  end

  test "invalid entry: module without resolve/1 → :invalid_init_state_plugin" do
    plugin = %InitStatePlugin{provider: Enum}

    assert {:error, {:invalid_init_state_plugin, {:missing_callback, Enum, :resolve, 1}}} =
             InitStateResolver.resolve([plugin], @base_context)
  end

  test "invalid entry on_error: :empty is skipped rather than aborting" do
    plugin = %InitStatePlugin{provider: Enum, on_error: :empty}
    later = %InitStatePlugin{provider: StaticProvider, opts: %{return: %{ok: true}}}

    assert {:ok, %{ok: true}} == InitStateResolver.resolve([plugin, later], @base_context)
  end
end
