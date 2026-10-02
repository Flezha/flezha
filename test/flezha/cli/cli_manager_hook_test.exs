defmodule FlezhaTest.Service.CliManagerHookTest do
  @moduledoc """
  Guards the caller-facing hook vocabulary: the `--hook=NAME
  --NAME.key=value` and `--plugins` spec paths that carry every value as text.

  Fails when: a sub-option declared as text does not reach `Extension.Table` as
  its declared type, which made the whole hook vocabulary inexpressible.
  """
  use ExUnit.Case, async: true

  # An unconvertible hook option is driven on purpose and the table
  # reports the halt at error level; the refusal is asserted as a return value.
  @moduletag :capture_log

  alias Flezha.Operator.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Plugin.SpecDecoder
  alias Flezha.Service.Cli.Plugin, as: CliPlugin
  alias FlezhaTest.Helper.ProviderPrepare

  defmodule EveryKind do
    use Flezha.Plugin.ProviderBehaviour

    @impl true
    def on_operator_init(ctx, _opts), do: {:ok, ctx}
    @impl true
    def on_input(ctx, _opts), do: {:ok, ctx}
    @impl true
    def on_interval(_ctx, _opts), do: :ok
    @impl true
    def on_terminate(_ctx, _opts), do: :ok
    @impl true
    def on_message(%Ctx.Msg{payload: p}, _opts), do: {:ok, "seen:" <> p}
  end

  defp cli_hooks(hooks) do
    {:ok, read, _provider_opts} = CliPlugin.read_settings(%{"hooks" => hooks})
    read
  end

  defp hook(name, opts), do: %{"name" => name, "opts" => opts}

  defp subscribe(hooks, overrides \\ []) do
    plugin = struct(%Plugin{id: "cli", provider: EveryKind, hooks: hooks}, overrides)

    Table.bulk_subscribe(Table.new(Overwatch.__extensions_catalog__()), [{plugin.id, plugin}])
  end

  # Fails when: an enable that names no hook is given one it never asked for.
  # `on_input` used to be that answer, and it is a SEND on every provider whose
  # two io callbacks are one function.
  test "an enable naming no hook has no hooks" do
    assert CliPlugin.read_settings(%{}) == {:ok, [], %{}}
  end

  # Fails when: the hooks a settings document names are not the hooks the
  # plugin gets, so `--hook` enables a plugin that sends nothing.
  test "a settings document's hooks are the plugin's hooks" do
    assert CliPlugin.read_settings(%{"hooks" => [hook("on_output", %{})]}) ==
             {:ok, [{"on_output", %{}}], %{}}
  end

  # Fails when: a hook entry of no shape the boundary reads crashes the verb
  # instead of being refused as a value.
  test "a hook entry of no known shape is refused" do
    assert CliPlugin.read_settings(%{"hooks" => [7]}) ==
             {:error, {:invalid_hook, :unrecognised_shape, 7}}
  end

  # Fails when: an enable that names no hook reaches the provider bound in a
  # direction nobody asked for. The websocket's `on_input` and `on_output` are
  # the same send, so a source made an echo ships every payload the operator
  # ingests back out on the socket it arrived on.
  test "a source-only enable has no hooks, allocates no sink and ships nothing" do
    plugin_id = "cli_source_only_#{System.unique_integer([:positive])}"
    ProviderPrepare.stub_sink_worker(Websocket, plugin_id)

    assert {:ok, hooks, _provider_opts} = CliPlugin.read_settings(%{})

    {:ok, table} =
      subscribe(hooks,
        id: plugin_id,
        provider: Websocket,
        opts: %{uri: "wss://flezha-test.invalid:9/never-connect", plugin_id: plugin_id}
      )

    assert %Plugin{opts: opts} = Map.fetch!(table.plugins, plugin_id)
    assert opts.subscribed_extensions == []
    assert opts.sink == nil

    refute Extension.active?(table, :on_input)

    ctx = %Ctx.IO{operator_id: "op", namespace: :default, message: "in", metadata: %{}}

    assert {:ok, ^ctx} = Extension.run(table, :on_input, ctx)
    refute_receive {:shipped, _, _}, 200
  end

  # Fails when: the hooks reach `init_provider/1` as a plugin option; a provider
  # with a strict allow-list (http_poll) then refuses every `flz plugin enable`
  # that names a hook.
  test "the hooks are stripped from the provider's options" do
    settings = %{"url" => "http://x", "hooks" => [hook("on_terminate", %{})]}

    assert CliPlugin.read_settings(settings) == {:ok, [{"on_terminate", %{}}], %{url: "http://x"}}
  end

  describe "the --hook vocabulary reaches the table typed" do
    test "an integer, an enum, a string and no sub-options at all" do
      subs =
        cli_hooks([
          hook("on_interval", %{"interval_ms" => "5000"}),
          hook("on_operator_init", %{"on_error" => "empty", "timeout_ms" => "1000"}),
          hook("on_message", %{"key" => "orders"}),
          hook("on_terminate", %{})
        ])

      {:ok, table} = subscribe(subs)

      assert %Plugin{hooks: stored} = Map.fetch!(table.plugins, "cli")

      assert stored == [
               {:on_interval, [interval_ms: 5_000]},
               {:on_operator_init, [timeout_ms: 1_000, on_error: "empty"]},
               {:on_message, [key: "orders"]},
               {:on_terminate, []}
             ]
    end

    test "an ordered kind lands at the position the caller typed" do
      subs = cli_hooks([hook("on_input", %{"position" => "0"})])

      {:ok, table} = subscribe(subs)

      assert [%{position: 0}] = Map.fetch!(table.subs, :on_input)
    end

    test "an ill-typed sub-option is refused naming the option, never raised" do
      subs = cli_hooks([hook("on_interval", %{"interval_ms" => "soon"})])

      assert {:error, {_id, {:invalid_hook_opts, :interval_ms, "soon"}}} =
               subscribe(subs)
    end

    # Fails when: an interval hook is given a period nobody wrote. The CLI has no
    # default for it, so the refusal is the table's.
    test "an interval hook with no interval is refused naming interval_ms" do
      assert {:error, {"cli", {:invalid_hook_opts, :interval_ms, nil}}} =
               subscribe(cli_hooks([hook("on_interval", %{})]))
    end

    # Fails when: a `:request` key declared on the CLI registers a subscriber
    # keyed on something dispatch can never match — accepted at enable, then
    # silently unreachable.
    test "a key declared on the CLI is reachable at dispatch" do
      subs = cli_hooks([hook("on_message", %{"key" => "orders"})])

      {:ok, table} = subscribe(subs)

      ref = make_ref()

      ctx = %Ctx.Msg{
        key: "orders",
        payload: "hi",
        reply_to: self(),
        ref: ref,
        operator_id: "p",
        namespace: "n"
      }

      assert :ok = Extension.run(table, :on_message, ctx)
      assert_receive {:flezha_response, ^ref, {:ok, "seen:hi"}}, 500
    end
  end

  describe "the --plugins spec path declares the same values" do
    test "a quoted number, an enum and a string all arrive typed" do
      spec =
        Jason.encode!([
          %{
            "name" => "spec_p",
            "provider" => "operator_stdout",
            "opts" => %{},
            "hooks" => [
              %{"name" => "on_interval", "opts" => %{"interval_ms" => "5000"}},
              %{"name" => "on_operator_init", "opts" => %{"position" => "0"}},
              %{"name" => "on_message", "opts" => %{"key" => "orders"}},
              "on_terminate"
            ]
          }
        ])

      assert {:ok, [plugin]} = SpecDecoder.decode(spec, "default", "spec_op")

      assert {:ok, %Plugin{hooks: subs}} =
               Table.canonicalise(Overwatch.__extensions_catalog__(), plugin)

      assert subs == [
               {:on_interval, [interval_ms: 5_000]},
               {:on_operator_init, [position: 0]},
               {:on_message, [key: "orders"]},
               {:on_terminate, []}
             ]
    end

    # Fails when: a spec that still names its hooks `subscriptions` decodes to a
    # binding with no hooks.
    test "hooks written under another key are refused" do
      spec =
        Jason.encode!([
          %{"name" => "old", "provider" => "operator_stdout", "subscriptions" => ["on_output"]}
        ])

      assert SpecDecoder.decode(spec, "default", "spec_op") ==
               {:error, {:plugin_fields_unknown, ["subscriptions"]}}
    end

    test "an ill-typed value in the spec is refused naming the option" do
      spec =
        Jason.encode!([
          %{
            "name" => "spec_bad",
            "provider" => "operator_stdout",
            "opts" => %{},
            "hooks" => [%{"name" => "on_input", "opts" => %{"position" => "first"}}]
          }
        ])

      assert {:ok, [plugin]} = SpecDecoder.decode(spec, "default", "spec_op")

      assert {:error, {:invalid_hook_opts, :position, "first"}} =
               Table.canonicalise(Overwatch.__extensions_catalog__(), plugin)
    end
  end
end
