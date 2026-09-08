defmodule FlezhaTest.Service.CliManagerSubscriptionTest do
  @moduledoc """
  Guards the operator-facing subscription vocabulary: the legacy
  `--trigger=<mode>` mapping, and the `--extension=NAME --NAME.key=value` and
  `--plugins` spec paths that carry every value as text.

  Fails when: a `--trigger=proactive` plugin is wired to `:on_terminate`
  (or any non-`:on_interval` extension), so a proactive plugin would never
  receive its periodic tick — the regression this test was written to catch;
  or when a sub-option declared as text does not reach `Extension.Table` as
  its declared type, which made the whole `--extension` vocabulary
  inexpressible.
  """
  use ExUnit.Case, async: true

  # An unconvertible subscription option is driven on purpose and the table
  # reports the halt at error level; the refusal is asserted as a return value.
  @moduletag :capture_log

  alias Flezha.Operative.Overwatch
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
    def on_operative_init(ctx, _opts), do: {:ok, ctx}
    @impl true
    def on_input(ctx, _opts), do: {:ok, ctx}
    @impl true
    def on_interval(_ctx, _opts), do: :ok
    @impl true
    def on_terminate(_ctx, _opts), do: :ok
    @impl true
    def on_message(%Ctx.Msg{payload: p}, _opts), do: {:ok, "seen:" <> p}
  end

  defp cli_subscriptions(extensions) do
    {:ok, subs} = CliPlugin.extract_subscriptions(%{"extensions" => extensions}, %{})
    subs
  end

  defp extension(name, opts), do: %{"name" => name, "opts" => opts}

  defp subscribe(subs, overrides \\ []) do
    plugin = struct(%Plugin{id: "cli", provider: EveryKind, subscriptions: subs}, overrides)

    Table.bulk_subscribe(Table.new(Overwatch.__extensions_catalog__()), [{plugin.id, plugin}])
  end

  describe "the --trigger vocabulary" do
    test "every row names the subscriptions it stands for" do
      for {written, subscriptions} <- [
            {"none", []},
            {"on_input", [{:on_input, []}]},
            {"on_output", [{:on_output, []}]},
            {"interval", [{:on_interval, [interval_ms: 5_000]}]},
            {"proactive", [{:on_interval, [interval_ms: 5_000]}]}
          ] do
        assert CliPlugin.trigger_to_subscription(written) == {:ok, subscriptions}
      end
    end

    # Fails when: an enable that names neither a trigger nor an extension is
    # given a subscription it never asked for. `on_input` used to be that
    # answer, and it is a SEND on every provider whose two io callbacks are
    # one function.
    test "an enable naming neither a trigger nor an extension subscribes to nothing" do
      assert CliPlugin.extract_subscriptions(%{}, %{}) == {:ok, []}
      assert CliPlugin.trigger_to_subscription(nil) == {:ok, []}
    end

    # Fails when: a typo is coerced onto a row instead of being refused, which
    # changes what the plugin is bound to with nothing said to the operator.
    test "a value naming no row is refused, naming the value and the accepted set" do
      assert {:error, message} = CliPlugin.trigger_to_subscription("on_inptu")

      assert message ==
               "unknown --trigger 'on_inptu'. Use 'none' or 'on_input' or " <>
                 "'on_output' or 'interval' or 'proactive'."
    end
  end

  # Fails when: an enable that names no trigger reaches the provider bound in a
  # direction nobody asked for. The websocket's `on_input` and `on_output` are
  # the same send, so a source made an echo ships every payload the operative
  # ingests back out on the socket it arrived on.
  test "a source-only enable subscribes to nothing, allocates no sink and ships nothing" do
    plugin_id = "cli_source_only_#{System.unique_integer([:positive])}"
    ProviderPrepare.stub_sink_worker(Websocket, plugin_id)

    assert {:ok, subscriptions} = CliPlugin.extract_subscriptions(%{}, %{})

    {:ok, table} =
      subscribe(subscriptions,
        id: plugin_id,
        provider: Websocket,
        opts: %{uri: "wss://flezha-test.invalid:9/never-connect", plugin_id: plugin_id}
      )

    assert %Plugin{opts: opts} = Map.fetch!(table.plugins, plugin_id)
    assert opts.subscribed_extensions == []
    assert opts.sink == nil

    refute Extension.active?(table, :on_input)

    ctx = %Ctx.IO{operative_id: "op", namespace: :default, message: "in", metadata: %{}}

    assert {:ok, ^ctx} = Extension.run(table, :on_input, ctx)
    refute_receive {:shipped, _, _}, 200
  end

  # Fails when: a subscription directive reaches `init_provider/1` as a plugin
  # option — a provider with a strict allow-list (http_poll) then refuses every
  # `flz plugin enable` that names an extension or a trigger.
  test "subscription directives are stripped from the provider's options" do
    opts =
      CliPlugin.provider_opts(%{
        "url" => "http://x",
        "trigger" => "on_input",
        "extensions" => [%{"name" => "on_terminate", "opts" => %{}}]
      })

    assert opts == %{url: "http://x"}
  end

  describe "the --extension vocabulary reaches the table typed" do
    test "an integer, an enum, a string and no sub-options at all" do
      subs =
        cli_subscriptions([
          extension("on_interval", %{"interval_ms" => "5000"}),
          extension("on_operative_init", %{"on_error" => "empty", "timeout_ms" => "1000"}),
          extension("on_message", %{"key" => "orders"}),
          extension("on_terminate", %{})
        ])

      {:ok, table} = subscribe(subs)

      assert %Plugin{subscriptions: stored} = Map.fetch!(table.plugins, "cli")

      assert stored == [
               {:on_interval, [interval_ms: 5_000]},
               {:on_operative_init, [timeout_ms: 1_000, on_error: "empty"]},
               {:on_message, [key: "orders"]},
               {:on_terminate, []}
             ]
    end

    test "an ordered kind lands at the position the operator typed" do
      subs = cli_subscriptions([extension("on_input", %{"position" => "0"})])

      {:ok, table} = subscribe(subs)

      assert [%{position: 0}] = Map.fetch!(table.subs, :on_input)
    end

    test "an ill-typed sub-option is refused naming the option, never raised" do
      subs = cli_subscriptions([extension("on_interval", %{"interval_ms" => "soon"})])

      assert {:error, {_id, {:invalid_subscription_opts, {:interval_ms, "soon"}}}} =
               subscribe(subs)
    end

    # Fails when: a `:request` key declared on the CLI registers a subscriber
    # keyed on something dispatch can never match — accepted at enable, then
    # silently unreachable.
    test "a key declared on the CLI is reachable at dispatch" do
      subs = cli_subscriptions([extension("on_message", %{"key" => "orders"})])

      {:ok, table} = subscribe(subs)

      ref = make_ref()

      ctx = %Ctx.Msg{
        key: "orders",
        payload: "hi",
        reply_to: self(),
        ref: ref,
        operative_id: "p",
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
            "id" => "spec_p",
            "provider" => "operative_stdout",
            "opts" => %{},
            "subscriptions" => [
              %{"name" => "on_interval", "opts" => %{"interval_ms" => "5000"}},
              %{"name" => "on_operative_init", "opts" => %{"position" => "0"}},
              %{"name" => "on_message", "opts" => %{"key" => "orders"}},
              "on_terminate"
            ]
          }
        ])

      assert {:ok, [plugin]} = SpecDecoder.decode(spec, "default", "spec_op")

      assert {:ok, %Plugin{subscriptions: subs}} =
               Table.canonicalise(Overwatch.__extensions_catalog__(), plugin)

      assert subs == [
               {:on_interval, [interval_ms: 5_000]},
               {:on_operative_init, [position: 0]},
               {:on_message, [key: "orders"]},
               {:on_terminate, []}
             ]
    end

    test "an ill-typed value in the spec is refused naming the option" do
      spec =
        Jason.encode!([
          %{
            "id" => "spec_bad",
            "provider" => "operative_stdout",
            "opts" => %{},
            "subscriptions" => [%{"name" => "on_input", "opts" => %{"position" => "first"}}]
          }
        ])

      assert {:ok, [plugin]} = SpecDecoder.decode(spec, "default", "spec_op")

      assert {:error, {:invalid_subscription_opts, {:position, "first"}}} =
               Table.canonicalise(Overwatch.__extensions_catalog__(), plugin)
    end
  end
end
