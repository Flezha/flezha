defmodule FlezhaTest.Plugin.ExtensionTest do
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host

  use ExUnit.Case, async: true

  import ExUnit.CaptureLog, only: [capture_log: 1, with_log: 1]

  @catalog Host.build_catalog(
             [
               {:on_operative_init, :fold, Ctx.Init},
               {:on_input, :transform, Ctx.IO},
               {:on_snapshot, :fanout, Ctx.Snapshot},
               {:on_message, :request, Ctx.Msg},
               {:on_interval, :timer, Ctx.Tick}
             ],
             %{file: __ENV__.file, line: __ENV__.line}
           )

  defmodule FoldA do
    def on_operative_init(%Ctx.Init{} = ctx, _opts) do
      {:ok, %Ctx.Init{ctx | accumulated: Map.put(ctx.accumulated, :a, 1)}}
    end
  end

  defmodule FoldB do
    def on_operative_init(%Ctx.Init{} = ctx, _opts) do
      {:ok, %Ctx.Init{ctx | accumulated: Map.put(ctx.accumulated, :b, 2)}}
    end
  end

  defmodule FoldErr do
    def on_operative_init(_ctx, _opts), do: {:error, :boom}
  end

  defmodule TimerPing do
    def on_interval(%Ctx.Tick{} = ctx, opts) do
      send(Map.fetch!(opts, :pid), {:tick, ctx.operative_id})
      :ok
    end
  end

  defmodule TransformUpcase do
    def on_input(%Ctx.IO{message: m} = ctx, _opts),
      do: {:ok, %Ctx.IO{ctx | message: String.upcase(m)}}
  end

  defmodule TransformSkip do
    def on_input(_ctx, _opts), do: {:skip, :rate_limited}
  end

  defmodule TransformError do
    def on_input(_ctx, _opts), do: {:error, :backend}
  end

  defmodule FanoutNotify do
    def on_snapshot(%Ctx.Snapshot{} = ctx, opts) do
      send(Map.fetch!(opts, :pid), {:fanout_called, ctx.operative_id})
      :ok
    end
  end

  defmodule FanoutRaise do
    def on_snapshot(_ctx, opts) do
      send(Map.fetch!(opts, :pid), :fanout_raise_seen)
      raise "kaboom"
    end
  end

  defmodule MsgEcho do
    def on_message(%Ctx.Msg{payload: p}, _opts), do: {:ok, "echo:" <> p}
  end

  describe ":fold dispatch" do
    test "empty returns ctx unchanged (fast path)" do
      table = Table.new(@catalog)
      ctx = %Ctx.Init{operative_id: "p", namespace: :n, factory_id: "f", accumulated: %{}}

      assert {:ok, ^ctx} = Extension.run(table, :on_operative_init, ctx)
    end

    test "single subscriber merges its accumulated" do
      table = Table.new(@catalog)

      {:ok, table} =
        Table.subscribe(table, "a", %Plugin{
          id: "a",
          provider: FoldA,
          opts: %{},
          subscriptions: [{:on_operative_init, []}]
        })

      ctx = %Ctx.Init{operative_id: "p", namespace: :n, factory_id: "f", accumulated: %{}}

      assert {:ok, %Ctx.Init{accumulated: %{a: 1}}} =
               Extension.run(table, :on_operative_init, ctx)
    end

    test "two subscribers chain in position order" do
      table = Table.new(@catalog)

      {:ok, table} =
        Table.subscribe(table, "a", %Plugin{
          id: "a",
          provider: FoldA,
          opts: %{},
          subscriptions: [{:on_operative_init, [position: 0]}]
        })

      {:ok, table} =
        Table.subscribe(table, "b", %Plugin{
          id: "b",
          provider: FoldB,
          opts: %{},
          subscriptions: [{:on_operative_init, [position: 1]}]
        })

      ctx = %Ctx.Init{operative_id: "p", namespace: :n, factory_id: "f", accumulated: %{}}

      assert {:ok, %Ctx.Init{accumulated: %{a: 1, b: 2}}} =
               Extension.run(table, :on_operative_init, ctx)
    end

    test "on_error: :fail halts chain" do
      table = Table.new(@catalog)

      {:ok, table} =
        Table.subscribe(table, "e", %Plugin{
          id: "e",
          provider: FoldErr,
          opts: %{},
          subscriptions: [{:on_operative_init, [on_error: :fail]}]
        })

      ctx = %Ctx.Init{operative_id: "p", namespace: :n, factory_id: "f", accumulated: %{}}
      assert {:error, :boom} = Extension.run(table, :on_operative_init, ctx)
    end

    test "on_error: :empty skips failure" do
      table = Table.new(@catalog)

      {:ok, table} =
        Table.subscribe(table, "e", %Plugin{
          id: "e",
          provider: FoldErr,
          opts: %{},
          subscriptions: [{:on_operative_init, [on_error: :empty]}]
        })

      {:ok, table} =
        Table.subscribe(table, "b", %Plugin{
          id: "b",
          provider: FoldB,
          opts: %{},
          subscriptions: [{:on_operative_init, [position: 1]}]
        })

      ctx = %Ctx.Init{operative_id: "p", namespace: :n, factory_id: "f", accumulated: %{}}

      assert {:ok, %Ctx.Init{accumulated: %{b: 2}}} =
               Extension.run(table, :on_operative_init, ctx)
    end
  end

  describe ":transform dispatch" do
    test "empty returns ctx unchanged" do
      table = Table.new(@catalog)
      ctx = %Ctx.IO{operative_id: "p", namespace: :n, message: "x", metadata: %{}}

      assert {:ok, ^ctx} = Extension.run(table, :on_input, ctx)
    end

    test "subscriber transforms payload" do
      table = Table.new(@catalog)

      {:ok, table} =
        Table.subscribe(table, "u", %Plugin{
          id: "u",
          provider: TransformUpcase,
          opts: %{},
          subscriptions: [{:on_input, []}]
        })

      ctx = %Ctx.IO{operative_id: "p", namespace: :n, message: "hi", metadata: %{}}
      assert {:ok, %Ctx.IO{message: "HI"}} = Extension.run(table, :on_input, ctx)
    end

    test ":skip halts chain" do
      table = Table.new(@catalog)

      {:ok, table} =
        Table.subscribe(table, "s", %Plugin{
          id: "s",
          provider: TransformSkip,
          opts: %{},
          subscriptions: [{:on_input, [position: 0]}]
        })

      ctx = %Ctx.IO{operative_id: "p", namespace: :n, message: "x", metadata: %{}}
      assert {:skip, :rate_limited} = Extension.run(table, :on_input, ctx)
    end

    test ":error is logged and chain continues with prior ctx" do
      table = Table.new(@catalog)

      {:ok, table} =
        Table.subscribe(table, "u", %Plugin{
          id: "u",
          provider: TransformUpcase,
          opts: %{},
          subscriptions: [{:on_input, [position: 0]}]
        })

      {:ok, table} =
        Table.subscribe(table, "err", %Plugin{
          id: "err",
          provider: TransformError,
          opts: %{},
          subscriptions: [{:on_input, [position: 1]}]
        })

      ctx = %Ctx.IO{operative_id: "p", namespace: :n, message: "hi", metadata: %{}}

      log =
        capture_log(fn ->
          assert {:ok, %Ctx.IO{message: "HI"}} = Extension.run(table, :on_input, ctx)
        end)

      # The chain logs the subscriber error (intentional — runners rely
      # on it during incident triage) and we assert it lands without
      # spilling into the suite's stdout.
      assert log =~ "transform subscriber"
    end
  end

  describe ":fanout dispatch" do
    test "empty returns :ok without invoking anything" do
      table = Table.new(@catalog)
      ctx = %Ctx.Snapshot{operative_id: "p", namespace: :n, snapshot: nil}

      assert :ok = Extension.run(table, :on_snapshot, ctx)
    end

    test "every subscriber runs; exceptions isolated" do
      table = Table.new(@catalog)

      {:ok, table} =
        Table.subscribe(table, "r", %Plugin{
          id: "r",
          provider: FanoutRaise,
          opts: %{pid: self()},
          subscriptions: [{:on_snapshot, []}]
        })

      {:ok, table} =
        Table.subscribe(table, "n", %Plugin{
          id: "n",
          provider: FanoutNotify,
          opts: %{pid: self()},
          subscriptions: [{:on_snapshot, []}]
        })

      ctx = %Ctx.Snapshot{operative_id: "p", namespace: :n, snapshot: nil}

      log =
        capture_log(fn ->
          assert :ok = Extension.run(table, :on_snapshot, ctx)
          assert_received :fanout_raise_seen
          assert_received {:fanout_called, "p"}
        end)

      # The fanout dispatch logs each raise/throw — we capture it here so
      # the test's intentional `kaboom` doesn't pollute the suite output.
      assert log =~ "fanout subscriber"
    end
  end

  describe ":request dispatch" do
    test "empty delivers :unknown_key to reply_to" do
      table = Table.new(@catalog)

      ref = make_ref()

      ctx = %Ctx.Msg{
        key: "nope",
        payload: <<>>,
        reply_to: self(),
        ref: ref,
        operative_id: "p",
        namespace: "n"
      }

      assert :ok = Extension.run(table, :on_message, ctx)
      assert_receive {:flezha_response, ^ref, {:error, {:unknown_key, :on_message, "nope"}}}, 200
    end

    test "keyed dispatch invokes subscriber" do
      table = Table.new(@catalog)

      {:ok, table} =
        Table.subscribe(table, "e", %Plugin{
          id: "e",
          provider: MsgEcho,
          opts: %{},
          subscriptions: [{:on_message, [key: "echo"]}]
        })

      ref = make_ref()

      ctx = %Ctx.Msg{
        key: "echo",
        payload: "hi",
        reply_to: self(),
        ref: ref,
        operative_id: "p",
        namespace: "n"
      }

      assert :ok = Extension.run(table, :on_message, ctx)
      assert_receive {:flezha_response, ^ref, {:ok, "echo:hi"}}, 500
    end
  end

  describe "subscription canonicalisation" do
    test "every sub-option coerces from the text an operator types" do
      assert {:ok, %Plugin{subscriptions: subs}} =
               Table.canonicalise(@catalog, %Plugin{
                 id: "c",
                 provider: MsgEcho,
                 subscriptions: [
                   {"on_interval", %{"interval_ms" => "5000"}},
                   {"on_operative_init",
                    %{"position" => "1", "timeout_ms" => "1000", "on_error" => "empty"}},
                   {"on_input", %{"position" => "0"}},
                   {"on_message", %{"key" => "orders"}}
                 ]
               })

      assert subs == [
               {:on_interval, [interval_ms: 5_000]},
               {:on_operative_init, [position: 1, timeout_ms: 1_000, on_error: "empty"]},
               {:on_input, [position: 0]},
               {:on_message, [key: "orders"]}
             ]
    end

    test "canonical input is returned unchanged" do
      canonical = [
        {:on_interval, [interval_ms: 5_000]},
        {:on_operative_init, [position: 1, timeout_ms: 1_000, on_error: "fail"]},
        {:on_message, [key: "orders"]},
        {:on_snapshot, []}
      ]

      plugin = %Plugin{id: "c", provider: MsgEcho, subscriptions: canonical}

      assert {:ok, ^plugin} = Table.canonicalise(@catalog, plugin)
    end

    test "a value that cannot be coerced is refused naming the sub-option" do
      for {name, opts, expected} <- [
            {"on_interval", %{"interval_ms" => "soon"}, {:interval_ms, "soon"}},
            {"on_operative_init", %{"timeout_ms" => :bad}, {:timeout_ms, :bad}},
            {"on_operative_init", %{"on_error" => "shrug"}, {:on_error, "shrug"}},
            {"on_message", %{"key" => 7}, {:key, 7}}
          ] do
        assert {:error, {:invalid_subscription_opts, ^expected}} =
                 Table.canonicalise(@catalog, %Plugin{
                   id: "c",
                   provider: MsgEcho,
                   subscriptions: [{name, opts}]
                 })
      end
    end

    test "a sub-option the kind does not declare is refused" do
      assert {:error, {:unknown_subscription_opt, :timer, "interval"}} =
               Table.canonicalise(@catalog, %Plugin{
                 id: "c",
                 provider: MsgEcho,
                 subscriptions: [{"on_interval", %{"interval" => "5000"}}]
               })
    end

    test "an unknown extension name is refused without being interned" do
      name = "on_never_declared_#{System.unique_integer([:positive])}"

      assert {:error, {:unknown_extension, ^name}} =
               Table.canonicalise(@catalog, %Plugin{
                 id: "c",
                 provider: MsgEcho,
                 subscriptions: [{name, %{}}]
               })

      assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
    end

    test "position: \"0\" is an integer on both ordered kinds, and a bad one never raises" do
      table = Table.new(@catalog)

      {:ok, table} =
        Table.subscribe(table, "f", %Plugin{
          id: "f",
          provider: FoldA,
          subscriptions: [{"on_operative_init", %{"position" => "0"}}]
        })

      {:ok, table} =
        Table.subscribe(table, "t", %Plugin{
          id: "t",
          provider: TransformUpcase,
          subscriptions: [{"on_input", %{"position" => "0"}}]
        })

      assert [%{position: 0}] = Map.fetch!(table.subs, :on_operative_init)
      assert [%{position: 0}] = Map.fetch!(table.subs, :on_input)

      for {provider, name} <- [{FoldA, "on_operative_init"}, {TransformUpcase, "on_input"}] do
        assert {:error, {:invalid_subscription_opts, {:position, "first"}}} =
                 Table.subscribe(table, "bad", %Plugin{
                   id: "bad",
                   provider: provider,
                   subscriptions: [{name, %{"position" => "first"}}]
                 })

        assert {:error, {:invalid_subscription_opts, {:position, -1}}} =
                 Table.subscribe(table, "bad", %Plugin{
                   id: "bad",
                   provider: provider,
                   subscriptions: [{name, %{"position" => "-1"}}]
                 })
      end
    end

    test "on_error declared as text drives the empty-on-error fold behaviour" do
      table = Table.new(@catalog)

      {:ok, table} =
        Table.subscribe(table, "e", %Plugin{
          id: "e",
          provider: FoldErr,
          subscriptions: [{"on_operative_init", %{"on_error" => "empty"}}]
        })

      ctx = %Ctx.Init{operative_id: "p", namespace: :n, factory_id: "f", accumulated: %{}}

      assert {:ok, %Ctx.Init{}} = Extension.run(table, :on_operative_init, ctx)
    end
  end

  describe "validation" do
    test "unknown extension rejected at subscribe" do
      table = Table.new(@catalog)

      assert {:error, {:unknown_extension, :nope}} =
               Table.subscribe(table, "x", %Plugin{
                 id: "x",
                 provider: FoldA,
                 subscriptions: [{:nope, []}]
               })
    end

    test "missing callback rejected" do
      table = Table.new(@catalog)

      assert {:error, {:missing_callback, :on_input}} =
               Table.subscribe(table, "x", %Plugin{
                 id: "x",
                 provider: FoldA,
                 subscriptions: [{:on_input, []}]
               })
    end

    test "position on every unordered kind rejected" do
      table = Table.new(@catalog)

      for {name, provider, opts} <- [
            {:on_snapshot, FanoutNotify, [position: 0]},
            {:on_message, MsgEcho, [key: "k", position: 0]},
            {:on_interval, TimerPing, [interval_ms: 10, position: 0]}
          ] do
        assert {:error, :position_not_applicable} =
                 Table.subscribe(table, "x", %Plugin{
                   id: "x",
                   provider: provider,
                   opts: %{pid: self()},
                   subscriptions: [{name, opts}]
                 })
      end
    end

    test "key conflict on :request rejected" do
      table = Table.new(@catalog)

      {:ok, table} =
        Table.subscribe(table, "a", %Plugin{
          id: "a",
          provider: MsgEcho,
          subscriptions: [{:on_message, [key: "k"]}]
        })

      assert {:error, {:key_conflict, :on_message, "k"}} =
               Table.subscribe(table, "b", %Plugin{
                 id: "b",
                 provider: MsgEcho,
                 subscriptions: [{:on_message, [key: "k"]}]
               })
    end

    test "invalid timeout rejected on :fold" do
      table = Table.new(@catalog)

      assert {:error, {:invalid_subscription_opts, {:timeout_ms, :bad}}} =
               Table.subscribe(table, "x", %Plugin{
                 id: "x",
                 provider: FoldA,
                 subscriptions: [{:on_operative_init, [timeout_ms: :bad]}]
               })
    end

    test "invalid on_error rejected on :fold" do
      table = Table.new(@catalog)

      assert {:error, {:invalid_subscription_opts, {:on_error, :weird}}} =
               Table.subscribe(table, "x", %Plugin{
                 id: "x",
                 provider: FoldA,
                 subscriptions: [{:on_operative_init, [on_error: :weird]}]
               })
    end
  end

  defmodule FoldErrInit do
    def init_provider(_opts), do: {:error, :declined}
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule RaisingInit do
    def init_provider(_opts), do: raise(ArgumentError, "boom from init_provider")
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule ThrowingInit do
    def init_provider(_opts), do: throw("bare throw from init_provider")
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule ExitingInit do
    def init_provider(_opts), do: exit(:collaborator_gone)
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule CrashingOnReprepare do
    def init_provider(%{reprepare: :raise}), do: raise(ArgumentError, "boom on re-prepare")
    def init_provider(%{reprepare: :throw}), do: throw("bare throw on re-prepare")
    def init_provider(%{reprepare: :exit}), do: exit(:collaborator_gone)
    def init_provider(%{mode: mode} = opts), do: {:ok, Map.put(opts, :reprepare, mode)}
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  describe "a provider that crashes in init_provider/1 refuses the enable" do
    for {kind, provider} <- [
          {"raises", RaisingInit},
          {"throws a value that is not the internal sentinel", ThrowingInit},
          {"exits", ExitingInit}
        ] do
      test "a provider that #{kind} is refused, not escaped" do
        plugin = %Plugin{
          id: "crashing",
          provider: unquote(provider),
          opts: %{},
          subscriptions: [{:on_input, []}]
        }

        {result, log} =
          with_log(fn -> Table.subscribe(Table.new(@catalog), "crashing", plugin) end)

        assert {:error, reason} = result
        assert inspect(reason) =~ inspect(unquote(provider))
        assert log =~ "init_provider"
      end
    end

    # `attempt_restart/3` carries its own `rescue`, so a RAISE on the heal path
    # is already contained — but a throw and an exit are not, and the reason it
    # logs names neither the callback nor the provider.
    for mode <- [:raise, :throw, :exit] do
      test "the plugin-heal path refuses a provider that #{mode}s on re-prepare" do
        plugin = %Plugin{
          id: "heal",
          provider: CrashingOnReprepare,
          opts: %{mode: unquote(mode)},
          subscriptions: [{:on_input, []}]
        }

        {:ok, table} = Table.subscribe(Table.new(@catalog), "heal", plugin)

        {table, log} = with_log(fn -> Table.retry(table, "heal") end)

        assert %Table{} = table
        assert log =~ "init_provider"
        assert log =~ "CrashingOnReprepare"
        refute Map.has_key?(table.owned_pids, "heal")
      end
    end

    test "a provider whose init_provider/1 returns an error is unchanged" do
      plugin = %Plugin{
        id: "declined",
        provider: FoldErrInit,
        opts: %{},
        subscriptions: [{:on_input, []}]
      }

      assert {:error, :declined} = Table.subscribe(Table.new(@catalog), "declined", plugin)
    end
  end

  describe "the table reports what it dropped and issues no effect" do
    test "teardown_all names every plugin it held, in the order it tore them down" do
      table = table_with(["a", "b", "c"])
      held = Enum.map(table.plugins, fn {plugin_id, _plugin} -> plugin_id end)

      {emptied, dropped} = Table.teardown_all(table)

      assert dropped == held
      assert Enum.sort(dropped) == ["a", "b", "c"]
      assert emptied.plugins == %{}
    end

    test "teardown_all of a table holding nothing reports nothing" do
      assert {_table, []} = Table.teardown_all(Table.new(@catalog))
    end

    test "unsubscribe names the one plugin it dropped" do
      assert {_table, ["b"]} = Table.unsubscribe(table_with(["a", "b"]), "b")
    end

    test "unsubscribe of a plugin the table never held reports nothing" do
      assert {_table, []} = Table.unsubscribe(table_with(["a"]), "ghost")
    end

    test "the table names no Flezha.Service module" do
      {:ok, {_module, [imports: imports]}} =
        :beam_lib.chunks(:code.which(Table), [:imports])

      assert Enum.filter(imports, fn {module, _fun, _arity} ->
               module |> Atom.to_string() |> String.starts_with?("Elixir.Flezha.Service.")
             end) == []
    end
  end

  defp table_with(plugin_ids) do
    Enum.reduce(plugin_ids, Table.new(@catalog), fn plugin_id, table ->
      {:ok, table} =
        Table.subscribe(table, plugin_id, %Plugin{
          id: plugin_id,
          provider: FoldA,
          subscriptions: [{:on_operative_init, []}]
        })

      table
    end)
  end

  defmodule MaskProvider do
    def on_operative_init(%Ctx.Init{} = ctx, _opts), do: {:ok, ctx}
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
    def on_snapshot(%Ctx.Snapshot{} = ctx, _opts), do: {:ok, ctx}
    def on_message(_ctx, _opts), do: {:ok, %{}}
    def on_interval(%Ctx.Tick{} = ctx, _opts), do: {:ok, ctx}
  end

  describe "active_mask" do
    test "bit toggles on first and last subscriber" do
      table = Table.new(@catalog)
      assert table.active_mask == 0

      {:ok, table} =
        Table.subscribe(table, "a", %Plugin{
          id: "a",
          provider: FoldA,
          subscriptions: [{:on_operative_init, []}]
        })

      assert table.active_mask != 0

      {table, _dropped} = Table.unsubscribe(table, "a")
      assert table.active_mask == 0
    end

    # `add_subscription/6` and `remove_subscription/3` mutate only the
    # subscriber lists; `refresh_chain/2` sets the bit. `Extension.run/3`
    # short-circuits on it, so a wrong bit silently skips every subscriber.
    for {name, opts, other_opts} <- [
          {:on_operative_init, [], []},
          {:on_input, [position: 0], [position: 0]},
          {:on_snapshot, [], []},
          {:on_message, [key: "k1"], [key: "k2"]},
          {:on_interval, [interval_ms: 60_000], [interval_ms: 60_000]}
        ] do
      test "#{name} sets its bit on the first subscriber and clears it on the last" do
        plugin = fn id, o ->
          %Plugin{id: id, provider: MaskProvider, subscriptions: [{unquote(name), o}]}
        end

        empty = Table.new(@catalog)
        {:ok, one} = Table.subscribe(empty, "a", plugin.("a", unquote(opts)))
        {:ok, two} = Table.subscribe(one, "b", plugin.("b", unquote(other_opts)))
        {minus_one, _} = Table.unsubscribe(two, "a")
        {none, _} = Table.unsubscribe(minus_one, "b")

        assert empty.active_mask == 0
        assert one.active_mask != 0
        assert two.active_mask == one.active_mask
        assert minus_one.active_mask == one.active_mask
        assert none.active_mask == 0
      end
    end
  end
end
