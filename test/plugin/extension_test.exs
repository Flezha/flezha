defmodule FlezhaTest.Plugin.ExtensionTest do
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host

  use ExUnit.Case, async: true

  import ExUnit.CaptureLog, only: [capture_log: 1]

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

      # The chain logs the subscriber error (intentional — executors rely
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
        key: :nope,
        payload: <<>>,
        reply_to: self(),
        ref: ref,
        operative_id: "p",
        namespace: "n"
      }

      assert :ok = Extension.run(table, :on_message, ctx)
      assert_receive {:flezha_response, ^ref, {:error, {:unknown_key, :on_message, :nope}}}, 200
    end

    test "keyed dispatch invokes subscriber" do
      table = Table.new(@catalog)

      {:ok, table} =
        Table.subscribe(table, "e", %Plugin{
          id: "e",
          provider: MsgEcho,
          opts: %{},
          subscriptions: [{:on_message, [key: :echo]}]
        })

      ref = make_ref()

      ctx = %Ctx.Msg{
        key: :echo,
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

    test "position on fanout rejected" do
      table = Table.new(@catalog)

      assert {:error, :position_not_applicable} =
               Table.subscribe(table, "x", %Plugin{
                 id: "x",
                 provider: FanoutNotify,
                 opts: %{pid: self()},
                 subscriptions: [{:on_snapshot, [position: 0]}]
               })
    end

    test "key conflict on :request rejected" do
      table = Table.new(@catalog)

      {:ok, table} =
        Table.subscribe(table, "a", %Plugin{
          id: "a",
          provider: MsgEcho,
          subscriptions: [{:on_message, [key: :k]}]
        })

      assert {:error, {:key_conflict, :on_message, :k}} =
               Table.subscribe(table, "b", %Plugin{
                 id: "b",
                 provider: MsgEcho,
                 subscriptions: [{:on_message, [key: :k]}]
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

      table = Table.unsubscribe(table, "a")
      assert table.active_mask == 0
    end
  end
end
