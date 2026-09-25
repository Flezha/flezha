defmodule FlezhaTest.Plugin.ExtensionDispatchTest do
  @moduledoc """
  Extension-point DISPATCH: what each kind does with a subscriber's answer.

  A `:fold` accumulates or, on `on_error: :empty`, skips the failure; a
  `:transform` passes the context on, halts the chain on `:skip`, and carries
  the prior context past an `:error`; a `:fanout` runs every subscriber and
  isolates each exception; a `:request` reaches the one subscriber its key
  names. The subscription contract that gets a plugin onto the table is
  `extension_test.exs`.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host

  use ExUnit.Case, async: true

  import ExUnit.CaptureLog, only: [capture_log: 1]

  @catalog Host.build_catalog(
             [
               {:on_operator_init, :fold, Ctx.Init},
               {:on_input, :transform, Ctx.IO},
               {:on_snapshot, :fanout, Ctx.Snapshot},
               {:on_message, :request, Ctx.Msg},
               {:on_interval, :timer, Ctx.Tick}
             ],
             %{file: __ENV__.file, line: __ENV__.line}
           )

  defmodule FoldA do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def on_operator_init(%Ctx.Init{} = ctx, _opts) do
      {:ok, %Ctx.Init{ctx | accumulated: Map.put(ctx.accumulated, :a, 1)}}
    end
  end

  defmodule FoldB do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def on_operator_init(%Ctx.Init{} = ctx, _opts) do
      {:ok, %Ctx.Init{ctx | accumulated: Map.put(ctx.accumulated, :b, 2)}}
    end
  end

  defmodule FoldErr do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def on_operator_init(_ctx, _opts), do: {:error, :boom}
  end

  defmodule TimerPing do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def on_interval(%Ctx.Tick{} = ctx, opts) do
      send(Map.fetch!(opts, :pid), {:tick, ctx.operator_id})
      :ok
    end
  end

  defmodule TransformUpcase do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def on_input(%Ctx.IO{message: m} = ctx, _opts),
      do: {:ok, %Ctx.IO{ctx | message: String.upcase(m)}}
  end

  defmodule TransformSkip do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def on_input(_ctx, _opts), do: {:skip, :rate_limited}
  end

  defmodule TransformError do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def on_input(_ctx, _opts), do: {:error, :backend}
  end

  defmodule FanoutNotify do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def on_snapshot(%Ctx.Snapshot{} = ctx, opts) do
      send(Map.fetch!(opts, :pid), {:fanout_called, ctx.operator_id})
      :ok
    end
  end

  defmodule FanoutRaise do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def on_snapshot(_ctx, opts) do
      send(Map.fetch!(opts, :pid), :fanout_raise_seen)
      raise "kaboom"
    end
  end

  defmodule MsgEcho do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def on_message(%Ctx.Msg{payload: p}, _opts), do: {:ok, "echo:" <> p}
  end

  # The table's one subscribe takes a list; a single plugin is a list of one,
  # and every case here is about one.
  defp subscribe(table, plugin_id, plugin),
    do: Table.bulk_subscribe(table, [{plugin_id, plugin}])

  describe ":fold dispatch" do
    test "empty returns ctx unchanged (fast path)" do
      table = Table.new(@catalog)
      ctx = %Ctx.Init{operator_id: "p", namespace: :n, factory_id: "f", accumulated: %{}}

      assert {:ok, ^ctx} = Extension.run(table, :on_operator_init, ctx)
    end

    test "single subscriber merges its accumulated" do
      table = Table.new(@catalog)

      {:ok, table} =
        subscribe(table, "a", %Plugin{
          id: "a",
          provider: FoldA,
          opts: %{},
          hooks: [{:on_operator_init, []}]
        })

      ctx = %Ctx.Init{operator_id: "p", namespace: :n, factory_id: "f", accumulated: %{}}

      assert {:ok, %Ctx.Init{accumulated: %{a: 1}}} =
               Extension.run(table, :on_operator_init, ctx)
    end

    test "two subscribers chain in position order" do
      table = Table.new(@catalog)

      {:ok, table} =
        subscribe(table, "a", %Plugin{
          id: "a",
          provider: FoldA,
          opts: %{},
          hooks: [{:on_operator_init, [position: 0]}]
        })

      {:ok, table} =
        subscribe(table, "b", %Plugin{
          id: "b",
          provider: FoldB,
          opts: %{},
          hooks: [{:on_operator_init, [position: 1]}]
        })

      ctx = %Ctx.Init{operator_id: "p", namespace: :n, factory_id: "f", accumulated: %{}}

      assert {:ok, %Ctx.Init{accumulated: %{a: 1, b: 2}}} =
               Extension.run(table, :on_operator_init, ctx)
    end

    test "on_error: :fail halts chain" do
      table = Table.new(@catalog)

      {:ok, table} =
        subscribe(table, "e", %Plugin{
          id: "e",
          provider: FoldErr,
          opts: %{},
          hooks: [{:on_operator_init, [on_error: :fail]}]
        })

      ctx = %Ctx.Init{operator_id: "p", namespace: :n, factory_id: "f", accumulated: %{}}
      assert {:error, :boom} = Extension.run(table, :on_operator_init, ctx)
    end

    test "on_error: :empty skips failure" do
      table = Table.new(@catalog)

      {:ok, table} =
        subscribe(table, "e", %Plugin{
          id: "e",
          provider: FoldErr,
          opts: %{},
          hooks: [{:on_operator_init, [on_error: :empty]}]
        })

      {:ok, table} =
        subscribe(table, "b", %Plugin{
          id: "b",
          provider: FoldB,
          opts: %{},
          hooks: [{:on_operator_init, [position: 1]}]
        })

      ctx = %Ctx.Init{operator_id: "p", namespace: :n, factory_id: "f", accumulated: %{}}

      assert {:ok, %Ctx.Init{accumulated: %{b: 2}}} =
               Extension.run(table, :on_operator_init, ctx)
    end
  end

  describe ":transform dispatch" do
    test "empty returns ctx unchanged" do
      table = Table.new(@catalog)
      ctx = %Ctx.IO{operator_id: "p", namespace: :n, message: "x", metadata: %{}}

      assert {:ok, ^ctx} = Extension.run(table, :on_input, ctx)
    end

    test "subscriber transforms payload" do
      table = Table.new(@catalog)

      {:ok, table} =
        subscribe(table, "u", %Plugin{
          id: "u",
          provider: TransformUpcase,
          opts: %{},
          hooks: [{:on_input, []}]
        })

      ctx = %Ctx.IO{operator_id: "p", namespace: :n, message: "hi", metadata: %{}}
      assert {:ok, %Ctx.IO{message: "HI"}} = Extension.run(table, :on_input, ctx)
    end

    test ":skip halts chain" do
      table = Table.new(@catalog)

      {:ok, table} =
        subscribe(table, "s", %Plugin{
          id: "s",
          provider: TransformSkip,
          opts: %{},
          hooks: [{:on_input, [position: 0]}]
        })

      ctx = %Ctx.IO{operator_id: "p", namespace: :n, message: "x", metadata: %{}}
      assert {:skip, :rate_limited} = Extension.run(table, :on_input, ctx)
    end

    test ":error is logged and chain continues with prior ctx" do
      table = Table.new(@catalog)

      {:ok, table} =
        subscribe(table, "u", %Plugin{
          id: "u",
          provider: TransformUpcase,
          opts: %{},
          hooks: [{:on_input, [position: 0]}]
        })

      {:ok, table} =
        subscribe(table, "err", %Plugin{
          id: "err",
          provider: TransformError,
          opts: %{},
          hooks: [{:on_input, [position: 1]}]
        })

      ctx = %Ctx.IO{operator_id: "p", namespace: :n, message: "hi", metadata: %{}}

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
      ctx = %Ctx.Snapshot{operator_id: "p", namespace: :n, snapshot: nil}

      assert :ok = Extension.run(table, :on_snapshot, ctx)
    end

    test "every subscriber runs; exceptions isolated" do
      table = Table.new(@catalog)

      {:ok, table} =
        subscribe(table, "r", %Plugin{
          id: "r",
          provider: FanoutRaise,
          opts: %{pid: self()},
          hooks: [{:on_snapshot, []}]
        })

      {:ok, table} =
        subscribe(table, "n", %Plugin{
          id: "n",
          provider: FanoutNotify,
          opts: %{pid: self()},
          hooks: [{:on_snapshot, []}]
        })

      ctx = %Ctx.Snapshot{operator_id: "p", namespace: :n, snapshot: nil}

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
        operator_id: "p",
        namespace: "n"
      }

      assert :ok = Extension.run(table, :on_message, ctx)
      assert_receive {:flezha_response, ^ref, {:error, {:unknown_key, :on_message, "nope"}}}, 200
    end

    test "keyed dispatch invokes subscriber" do
      table = Table.new(@catalog)

      {:ok, table} =
        subscribe(table, "e", %Plugin{
          id: "e",
          provider: MsgEcho,
          opts: %{},
          hooks: [{:on_message, [key: "echo"]}]
        })

      ref = make_ref()

      ctx = %Ctx.Msg{
        key: "echo",
        payload: "hi",
        reply_to: self(),
        ref: ref,
        operator_id: "p",
        namespace: "n"
      }

      assert :ok = Extension.run(table, :on_message, ctx)
      assert_receive {:flezha_response, ^ref, {:ok, "echo:hi"}}, 500
    end
  end
end
