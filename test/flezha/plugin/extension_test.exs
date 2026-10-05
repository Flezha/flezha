defmodule FlezhaTest.Plugin.ExtensionTest do
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host

  use ExUnit.Case, async: true

  # Most cases here drive a prepare refusal on purpose, and the table reports
  # every halt at error level. The refusal each case asserts is the return
  # value; capture the report so a green run stays quiet.
  @moduletag :capture_log

  import ExUnit.CaptureLog, only: [with_log: 1]

  @catalog Host.build_catalog(
             [
               {:on_relay_init, :fold, Ctx.Init},
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
    def on_relay_init(%Ctx.Init{} = ctx, _opts) do
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
    def on_relay_init(%Ctx.Init{} = ctx, _opts) do
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
    def on_relay_init(_ctx, _opts), do: {:error, :boom}
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
      send(Map.fetch!(opts, :pid), {:tick, ctx.relay_id})
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
      send(Map.fetch!(opts, :pid), {:fanout_called, ctx.relay_id})
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

  describe "subscription canonicalisation" do
    test "every sub-option coerces from the text a user types" do
      assert {:ok, %Plugin{hooks: subs}} =
               Table.canonicalise(@catalog, %Plugin{
                 id: "c",
                 provider: MsgEcho,
                 hooks: [
                   {"on_interval", %{"interval_ms" => "5000"}},
                   {"on_relay_init",
                    %{"position" => "1", "timeout_ms" => "1000", "on_error" => "empty"}},
                   {"on_input", %{"position" => "0"}},
                   {"on_message", %{"key" => "orders"}}
                 ]
               })

      assert subs == [
               {:on_interval, [interval_ms: 5_000]},
               {:on_relay_init, [position: 1, timeout_ms: 1_000, on_error: "empty"]},
               {:on_input, [position: 0]},
               {:on_message, [key: "orders"]}
             ]
    end

    test "canonical input is returned unchanged" do
      canonical = [
        {:on_interval, [interval_ms: 5_000]},
        {:on_relay_init, [position: 1, timeout_ms: 1_000, on_error: "fail"]},
        {:on_message, [key: "orders"]},
        {:on_snapshot, []}
      ]

      plugin = %Plugin{id: "c", provider: MsgEcho, hooks: canonical}

      assert {:ok, ^plugin} = Table.canonicalise(@catalog, plugin)
    end

    test "a value that cannot be coerced is refused naming the sub-option" do
      for {name, opts, expected} <- [
            {"on_interval", %{"interval_ms" => "soon"}, {:interval_ms, "soon"}},
            {"on_relay_init", %{"timeout_ms" => :bad}, {:timeout_ms, :bad}},
            {"on_relay_init", %{"on_error" => "shrug"}, {:on_error, "shrug"}},
            {"on_message", %{"key" => 7}, {:key, 7}}
          ] do
        {option, value} = expected

        assert {:error, {:invalid_hook_opts, ^option, ^value}} =
                 Table.canonicalise(@catalog, %Plugin{
                   id: "c",
                   provider: MsgEcho,
                   hooks: [{name, opts}]
                 })
      end
    end

    test "a sub-option the kind does not declare is refused" do
      assert {:error, {:unknown_hook_opt, :timer, "interval"}} =
               Table.canonicalise(@catalog, %Plugin{
                 id: "c",
                 provider: MsgEcho,
                 hooks: [{"on_interval", %{"interval" => "5000"}}]
               })
    end

    test "an unknown extension name is refused without being interned" do
      name = "on_never_declared_#{System.unique_integer([:positive])}"

      assert {:error, {:unknown_hook, ^name, _allowed}} =
               Table.canonicalise(@catalog, %Plugin{
                 id: "c",
                 provider: MsgEcho,
                 hooks: [{name, %{}}]
               })

      assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
    end

    test "position: \"0\" is an integer on both ordered kinds, and a bad one never raises" do
      table = Table.new(@catalog)

      {:ok, table} =
        subscribe(table, "f", %Plugin{
          id: "f",
          provider: FoldA,
          hooks: [{"on_relay_init", %{"position" => "0"}}]
        })

      {:ok, table} =
        subscribe(table, "t", %Plugin{
          id: "t",
          provider: TransformUpcase,
          hooks: [{"on_input", %{"position" => "0"}}]
        })

      assert [%{position: 0}] = Map.fetch!(table.subs, :on_relay_init)
      assert [%{position: 0}] = Map.fetch!(table.subs, :on_input)

      for {provider, name} <- [{FoldA, "on_relay_init"}, {TransformUpcase, "on_input"}] do
        assert {:error, {_id, {:invalid_hook_opts, :position, "first"}}} =
                 subscribe(table, "bad", %Plugin{
                   id: "bad",
                   provider: provider,
                   hooks: [{name, %{"position" => "first"}}]
                 })

        assert {:error, {_id, {:invalid_hook_opts, :position, -1}}} =
                 subscribe(table, "bad", %Plugin{
                   id: "bad",
                   provider: provider,
                   hooks: [{name, %{"position" => "-1"}}]
                 })
      end
    end

    test "on_error declared as text drives the empty-on-error fold behaviour" do
      table = Table.new(@catalog)

      {:ok, table} =
        subscribe(table, "e", %Plugin{
          id: "e",
          provider: FoldErr,
          hooks: [{"on_relay_init", %{"on_error" => "empty"}}]
        })

      ctx = %Ctx.Init{relay_id: "p", namespace: :n, factory_id: "f", accumulated: %{}}

      assert {:ok, %Ctx.Init{}} = Extension.run(table, :on_relay_init, ctx)
    end
  end

  describe "validation" do
    test "unknown extension rejected at subscribe" do
      table = Table.new(@catalog)

      assert {:error, {_id, {:unknown_hook, :nope, _allowed}}} =
               subscribe(table, "x", %Plugin{
                 id: "x",
                 provider: FoldA,
                 hooks: [{:nope, []}]
               })
    end

    test "missing callback rejected" do
      table = Table.new(@catalog)

      assert {:error, {_id, {:missing_callback, :on_input}}} =
               subscribe(table, "x", %Plugin{
                 id: "x",
                 provider: FoldA,
                 hooks: [{:on_input, []}]
               })
    end

    test "position on every unordered kind rejected" do
      table = Table.new(@catalog)

      for {name, provider, opts} <- [
            {:on_snapshot, FanoutNotify, [position: 0]},
            {:on_message, MsgEcho, [key: "k", position: 0]},
            {:on_interval, TimerPing, [interval_ms: 10, position: 0]}
          ] do
        assert {:error, {_id, :position_not_applicable}} =
                 subscribe(table, "x", %Plugin{
                   id: "x",
                   provider: provider,
                   opts: %{pid: self()},
                   hooks: [{name, opts}]
                 })
      end
    end

    test "key conflict on :request rejected" do
      table = Table.new(@catalog)

      {:ok, table} =
        subscribe(table, "a", %Plugin{
          id: "a",
          provider: MsgEcho,
          hooks: [{:on_message, [key: "k"]}]
        })

      assert {:error, {_id, {:key_conflict, :on_message, "k"}}} =
               subscribe(table, "b", %Plugin{
                 id: "b",
                 provider: MsgEcho,
                 hooks: [{:on_message, [key: "k"]}]
               })
    end

    test "invalid timeout rejected on :fold" do
      table = Table.new(@catalog)

      assert {:error, {_id, {:invalid_hook_opts, :timeout_ms, :bad}}} =
               subscribe(table, "x", %Plugin{
                 id: "x",
                 provider: FoldA,
                 hooks: [{:on_relay_init, [timeout_ms: :bad]}]
               })
    end

    test "invalid on_error rejected on :fold" do
      table = Table.new(@catalog)

      assert {:error, {_id, {:invalid_hook_opts, :on_error, :weird}}} =
               subscribe(table, "x", %Plugin{
                 id: "x",
                 provider: FoldA,
                 hooks: [{:on_relay_init, [on_error: :weird]}]
               })
    end
  end

  defmodule FoldErrInit do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(_opts), do: {:error, :declined}
    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule RaisingInit do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(_opts), do: raise(ArgumentError, "boom from init_provider")
    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule ThrowingInit do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(_opts), do: throw("bare throw from init_provider")
    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule ExitingInit do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(_opts), do: exit(:collaborator_gone)
    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule CrashingOnReprepare do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(%{reprepare: :raise}), do: raise(ArgumentError, "boom on re-prepare")
    @impl true
    def init_provider(%{reprepare: :throw}), do: throw("bare throw on re-prepare")
    @impl true
    def init_provider(%{reprepare: :exit}), do: exit(:collaborator_gone)
    @impl true
    def init_provider(%{mode: mode} = opts), do: {:ok, Map.put(opts, :reprepare, mode)}
    @impl true
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
          hooks: [{:on_input, []}]
        }

        {result, log} =
          with_log(fn -> subscribe(Table.new(@catalog), "crashing", plugin) end)

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
          hooks: [{:on_input, []}]
        }

        {:ok, table} = subscribe(Table.new(@catalog), "heal", plugin)

        {{:error, _reason, table}, log} =
          with_log(fn -> Table.attempt_restart(table, "heal", plugin) end)

        assert %Table{} = table
        assert log =~ "init_provider"
        assert log =~ "CrashingOnReprepare"
        assert Table.workers(table, "heal") == %{}
      end
    end

    test "a provider whose init_provider/1 returns an error is unchanged" do
      plugin = %Plugin{
        id: "declined",
        provider: FoldErrInit,
        opts: %{},
        hooks: [{:on_input, []}]
      }

      assert {:error, {_id, :declined}} =
               subscribe(Table.new(@catalog), "declined", plugin)
    end
  end

  describe "the table drops what it is told to and issues no effect of its own" do
    test "teardown_all empties the table" do
      emptied = Table.teardown_all(table_with(["a", "b", "c"]))

      assert emptied.plugins == %{}
      assert emptied.entries == %{}
    end

    test "unsubscribe drops the one plugin named" do
      assert Map.keys(Table.unsubscribe(table_with(["a", "b"]), "b").plugins) == ["a"]
    end

    test "unsubscribe of a plugin the table never held changes nothing" do
      table = table_with(["a"])
      assert Table.unsubscribe(table, "ghost") == table
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
        subscribe(table, plugin_id, %Plugin{
          id: plugin_id,
          provider: FoldA,
          hooks: [{:on_relay_init, []}]
        })

      table
    end)
  end

  defmodule MaskProvider do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        mode: [type: :any, internal: true],
        pid: [type: :any, internal: true],
        reprepare: [type: :any, internal: true]
      ]

    @impl true
    def on_relay_init(%Ctx.Init{} = ctx, _opts), do: {:ok, ctx}
    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
    @impl true
    def on_snapshot(%Ctx.Snapshot{} = ctx, _opts), do: {:ok, ctx}
    @impl true
    def on_message(_ctx, _opts), do: {:ok, %{}}
    @impl true
    def on_interval(%Ctx.Tick{} = ctx, _opts), do: {:ok, ctx}
  end

  describe "active_mask" do
    test "bit toggles on first and last subscriber" do
      table = Table.new(@catalog)
      assert table.active_mask == 0

      {:ok, table} =
        subscribe(table, "a", %Plugin{
          id: "a",
          provider: FoldA,
          hooks: [{:on_relay_init, []}]
        })

      assert table.active_mask != 0

      table = Table.unsubscribe(table, "a")
      assert table.active_mask == 0
    end

    # `add_subscription/6` and `remove_subscription/3` mutate only the
    # subscriber lists; `refresh_chain/2` sets the bit. `Extension.run/3`
    # short-circuits on it, so a wrong bit silently skips every subscriber.
    for {name, opts, other_opts} <- [
          {:on_relay_init, [], []},
          {:on_input, [position: 0], [position: 0]},
          {:on_snapshot, [], []},
          {:on_message, [key: "k1"], [key: "k2"]},
          {:on_interval, [interval_ms: 60_000], [interval_ms: 60_000]}
        ] do
      test "#{name} sets its bit on the first subscriber and clears it on the last" do
        plugin = fn id, o ->
          %Plugin{id: id, provider: MaskProvider, hooks: [{unquote(name), o}]}
        end

        empty = Table.new(@catalog)
        {:ok, one} = subscribe(empty, "a", plugin.("a", unquote(opts)))
        {:ok, two} = subscribe(one, "b", plugin.("b", unquote(other_opts)))
        minus_one = Table.unsubscribe(two, "a")
        none = Table.unsubscribe(minus_one, "b")

        assert empty.active_mask == 0
        assert one.active_mask != 0
        assert two.active_mask == one.active_mask
        assert minus_one.active_mask == one.active_mask
        assert none.active_mask == 0
      end
    end
  end
end
