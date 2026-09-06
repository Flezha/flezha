defmodule Flezha.Plugin.ProviderBehaviourTest do
  @moduledoc """
  The generated `build/1` contract: a value the provider's `convert_value/2`
  refuses is answered as `{:error, String.t()}`, which is what the
  `@callback` declares. Driven through real providers, because the defect
  this pins is invisible to a provider that declares no `@enforce_keys`.
  """

  use ExUnit.Case, async: true

  # A refused declaration is driven on purpose and the table reports the halt
  # at error level; the refusal is asserted as a return value.
  @moduletag :capture_log

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.BeamMsgForward
  alias Flezha.Plugin.Provider.Filter
  alias Flezha.Plugin.Provider.HttpPoll
  alias Flezha.Plugin.Provider.Kafka
  alias Flezha.Plugin.Provider.OperativeForward
  alias Flezha.Plugin.Provider.Postgres
  alias Flezha.Plugin.Provider.StdOut
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions

  @catalog Host.build_catalog(
             [{:on_input, :transform, Ctx.IO}],
             %{file: __ENV__.file, line: __ENV__.line}
           )

  describe "build/1 refuses an unconvertible value instead of raising" do
    test "a bad integer option on a provider with required fields is a refusal" do
      assert {:error, message} =
               Kafka.Producer.build(%{"hosts" => "h:1", "topic" => "t", "linger_ms" => "zz"})

      assert message =~ "linger_ms"
      assert message =~ "zz"
    end

    test "the refusal names the field once, not twice" do
      {:error, message} =
        Kafka.Producer.build(%{"hosts" => "h:1", "topic" => "t", "linger_ms" => "zz"})

      assert message == "Invalid value for linger_ms: zz"
    end

    test "the same holds for a boolean option" do
      assert {:error, message} = Filter.build(%{"regex_pattern" => "x", "inverted" => "maybe"})
      assert message =~ "inverted"
    end

    test "the same holds for every provider that can refuse a value" do
      refusals = [
        {"postgres", Postgres.build(%{"database" => "d", "username" => "u", "port" => "zz"})},
        {"http_poll", HttpPoll.build(%{"url" => "u", "channel" => "c", "interval_ms" => "zz"})}
      ]

      for {who, result} <- refusals do
        assert {:error, message} = result, "#{who} did not refuse"
        assert is_binary(message), "#{who} answered a non-binary refusal"
      end
    end

    test "a malformed JSON option is refused as a string, not as a decode struct" do
      for field <- ["ws_opts", "payload_metadata", "subscription_headers", "post_connect"] do
        assert {:error, message} = Websocket.build(%{"uri" => "wss://x", field => "{not json"})

        assert is_binary(message),
               "#{field} answered a non-binary refusal: #{inspect(message)}"

        assert message =~ field
        assert message =~ "{not json"
      end
    end

    test "a provider that declares no required fields is unchanged" do
      assert {:ok, %StdOut{}} = StdOut.build(%{})
    end
  end

  describe "the @known_opts contract survives the delegating build/1" do
    # `__before_compile__` emits `defoverridable build: 1` and an override that
    # calls `super/1`; the base is now a one-line delegation, and `super` must
    # still reach it.
    for {provider, name, required} <- [
          {HttpPoll, "http_poll", %{"url" => "u", "channel" => "c"}},
          {OperativeForward, "operative_forward", %{"target_operative_id" => "b"}},
          {Websocket, "websocket", %{"uri" => "wss://example.com"}}
        ] do
      test "#{name} refuses an unknown option through build/1" do
        assert {:error, message} =
                 unquote(provider).build(Map.put(unquote(Macro.escape(required)), "nope", "1"))

        assert message =~ "unknown option(s) for #{unquote(name)}: nope"
      end

      test "#{name} refuses an unknown option through validate_opts/1" do
        assert {:error, message} = unquote(provider).validate_opts(%{nope: 1})
        assert message =~ "unknown option(s) for #{unquote(name)}: nope"
      end

      test "#{name} still builds when every option is known" do
        assert {:ok, _} = unquote(provider).build(unquote(Macro.escape(required)))
      end
    end
  end

  describe "one conversion table, on both paths" do
    # KI-62 was filed against the older shape, where `build/1` was the only
    # caller of the conversion table and a `%Flezha.Plugin{}` carrying typed
    # opts crossed it uncoerced, so an operator's `--plugins` spec was checked
    # by nothing. The declaration is now read on both paths, and the same value
    # earns the same sentence either way.
    test "a value the declaration refuses is refused on the spec path too" do
      opts = %{regex_pattern: "x", inverted: "not-a-bool"}

      plugin = %Plugin{id: "ki62", provider: Filter, opts: opts, subscriptions: [{:on_input, []}]}

      assert {:error, {"ki62", spec_refusal}} =
               Table.bulk_subscribe(Table.new(@catalog), [{"ki62", plugin}])

      assert {:error, cli_refusal} =
               Filter.build(%{"regex_pattern" => "x", "inverted" => "not-a-bool"})

      assert spec_refusal == cli_refusal
      assert spec_refusal == "Invalid value for inverted: not-a-bool"
    end

    test "a value it accepts reaches the provider converted, with every default present" do
      plugin = %Plugin{
        id: "ok",
        provider: Filter,
        opts: %{regex_pattern: "x", inverted: "true"},
        subscriptions: [{:on_input, []}]
      }

      {:ok, table} = Table.bulk_subscribe(Table.new(@catalog), [{"ok", plugin}])

      assert table.plugins["ok"].opts.inverted == true
      assert table.plugins["ok"].opts.regex_pattern == "x"
    end
  end

  defmodule DeclaredConverted do
    @moduledoc false

    use Flezha.Plugin.ProviderBehaviour,
      options: [
        name: [type: :string, required: true, doc: "The thing's name"],
        mode: [type: :string, doc: "A mode this provider converts by hand"],
        untyped: [type: :any, doc: "A value nothing interprets"]
      ]

    # Names `:mode`'s string form. Every other shape falls through to the row,
    # which declares the type that refuses it.
    defp convert_value(:mode, value) when is_binary(value), do: {:ok, {:mode, value}}
  end

  describe "the row refuses a value the provider's own clauses do not match" do
    test "an unmatched value for a typed option is refused" do
      assert {:error, message} = DeclaredConverted.build(%{"name" => "n", "mode" => 42})
      assert message =~ "mode"
      assert message =~ "42"
    end

    test "a matched value for the same option still converts by hand" do
      assert {:ok, %DeclaredConverted{mode: {:mode, "fast"}}} =
               DeclaredConverted.build(%{"name" => "n", "mode" => "fast"})
    end

    test "an option declared `:any` passes through unchanged" do
      assert {:ok, %DeclaredConverted{untyped: %{"a" => 1}}} =
               DeclaredConverted.build(%{"name" => "n", "untyped" => %{"a" => 1}})
    end

    test "a provider that declares an unknown option type does not compile" do
      source = """
      defmodule Flezha.ProviderBehaviourTest.BadType#{System.unique_integer([:positive])} do
        use Flezha.Plugin.ProviderBehaviour, options: [mode: [type: :colour]]
      end
      """

      assert_raise CompileError, fn -> Code.compile_string(source) end
    end
  end

  describe "the shared conversion helpers are total" do
    test "parse_int/2 refuses a shape it cannot parse instead of raising" do
      for value <- [nil, :nope, %{}, [1]] do
        assert {:error, message} = ProviderBehaviour.parse_int(value, :thing),
               "expected #{inspect(value)} to be refused"

        assert message =~ "thing"
      end
    end

    test "parse_int/2 still accepts an integer and a string naming one" do
      assert {:ok, 5} = ProviderBehaviour.parse_int(5, :thing)
      assert {:ok, 5} = ProviderBehaviour.parse_int("5", :thing)
      assert {:error, "Invalid value for thing: zz"} = ProviderBehaviour.parse_int("zz", :thing)
    end

    test "parse_bool/2 is unchanged" do
      assert {:ok, true} = ProviderBehaviour.parse_bool("true", :thing)
      assert {:ok, false} = ProviderBehaviour.parse_bool(false, :thing)
      assert {:error, _} = ProviderBehaviour.parse_bool("maybe", :thing)
    end
  end

  describe "the strict option set is enforced on the init_provider/1 path too" do
    test "an option the provider does not offer is refused before init_provider/1" do
      assert {:error, message} =
               prepare(OperativeForward, %{
                 target_operative_id: "b",
                 source_operative_id: "a",
                 source_namespace: :default,
                 nope: 1
               })

      assert message =~ "nope"
    end

    # Not redundant with the refusal above, and deleting it re-opens a wedge:
    # `:worker_pid` and `:routes` are resolved by the provider and carried back
    # by every re-prepare, and neither is a `host_opts/0` key — so a strict
    # `validate_opts/1` that does not allow them refuses every plugin HEAL,
    # not just a bad config. The refusal this asserts is the target check one
    # step later, which is only reached because the option check passed.
    test "a re-prepare's own resolved keys are accepted, or every plugin heal is refused" do
      assert {:error, message} =
               prepare(OperativeForward, %{
                 target_operative_id: "a",
                 source_operative_id: "a",
                 source_namespace: :default,
                 plugin_id: "a->a",
                 routes: []
               })

      assert message =~ "is the operative itself"
    end
  end

  describe "a partition member's injected opts reach every strict provider" do
    # `Plugin.for_partition/3` merges `Partition.provider_opts/1` into EVERY
    # plugin's opts of a group member, so a strict allow-list that omits them
    # refuses every enable on a partitioned operative rather than one bad
    # config. This is the failure `host_opts/0`'s own doc names — "a key added
    # to the host side and missed in one provider's private copy turns that
    # provider's every enable into a refusal" — for a second set of keys, and
    # the reason they belong in that one list rather than in per-provider
    # copies.
    #
    # `mix test` alone cannot see the consequence: the only end-to-end coverage
    # is `partitioned_operatives_e2e_test.exs`, which is `@moduletag :system`
    # and excluded by default. These two run in the default suite.
    @partition_opts Flezha.Partition.provider_opts(%{index: 1, count: 3, range: {85, 169}})

    test "every strict provider accepts them, because every one of them receives them" do
      for {id, module} <- Flezha.Plugin.Providers.all(), Code.ensure_loaded?(module) do
        assert :ok == module.validate_opts(@partition_opts),
               "provider `#{id}` refuses a partition member's own injected opts, so every " <>
                 "plugin on a partitioned operative fails to enable"
      end
    end

    test "operative_forward gets past the option check on the path a group start uses" do
      opts =
        Map.merge(@partition_opts, %{
          target_operative_id: "a",
          source_operative_id: "a",
          source_namespace: :default
        })

      assert {:error, message} = prepare(OperativeForward, opts)

      # The target check, one step AFTER the option check, reached only because
      # the injected keys were accepted.
      assert message =~ "is the operative itself"
      refute message =~ "unknown option"
    end
  end

  describe "the postgres sink refuses at preparation, before it holds anything" do
    # Preparation starts nothing for any provider now, so none of these needs a
    # database. The refusals are the declaration's, made before `init_provider/1`
    # is reached at all.
    defp pg_opts(extra) do
      plugin_id = "pg-refusal-#{System.unique_integer([:positive])}"
      Map.merge(%{database: "d", username: "u", plugin_id: plugin_id}, extra)
    end

    defp prepare(provider, opts) do
      with {:ok, opts} <- ProviderOptions.normalise_opts(provider, opts),
           do: provider.init_provider(opts)
    end

    test "a health threshold it cannot interpret is refused" do
      assert {:error, message} = prepare(Postgres, pg_opts(%{unhealthy_after_ms: "soon"}))
      assert is_binary(message)
      assert message =~ "unhealthy_after_ms"
    end

    test "a boolean it cannot interpret is refused rather than raising" do
      assert {:error, message} = prepare(Postgres, pg_opts(%{ssl: "yes"}))
      assert is_binary(message)
      assert message =~ "ssl"
    end

    test "an option the sink does not offer is refused" do
      assert {:error, message} = prepare(Postgres, pg_opts(%{nope: 1}))
      assert is_binary(message)
      assert message =~ "nope"
    end

    test "the keys the host injects are still accepted, and nothing is started" do
      opts =
        pg_opts(%{
          source_namespace: "ns",
          source_operative_id: "op",
          source_plugin_id: "p",
          effectively_once: false
        })

      assert {:ok, prepared} = prepare(Postgres, opts)
      assert prepared.plugin_id == opts.plugin_id
    end
  end

  describe "a guarded clause that does not match is a refusal, not a raise" do
    # [[KI-93]] measured 44 raising fields across 7 providers and recorded
    # `BeamMsgForward` clean — its scan drove OPTIONAL fields, and `:recv_pid`
    # is `@enforce_keys`, so this case sat outside the measurement.
    test "a pid string that names no process is refused" do
      assert {:error, message} = BeamMsgForward.build(%{"recv_pid" => "zzz"})
      assert is_binary(message)
      assert message =~ "recv_pid" or message =~ "zzz"
    end

    test "a boolean option refuses a non-string it cannot interpret" do
      assert {:error, message} = Filter.build(%{"regex_pattern" => "x", "inverted" => %{}})
      assert message =~ "inverted"
    end

    test "a named process still resolves" do
      name = :"pb_test_#{System.unique_integer([:positive])}"
      Process.register(self(), name)

      assert {:ok, %BeamMsgForward{recv_pid: pid}} =
               BeamMsgForward.build(%{"recv_pid" => Atom.to_string(name)})

      assert pid == self()
    end
  end

  describe "the refusals build/1 already made are untouched" do
    test "a missing required field still names the field, through the one builder" do
      assert {:error, "kafka requires :hosts, :topic"} =
               Kafka.Producer.build(%{"linger_ms" => "10"})
    end

    test "an unknown option is still refused ahead of any conversion" do
      assert {:error, message} = HttpPoll.build(%{"url" => "u", "channel" => "c", "nope" => "1"})
      assert message =~ "unknown option(s) for http_poll: nope"
    end

    test "a convertible map still builds" do
      assert {:ok, %Kafka.Producer{linger_ms: 25}} =
               Kafka.Producer.build(%{"hosts" => "h:1", "topic" => "t", "linger_ms" => "25"})
    end
  end

  # One vocabulary, one root. A provider that invents a second spelling for a
  # fact two others already report is unreachable by anything subscribing to
  # the family, and the divergence is invisible until someone greps for it —
  # which is what the retired `[:flezha, :ws, …]` root was.
  describe "the provider event vocabulary" do
    test "the prefix is derived from the provider id, and the verb is appended" do
      assert ProviderBehaviour.event(:kafka, :shipped) == [:flezha, :plugin, :kafka, :shipped]

      # A provider id is carried as a string by the declaration, and names the
      # same event either way.
      assert ProviderBehaviour.event("kafka", :shipped) ==
               ProviderBehaviour.event(:kafka, :shipped)
    end

    test "the four shared verbs are declared once" do
      assert Enum.sort(ProviderBehaviour.verbs()) ==
               Enum.sort([:shipped, :retried, :failed, :overflowed])
    end

    test "a provider-specific fact keeps its own verb under the same prefix" do
      assert ProviderBehaviour.event(:kafka, :topic_created) ==
               [:flezha, :plugin, :kafka, :topic_created]
    end

    test "no provider builds an event name of its own" do
      offenders =
        for file <- Path.wildcard("lib/flezha/plugin/provider/**/*.ex"),
            body = File.read!(file),
            body =~
              ~r/:telemetry\.execute\(\s*\[:flezha, :plugin|@prefix \[:flezha|@telemetry \[:flezha|\[:flezha, :ws,/,
            do: file

      assert offenders == [],
             "providers naming their own telemetry events: #{inspect(offenders)} — " <>
               "every provider event goes through ProviderBehaviour.emit/3"
    end

    test "every emitted verb reaches the bridge or is a declared provider-specific one" do
      emitted =
        for file <- Path.wildcard("lib/flezha/plugin/provider/**/*.ex"),
            [_, verb] <- Regex.scan(~r/emit\(\s*:([a-z_]+),/, File.read!(file)),
            do: String.to_atom(verb)

      refute emitted == [], "no provider emit was found, so this test asserts nothing"

      mapped =
        Flezha.Observability.TelemetryBridge.mapping()
        |> Map.keys()
        |> Enum.filter(&match?([:flezha, :plugin | _], &1))
        |> MapSet.new(&List.last/1)

      unbridged = emitted |> Enum.uniq() |> Enum.reject(&MapSet.member?(mapped, &1))

      assert unbridged == [],
             "provider verbs that reach no sink: #{inspect(unbridged)}"
    end
  end
end
