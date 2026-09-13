defmodule Flezha.Plugin.ProviderBehaviourTest do
  @moduledoc """
  The generated `build/1` contract: a value the provider's `convert_value/2`
  refuses is answered as `{:error, String.t()}`, which is what the
  `@callback` declares. Driven through real providers, because the defect
  this pins is invisible to a provider that declares no `@enforce_keys`.
  """

  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

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
  alias Flezha.Service.Cli

  @names_rows [channel_out: [type: :names]]

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

  defmodule NoFeedSource do
    @moduledoc false

    # A provider offering the entry-side spelling and never declaring that it
    # feeds the operative — the shape the `channel_in` refusal exists for, and
    # one no first-party provider can produce, because both that declare the row
    # also feed.
    use Flezha.Plugin.ProviderBehaviour,
      options: [channel_in: [type: :names, doc: "Entry channel"]]
  end

  describe "a provider declares whether it feeds the operative" do
    test "the providers whose worker injects declare it, and no other does" do
      feeding =
        for {id, module} <- Flezha.Plugin.Providers.all(),
            module.__provider__().feeds,
            do: id

      assert Enum.sort(feeding) == [
               "operative_file_source",
               "operative_http_poll",
               "operative_websocket"
             ]
    end

    test "it cannot be inferred from the callback list" do
      # The Kafka producer subscribes to `on_input` and feeds nothing: it
      # publishes what the operative ingested.
      assert :on_input in Kafka.Producer.__provider__().extensions
      refute Kafka.Producer.__provider__().feeds
    end

    test "the jobs a plugin's channel can name are the declaration and the subscriptions" do
      chain = %{subscribed_extensions: [:on_output]}

      assert ProviderBehaviour.channel_jobs(Websocket.__provider__(), chain) == [:in, :out]
      assert ProviderBehaviour.channel_jobs(Websocket.__provider__(), %{}) == [:in]
      assert ProviderBehaviour.channel_jobs(Kafka.Producer.__provider__(), chain) == [:out]
      assert ProviderBehaviour.channel_jobs(Kafka.Producer.__provider__(), %{}) == []
    end
  end

  describe "one channel option per direction" do
    defp normalised(provider, opts), do: ProviderOptions.normalise_opts(provider, opts)

    defp poll_opts(extra), do: Map.merge(%{url: "https://example.invalid/c"}, extra)

    defp kafka_opts(extra), do: Map.merge(%{hosts: [{"h", 1}], topic: "t"}, extra)

    test "`channel` fills the half the plugin has, and only that half" do
      assert {:ok, %{entry_channel: "cfg", filter_channel: nil}} =
               normalised(HttpPoll, poll_opts(%{channel: "cfg"}))

      assert {:ok, %{entry_channel: nil, filter_channel: ["out"]}} =
               normalised(
                 Kafka.Producer,
                 kafka_opts(%{channel: "out", subscribed_extensions: [:on_input]})
               )
    end

    test "either spelling satisfies a provider that requires an entry channel" do
      assert {:ok, %{entry_channel: "cfg"}} = prepare(HttpPoll, poll_opts(%{channel_in: "cfg"}))
      assert {:ok, %{entry_channel: "cfg"}} = prepare(HttpPoll, poll_opts(%{channel: "cfg"}))
    end

    test "naming neither is refused, and the refusal names both spellings" do
      assert {:error, message} = prepare(HttpPoll, poll_opts(%{}))
      assert message == "http_poll requires an entry channel: write channel or channel_in"
    end

    # A row can declare itself required and nothing can declare a PAIR required,
    # so both operator surfaces would otherwise read "optional" for an option the
    # enable refuses to do without. Both render the row's own `doc:`, which is
    # where the rule is written.
    test "both operator surfaces say the entry channel is required" do
      for text <- [
            HttpPoll.help("operative_http_poll"),
            ProviderOptions.render_options_table(HttpPoll)
          ] do
        assert text =~ "Required, unless `channel_in` names it"
        assert text =~ "Required, unless `channel` names it"
      end
    end

    test "a spelling for a direction the provider does not work in is not a declared row" do
      assert {:error, message} = normalised(HttpPoll, poll_opts(%{channel_out: "x"}))
      assert message == "unknown option(s) for http_poll: channel_out"

      assert {:error, message} = normalised(Kafka.Producer, kafka_opts(%{channel_in: "x"}))

      assert message == "unknown option(s) for kafka: channel_in"
    end

    test "a spelling for a job the plugin was not wired for is refused" do
      assert {:error, message} = normalised(NoFeedSource, %{channel_in: "us"})
      assert message =~ "no_feed_source names channel_in"
      assert message =~ "does not feed the operative"
    end

    test "`channel` on a plugin doing neither job is refused" do
      assert {:error, message} = normalised(Kafka.Producer, kafka_opts(%{channel: "x"}))

      assert message =~ "kafka names channel"
      assert message =~ "nothing would read it"
    end

    test "an empty name is refused for every spelling" do
      assert {:error, "Invalid value for channel: "} =
               normalised(HttpPoll, poll_opts(%{channel: ""}))

      assert {:error, "Invalid value for channel_in: "} =
               normalised(HttpPoll, poll_opts(%{channel_in: ""}))

      # The row's type refuses it, so the struct builder answers the same
      # sentence — which the resolution alone never reached.
      assert {:error, "Invalid value for channel: "} =
               HttpPoll.build(%{"url" => "https://example.invalid/c", "channel" => ""})
    end

    test "a channel written as an atom still names the same channel" do
      assert {:ok, %{entry_channel: "cfg"}} = normalised(HttpPoll, poll_opts(%{channel: :cfg}))
    end

    # A plugin is re-prepared on every restart and handover from the options it
    # already holds (`Extension.Table.attempt_restart/3`), so a resolution that
    # rewrote the rows it reads would refuse its own output the second time.
    test "resolving an already-resolved plugin answers the same thing" do
      opts = poll_opts(%{channel_in: "cfg"})

      assert {:ok, once} = normalised(HttpPoll, opts)
      assert {:ok, twice} = normalised(HttpPoll, once)

      assert twice.entry_channel == "cfg"

      assert Map.take(twice, [:channel, :channel_in, :entry_channel]) ==
               Map.take(once, [:channel, :channel_in, :entry_channel])
    end
  end

  describe "a channel filter names a set" do
    defp names_row(value), do: ProviderOptions.convert_row(:channel_out, value, @names_rows)

    defp kafka_out(channel),
      do:
        normalised(
          Kafka.Producer,
          kafka_opts(Map.put(channel, :subscribed_extensions, [:on_input]))
        )

    test "one name is a set of one, and several are the set they spell" do
      assert names_row("alerts") == {:ok, ["alerts"]}
      assert names_row("alerts,audit") == {:ok, ["alerts", "audit"]}
      assert names_row(" alerts , audit ") == {:ok, ["alerts", "audit"]}
    end

    # The shape a manifest carries, and the shape a plugin re-prepared from the
    # options it already holds carries: parsing it again must answer itself.
    test "an already-built list passes through unchanged" do
      assert names_row(["alerts", "audit"]) == {:ok, ["alerts", "audit"]}

      # The one way to name a channel whose name contains a comma, which the
      # comma-separated spelling cannot reach.
      assert names_row(["alerts,audit"]) == {:ok, ["alerts,audit"]}
    end

    test "a repeated name is accepted, since it selects the same records" do
      assert names_row("alerts,alerts") == {:ok, ["alerts", "alerts"]}
    end

    test "an empty member is refused rather than dropped, naming the option" do
      for value <- ["alerts,,audit", "alerts,", ",", "", " ", ["alerts", ""]] do
        assert {:error, "Invalid value for channel_out: " <> _} = names_row(value)
      end
    end

    test "a value that is no name at all is refused" do
      assert {:error, "Invalid value for channel_out: 7"} = names_row(7)
      assert {:error, "Invalid value for channel_out: [\"a\", 7]"} = names_row(["a", 7])
    end

    test "the filter matches any of its names, and no filter matches everything" do
      assert ProviderBehaviour.in_channel?(%{"channel" => "alerts"}, nil)
      assert ProviderBehaviour.in_channel?(%{}, nil)
      assert ProviderBehaviour.in_channel?(%{"channel" => "alerts"}, ["alerts"])
      refute ProviderBehaviour.in_channel?(%{"channel" => "metrics"}, ["alerts"])
      assert ProviderBehaviour.in_channel?(%{"channel" => "audit"}, ["alerts", "audit"])
      assert ProviderBehaviour.in_channel?(%{"channel" => "alerts"}, ["alerts", "alerts"])
      refute ProviderBehaviour.in_channel?(%{"channel" => "metrics"}, ["alerts", "audit"])
      refute ProviderBehaviour.in_channel?(%{}, ["alerts", "audit"])
    end

    test "a plugin that only acts on the chain filters several through `channel`" do
      assert {:ok, %{filter_channel: ["alerts", "audit"]}} = kafka_out(%{channel: "alerts,audit"})

      assert {:ok, %{filter_channel: ["alerts", "audit"]}} =
               kafka_out(%{channel_out: "alerts,audit"})
    end

    test "the entry side takes one name, whichever spelling names it" do
      for opts <- [%{channel: "alerts,audit"}, %{channel_in: "alerts,audit"}] do
        assert {:error, message} = normalised(HttpPoll, poll_opts(opts))
        assert message =~ "http_poll names several channels in channel"
        assert message =~ "a payload is injected on exactly one channel"
      end
    end

    # A websocket plugin's `channel` names the entry channel too, so a set there
    # is refused for the same reason and `channel_out` is where it belongs.
    test "a feeding plugin's `channel` is refused a set, and its `channel_out` is not" do
      ws = %{uri: "wss://example.invalid/s", subscribed_extensions: [:on_output]}

      assert {:error, message} = normalised(Websocket, Map.put(ws, :channel, "alerts,audit"))
      assert message =~ "websocket names several channels in channel"

      assert {:ok, %{entry_channel: nil, filter_channel: ["alerts", "audit"]}} =
               normalised(Websocket, Map.put(ws, :channel_out, "alerts,audit"))
    end

    test "resolving an already-resolved set answers the same thing" do
      assert {:ok, once} = kafka_out(%{channel_out: "alerts,audit"})
      assert {:ok, twice} = normalised(Kafka.Producer, once)

      assert twice.filter_channel == ["alerts", "audit"]
      keys = [:channel, :channel_out, :filter_channel]
      assert Map.take(twice, keys) == Map.take(once, keys)
    end

    # The resolution answers a set to `in_channel?/2` and one name to the
    # injection sites, and it reads whatever the row's type left behind. A row
    # typed as a plain string would make that a second reading of the option.
    test "every declared channel row is typed as a name set" do
      drifted =
        for {id, module} <- Flezha.Plugin.Providers.all(),
            {name, row} <- module.__provider__().options,
            name in [:channel, :channel_in, :channel_out],
            row[:type] != :names,
            do: "#{id}.#{name} is #{inspect(row[:type])}"

      assert drifted == []
    end
  end

  describe "the help screen offers what the enable takes" do
    # Driven through the verb, because the id an operator copies from the screen
    # is the one the verb was asked for, and the module behind it cannot know it.
    test "every provider's screen names its registered id and lists its public options only" do
      checked =
        for {id, module} <- Flezha.Plugin.Providers.all(), Code.ensure_loaded?(module) do
          screen = capture_io(fn -> Cli.Plugin.plugin_help(id, %{opts: []}) end)

          assert screen =~ "flz plugin enable <plugin_id> #{id} [OPTIONS]"

          for {name, row} <- module.__provider__().options do
            internal = Keyword.get(row, :internal, false)
            offered = screen =~ "--X#{name} <value>"

            assert offered != internal,
                   "#{id}: --X#{name} is internal: #{internal}, and the screen says otherwise"
          end

          id
        end

      # An empty table, or one without the provider that declares internal rows,
      # would pass every assertion above.
      assert "operative_websocket" in checked
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
  end
end
