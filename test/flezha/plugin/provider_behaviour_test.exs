defmodule Flezha.Plugin.ProviderBehaviourTest do
  @moduledoc """
  The generated `build/1` contract: a value the provider's `convert_value/2`
  refuses is answered as `{:error, String.t()}`, which is what the
  `@callback` declares. Driven through real providers, because the defect
  this pins is invisible to a provider that declares no `@enforce_keys`.
  """

  use ExUnit.Case, async: true

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.Filter
  alias Flezha.Plugin.Provider.HttpPoll
  alias Flezha.Plugin.Provider.Kafka
  alias Flezha.Plugin.Provider.Postgres
  alias Flezha.Plugin.Provider.OperativeForward
  alias Flezha.Plugin.Provider.StdOut
  alias Flezha.Plugin.Provider.Websocket

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

  describe "convert_value/2 runs on the CLI path only" do
    # KI-62, resolved NOT A BUG: `build/1` takes a string map and is the only
    # caller of `convert_value/2`. A `%Flezha.Plugin{}` carrying typed opts
    # never crosses it, and the delegating shim must not widen that.
    test "a typed opts map reaches the provider as written" do
      opts = %{regex_pattern: "x", inverted: "not-a-bool"}

      plugin = %Plugin{id: "ki62", provider: Filter, opts: opts, subscriptions: [{:on_input, []}]}

      {:ok, table} = Table.subscribe(Table.new(@catalog), "ki62", plugin)

      # `init_provider/1` adds its own keys; what matters is that the value
      # `convert_value/2` would have refused is present, uncoerced.
      assert table.plugins["ki62"].opts.inverted == "not-a-bool"
      assert table.plugins["ki62"].opts.regex_pattern == "x"
    end

    test "the same value IS refused when it arrives as a CLI string map" do
      assert {:error, _} = Filter.build(%{"regex_pattern" => "x", "inverted" => "not-a-bool"})
    end
  end

  describe "the refusals build/1 already made are untouched" do
    test "a missing required field still names the field" do
      assert {:error, "Missing required fields: [:hosts, :topic]"} =
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
end
