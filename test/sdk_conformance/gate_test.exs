defmodule FlezhaTest.SdkConformance.GateTest do
  @moduledoc """
  The static drift gate: everything about the cross-SDK conformance suite that
  can be checked without spawning a runner. Untagged on purpose — it runs in
  plain `mix test`, on every pull request, so a manifest, a capability tag or a
  fixture id that drifts is caught by the change that introduces it rather than
  by a nightly.

  What the nightly proves is behaviour; what this proves is that every SDK is
  still accounted for. Both failure directions ride the same rules: an SDK that
  neither carries a capability nor defers it by name, and a capability, fixture
  or runtime added to one SDK and not the others.
  """

  use ExUnit.Case, async: true

  alias Flezha.Service.PortPilot
  alias FlezhaTest.Helper.NodeTestHelper

  @apps_root "test/apps"
  @system_dir "test/system"
  @nightly ".github/workflows/nightly-e2e.yml"
  @docs_section "docs/sdk/conformance.md § Adding an SDK to the conformance suite"

  # The environment contract every SDK's connect path reads, pinned as data the
  # way declaration keys are. `flz_transport` pins its own constants against
  # this file; here it is measured against every SDK.
  @connect_env_fixture "test/fixtures/runner_connect_env.txt"

  # Where each SDK decides where its controller is. Rust, Java, Node, Ruby, PHP
  # and Python reach the shared ladder through `flz_core`, so their own sources
  # should name no connect variable at all; Elixir asks the same ladder through
  # `flz_nif`.
  @connect_sources %{
    rust: [
      "flz_transport/src/utility/path_helper.rs",
      "flz_transport/src/transport/config.rs",
      "flz_core/src/control_unit.rs"
    ],
    java: Path.wildcard("sdk/java/**/*.java"),
    node: Path.wildcard("sdk/node/src/**/*.ts"),
    ruby: Path.wildcard("sdk/ruby/lib/**/*.rb"),
    php: Path.wildcard("sdk/php/src/**/*.php"),
    python: Path.wildcard("sdk/python/src/**/*.py"),
    elixir: ["lib/flezha/sdk/transport.ex", "lib/flezha/transport/native.ex"]
  }

  # A variable this contract governs: the socket directory, the QUIC endpoints,
  # the group boundary. Anything else an SDK reads is its own business.
  @connect_env_shape ~r/FLEZHA_(?:[A-Z0-9_]*SOCKET[A-Z0-9_]*|QUIC_ADDRESS(?:ES)?)/

  # The one file allowed to name a retired variable is the one that refuses it.
  @retired_refusal_site "flz_transport/src/transport/config.rs"

  # A manifest must declare all of these.
  @required_fields ~w(runtime sdk_lang command artifacts build connect_timeout_ms transports dispatch toolchain deferred fixtures docs)

  # How a host executes activations. The vocabulary is closed and lives here,
  # so a new SDK either fits an existing model or extends this list on purpose.
  @dispatch_models ~w(thread-pool event-loop process-per-entity)

  defp runtimes, do: PortPilot.runtimes()

  defp reference, do: NodeTestHelper.reference_runtime()

  defp manifest(runtime), do: PortPilot.manifest(runtime)

  defp fail(message), do: flunk(message <> "\n\nSee " <> @docs_section <> ".")

  describe "manifests" do
    test "every SDK test app carries one" do
      apps =
        Path.wildcard(Path.join(@apps_root, "*_runner"))
        |> Enum.filter(&File.dir?/1)

      missing = Enum.reject(apps, &File.exists?(Path.join(&1, "conformance.json")))

      if missing != [] do
        fail("these SDK test app directories have no conformance.json: #{inspect(missing)}")
      end
    end

    test "each declares every field, with both artifact and build profiles" do
      for runtime <- runtimes() do
        manifest = manifest(runtime)
        path = PortPilot.manifest_path(runtime)

        for field <- @required_fields do
          unless Map.has_key?(manifest, field) do
            fail("#{path} declares no #{field}")
          end
        end

        unless manifest["runtime"] == Atom.to_string(runtime) do
          fail("#{path} declares runtime #{inspect(manifest["runtime"])}, not #{runtime}")
        end

        for profile <- ~w(test bench), section <- ~w(command artifacts build) do
          unless Map.has_key?(manifest[section], profile) do
            fail("#{path}: #{section} has no #{profile} profile")
          end
        end

        unless manifest["artifacts"]["test"] != [] and manifest["artifacts"]["bench"] != [] do
          fail("#{path}: an artifacts profile is empty")
        end
      end
    end

    test "each declares how its host dispatches and how to ask its toolchain's version" do
      for runtime <- runtimes() do
        path = PortPilot.manifest_path(runtime)
        dispatch = PortPilot.dispatch(runtime)

        unless dispatch["model"] in @dispatch_models do
          fail(
            "#{path} declares dispatch.model #{inspect(dispatch["model"])}; expected one of " <>
              "#{inspect(@dispatch_models)} — a number taken under one dispatch topology and " <>
              "one taken under another are different measurements of the same fixture"
          )
        end

        unless is_binary(dispatch["detail"]) and dispatch["detail"] != "" do
          fail("#{path} declares no dispatch.detail sentence")
        end

        unless is_map(dispatch["env"]) do
          fail(
            "#{path} declares dispatch.env #{inspect(dispatch["env"])}; expected a map from " <>
              "each environment variable that selects the topology to what it selects " <>
              "(empty when the topology is fixed)"
          )
        end

        unless is_binary(manifest(runtime)["toolchain"]) and
                 manifest(runtime)["toolchain"] != "" do
          fail(
            "#{path} declares no toolchain version command — a bench header names the " <>
              "toolchain a number was taken under, and reads it from here rather than " <>
              "from a per-runtime clause of its own"
          )
        end

        for {var, means} <- dispatch["env"] do
          unless is_binary(var) and String.starts_with?(var, "FLEZHA_") and
                   is_binary(means) and means != "" do
            fail("#{path}: dispatch.env entry #{inspect({var, means})} is not FLEZHA_VAR => text")
          end
        end
      end
    end

    test "each declares a distinct, allocated sdk_lang" do
      langs =
        for runtime <- runtimes(), into: %{} do
          lang = PortPilot.sdk_lang(runtime)
          path = PortPilot.manifest_path(runtime)

          unless is_integer(lang) and lang > 0 do
            fail(
              "#{path} declares sdk_lang #{inspect(lang)}; every runtime holds an " <>
                "allocated value from the table in flz_core/src/header.rs (0 is " <>
                "reserved and never places)"
            )
          end

          {runtime, lang}
        end

      duplicates =
        langs
        |> Enum.group_by(fn {_runtime, lang} -> lang end, fn {runtime, _lang} -> runtime end)
        |> Enum.filter(fn {_lang, runtimes} -> length(runtimes) > 1 end)

      for {lang, runtimes} <- duplicates do
        fail(
          "sdk_lang #{lang} is declared by #{inspect(runtimes)}; one value per SDK, " <>
            "allocated in landing order — take the next free value from the table in " <>
            "flz_core/src/header.rs"
        )
      end
    end

    test "each manifest's sdk_lang is the value its SDK actually stamps" do
      # Static half of the agreement check: the manifest against the constant
      # the SDK compiles in. The nightly `interop` scenario proves the other
      # half — that the runner advertises it on connect.
      stamped = %{
        rust: {"flz_core/src/header.rs", ~r/pub const SDK_LANG_RUST: u8 = (\d+);/},
        java:
          {"sdk/java/flezha-sdk-runtime/src/main/java/com/flezha/sdk/runtime/core/HostClassDeclaration.java",
           ~r/SDK_LANG_JAVA = (\d+);/},
        elixir: {"lib/flezha/sdk/codec.ex", ~r/@sdk_lang (\d+)/},
        node: {"sdk/node/src/graph.ts", ~r/SDK_LANG_NODE = (\d+)/},
        ruby: {"sdk/ruby/lib/flezha/graph.rb", ~r/SDK_LANG_RUBY = (\d+)/},
        php: {"sdk/php/src/Graph/HostClassDecl.php", ~r/SDK_LANG_PHP = (\d+);/},
        python: {"sdk/python/src/flezha/graph.py", ~r/SDK_LANG_PYTHON = (\d+)/}
      }

      for runtime <- runtimes() do
        {path, pattern} =
          Map.get(stamped, runtime) ||
            fail(
              "runtime #{runtime} has a manifest but no entry in this test's " <>
                "source-of-truth table; add where its SDK spells its language byte"
            )

        source = File.read!(path)

        declared =
          case Regex.run(pattern, source) do
            [_, value] -> String.to_integer(value)
            nil -> fail("#{path} no longer spells #{runtime}'s language byte as expected")
          end

        unless declared == PortPilot.sdk_lang(runtime) do
          fail(
            "#{PortPilot.manifest_path(runtime)} declares sdk_lang " <>
              "#{PortPilot.sdk_lang(runtime)}, but #{path} stamps #{declared} — a " <>
              "manifest cannot drift from its SDK"
          )
        end
      end
    end

    test "every capability appears exactly once, under deferred or fixtures" do
      vocabulary = MapSet.new(NodeTestHelper.capabilities())

      for runtime <- runtimes() do
        manifest = manifest(runtime)
        path = PortPilot.manifest_path(runtime)
        deferred = MapSet.new(manifest["deferred"], &String.to_atom/1)
        carried = MapSet.new(Map.keys(manifest["fixtures"]), &String.to_atom/1)

        for unknown <- MapSet.difference(MapSet.union(deferred, carried), vocabulary) do
          fail("#{path} names #{unknown}, which is not a capability")
        end

        for both <- MapSet.intersection(deferred, carried) do
          fail("#{path} both defers and carries #{both}")
        end

        for missing <- MapSet.difference(vocabulary, MapSet.union(deferred, carried)) do
          fail("#{path} accounts for neither carrying nor deferring #{missing}")
        end
      end
    end

    test "no required capability is deferred" do
      required = MapSet.new(NodeTestHelper.required_capabilities())

      for runtime <- runtimes() do
        for deferred <-
              MapSet.intersection(MapSet.new(NodeTestHelper.deferred(runtime)), required) do
          fail(
            "#{PortPilot.manifest_path(runtime)} defers #{deferred}, which every SDK carries " <>
              "(the required set is #{inspect(MapSet.to_list(required))})"
          )
        end
      end
    end

    test "a carried capability's fixtures equal the reference's" do
      reference = manifest(reference())["fixtures"]

      for runtime <- runtimes(), runtime != reference() do
        path = PortPilot.manifest_path(runtime)

        for {capability, ids} <- manifest(runtime)["fixtures"] do
          expected = MapSet.new(Map.get(reference, capability, []))
          actual = MapSet.new(ids)

          for extra <- MapSet.difference(actual, expected) do
            fail(
              "#{path}: #{capability} lists #{extra}, which the #{reference()} reference does not"
            )
          end

          for missing <- MapSet.difference(expected, actual) do
            fail(
              "#{path}: #{capability} is carried but does not list #{missing}, which the " <>
                "#{reference()} reference does"
            )
          end
        end
      end
    end

    test "every runtime is dispatchable from the nightly" do
      options =
        @nightly
        |> File.read!()
        |> String.split("\n")
        |> Enum.drop_while(&(not String.contains?(&1, "options:")))
        |> Enum.drop(1)
        |> Enum.take_while(&Regex.match?(~r/^\s+-\s+\S+\s*$/, &1))
        |> Enum.map(&(&1 |> String.trim() |> String.trim_leading("- ")))
        |> MapSet.new()

      for runtime <- runtimes(), Atom.to_string(runtime) not in options do
        fail(
          "#{runtime} has a manifest but is not an option of #{@nightly}'s workflow_dispatch " <>
            "runtime input (options: #{inspect(MapSet.to_list(options))})"
        )
      end
    end

    test "each names a docs page that points back at it" do
      for runtime <- runtimes() do
        manifest = manifest(runtime)
        path = PortPilot.manifest_path(runtime)
        docs = manifest["docs"]

        unless File.exists?(docs) do
          fail("#{path} names docs page #{docs}, which does not exist")
        end

        unless File.read!(docs) =~ path do
          fail("#{docs} does not reference #{path}; the manifest is where the scope is declared")
        end
      end
    end
  end

  describe "test/system" do
    test "every module declares a vocabulary capability" do
      vocabulary = NodeTestHelper.capabilities()

      for {file, source} <- system_sources() do
        case Regex.run(~r/use FlezhaTest\.SystemCase, capability: :(\w+)/, source) do
          nil ->
            fail("#{file} does not `use FlezhaTest.SystemCase, capability: :<capability>`")

          [_, capability] ->
            unless String.to_atom(capability) in vocabulary do
              fail("#{file} declares capability :#{capability}, which is not in the vocabulary")
            end
        end
      end

      # A per-test or per-describe override must carry its own skip, or a
      # deferring SDK would run it anyway (an include overrides an exclude).
      for {file, source} <- system_sources(),
          [declaration] <-
            Regex.scan(~r/^.*@(?:describe)?tag capability: :\w+.*(?:\n\s+\S.*)?$/m, source),
          not (declaration =~ "skip:") do
        fail("#{file} overrides the capability without a skip: #{String.trim(declaration)}")
      end
    end

    test "a fixture id a file uses belongs to a capability that file is tagged with" do
      reference = manifest(reference())["fixtures"]

      # An id may be shared by capabilities (a partitioned fixture is also a
      # keyed one), so a file is allowed any capability that lists it.
      capabilities_of =
        Enum.reduce(reference, %{}, fn {capability, ids}, acc ->
          Enum.reduce(ids, acc, fn id, acc ->
            Map.update(acc, id, [capability], &[capability | &1])
          end)
        end)

      for {file, source} <- system_sources() do
        tagged = capabilities_in(source)

        for [_, literal] <- Regex.scan(~r/"([a-z][a-z0-9_]*)"/, source),
            owners = Map.get(capabilities_of, literal),
            owners != nil,
            owners -- (owners -- tagged) == [] do
          fail(
            "#{file} is tagged #{inspect(tagged)} but drives the fixture #{inspect(literal)}, " <>
              "which the reference manifest lists under #{inspect(owners)}"
          )
        end
      end
    end

    test "no runtime literal outside an interop scenario" do
      for {file, source} <- system_sources(),
          not (source =~ ~r/@(module|describe)?tag interop: true/) do
        for [line] <- Regex.scan(~r/^.*\bruntime: :\w+.*$/m, source) do
          fail("#{file} pins a runtime (#{String.trim(line)}); the run selects it")
        end
      end
    end

    test "a capability with no tagged module is reported, not failed" do
      tagged =
        for {_file, source} <- system_sources(),
            capability <- capabilities_in(source),
            into: MapSet.new(),
            do: String.to_atom(capability)

      untested = Enum.reject(NodeTestHelper.capabilities(), &MapSet.member?(tagged, &1))

      if untested != [] do
        IO.puts(
          "[conformance] no test/system module is tagged for: " <>
            Enum.map_join(untested, ", ", &inspect/1)
        )
      end
    end
  end

  describe "connect discovery" do
    test "the pinned contract names the variables and the ladder, in order" do
      %{variables: variables, retired: retired, ladder: ladder} = connect_env()

      assert "FLEZHA_SOCKET_FILES_PATH" in variables
      assert "FLEZHA_QUIC_ADDRESSES" in variables
      assert "FLEZHA_SOCKET_DIR_GROUP" in variables
      assert retired == ["FLEZHA_QUIC_ADDRESS"]

      assert Enum.map(ladder, &elem(&1, 0)) == ["explicit", "xdg_runtime", "run", "repository"]
    end

    test "no SDK's connect path reads a connect variable the fixture does not pin" do
      %{variables: variables, retired: retired} = connect_env()
      pinned = MapSet.new(variables ++ retired)

      for {runtime, sources} <- @connect_sources,
          file <- sources,
          File.exists?(file),
          [found] <- Regex.scan(@connect_env_shape, File.read!(file)),
          not MapSet.member?(pinned, found) do
        fail(
          "#{runtime} reads #{found} in #{file}, which #{@connect_env_fixture} does not pin — " <>
            "a variable spelled differently in one SDK is a variable the others cannot honour"
        )
      end
    end

    test "the ladder is walked in the pinned order by the one implementation that walks it" do
      %{ladder: ladder} = connect_env()
      source = File.read!("flz_transport/src/utility/path_helper.rs")

      body =
        case Regex.run(~r/fn candidates_from\(.*?\n\}/s, source) do
          [body] -> body
          nil -> fail("flz_transport no longer builds the ladder in candidates_from/2")
        end

      walked =
        for [_, rung] <- Regex.scan(~r/Rung::(\w+)/, body), uniq: true, do: rung

      expected = for {rung, _value} <- ladder, do: Macro.camelize(rung)

      unless walked == expected do
        fail(
          "flz_transport walks the ladder as #{inspect(walked)} but " <>
            "#{@connect_env_fixture} pins #{inspect(expected)}"
        )
      end
    end

    test "no SDK and no test app honours a retired name" do
      %{retired: retired} = connect_env()

      apps =
        for runtime <- runtimes(),
            file <-
              Path.wildcard(
                "#{@apps_root}/#{runtime}_runner/**/*.{rs,java,ex,exs,ts,js,mjs,json}"
              ),
            do: {runtime, file}

      sdks = for {runtime, files} <- @connect_sources, file <- files, do: {runtime, file}

      for {runtime, file} <- sdks ++ apps,
          file != @retired_refusal_site,
          File.exists?(file),
          name <- retired,
          # The replacement contains the retired name as a prefix, so only a
          # whole-word match is a reading of the retired one.
          File.read!(file) =~ ~r/#{name}(?![A-Z0-9_])/ do
        fail("#{runtime} reads the retired #{name} in #{file}; set FLEZHA_QUIC_ADDRESSES instead")
      end
    end
  end

  # The fixture, parsed once per test: whitespace-separated rows, `#` comments.
  defp connect_env do
    rows =
      @connect_env_fixture
      |> File.read!()
      |> String.split("\n")
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == "" or String.starts_with?(&1, "#")))
      |> Enum.map(&String.split/1)

    %{
      variables: for(["variable", name] <- rows, do: name),
      retired: for(["retired", name] <- rows, do: name),
      ladder: for(["ladder", _index, rung, value] <- rows, do: {rung, value})
    }
  end

  defp system_sources do
    for file <- Path.wildcard(Path.join(@system_dir, "*.exs")), do: {file, File.read!(file)}
  end

  # Every capability a file claims: its `@moduletag` plus any per-test
  # `@tag capability:` override, as the capability strings the manifests use.
  defp capabilities_in(source) do
    for [_, capability] <-
          Regex.scan(
            ~r/(?:use FlezhaTest\.SystemCase, capability|@(?:module|describe)?tag capability): :(\w+)/,
            source
          ),
        uniq: true,
        do: capability
  end
end
