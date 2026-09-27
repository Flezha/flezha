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

  It also holds the cross-SDK contracts that are static: the connect
  environment every SDK's discovery path reads, the heartbeat key set the
  controller must mirror, and the diagnostics every runner application this
  repository ships must install. Each is a fact two languages hold separately
  and nothing else compares.
  """

  use ExUnit.Case, async: true

  alias Flezha.Service.PortPilot
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.ReferenceTopologies
  alias FlezhaTest.Helper.SourceWalk

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
      "flz_transport/src/utility/socket_dir.rs",
      "flz_transport/src/utility/socket_probe.rs",
      "flz_transport/src/transport/config.rs",
      "flz_core/src/control_unit.rs"
    ],
    java: Path.wildcard("sdk/java/**/*.java"),
    node: Path.wildcard("sdk/node/src/**/*.ts"),
    ruby: Path.wildcard("sdk/ruby/lib/**/*.rb"),
    php: Path.wildcard("sdk/php/src/**/*.php"),
    python: Path.wildcard("sdk/python/src/**/*.py"),
    elixir: Path.wildcard("sdk/elixir/lib/**/*.ex")
  }

  # A variable this contract governs: the socket directory, the QUIC endpoints,
  # the group boundary. Anything else an SDK reads is its own business.
  @connect_env_shape ~r/FLEZHA_(?:[A-Z0-9_]*SOCKET[A-Z0-9_]*|QUIC_ADDRESS(?:ES)?)/

  # The one file allowed to name a retired variable is the one that refuses it.
  @retired_refusal_site "flz_transport/src/transport/config.rs"

  # The heartbeat's self-report is built here for every SDK but Elixir's, whose
  # core inherits nothing from the kernel and builds its own.
  @heartbeat_site "flz_core/src/coordinator.rs"

  # The administration surface an application reaches its own namespace with.
  # The verb travels as data, so every SDK spells the same names and nothing
  # compares them but this: the set is declared once here and each SDK's own
  # wrapper is measured against it. An SDK that defers the surface names the
  # change that owes it in its manifest and comes off this map in the same
  # commit.
  @admin_verbs ~w(
    Operator.start
    Operator.stop
    Operator.send
    Operator.list_operators
    Operator.cell_state
    PluginManager.enable
    PluginManager.disable
    PluginManager.list
  )

  @admin_surfaces %{
    rust: "sdk/rust/src/admin.rs",
    java: "sdk/java/flezha-sdk-api/src/main/java/com/flezha/sdk/admin/Admin.java",
    node: "sdk/node/src/admin.ts",
    python: "sdk/python/src/flezha/admin.py",
    ruby: "sdk/ruby/lib/flezha/admin.rb",
    php: "sdk/php/src/Admin.php",
    elixir: "sdk/elixir/lib/flezha/sdk/admin.ex"
  }

  # Names the bounded operator authoring surface retired: a bounded operator declares its graph
  # in `channels` and its retry ladder on the edge, as an unbounded operator does, and Node's
  # stream function is `defineUnbounded`. Each pattern is the spelling a
  # definition or a use would take, never prose.
  @retired_bounded_names [
    {"a `tasklets` body", ~r/\b(?:fn|def|void)\s+tasklets\s*\(/},
    {"`defineOperator`", ~r/\bdefineOperator\b/},
    {"a class-level retry knob",
     ~r/\b(?:retry_attempts|retryAttempts|barrier_retry_attempts|barrierRetryAttempts)\b|\bfn retry_backoff\(|\bretryBackoff\(/},
    {"`ZeroRetryBackoff`", ~r/\bZeroRetryBackoff\b/}
  ]

  # The authoring surface and everything written against it, but not the SDK
  # suites that name a retired spelling to prove it is refused.
  @authoring_roots ~w(
    flz_core/src flz_core_ffi/src sdk/rust/src sdk/rust/macros/src sdk/rust/examples
    sdk/rust/test_app sdk/java/flezha-sdk-api/src/main sdk/java/flezha-sdk-runtime/src/main
    sdk/java/flezha-sdk-examples sdk/java/examples
    sdk/java/test_app/src sdk/node/src sdk/node/examples sdk/node/test_app sdk/python/src
    sdk/python/test_app sdk/ruby/lib sdk/ruby/test_app sdk/php/src sdk/php/examples
    sdk/php/test_app sdk/elixir/lib sdk/elixir/test_app/lib examples
  )

  # A manifest must declare all of these.
  @required_fields ~w(runtime sdk_lang command artifacts build connect_timeout_ms transports dispatch toolchain deferred fixtures docs)

  # How a host executes activations. The vocabulary is closed and lives here,
  # so a new SDK either fits an existing model or extends this list on purpose.
  @dispatch_models ~w(thread-pool event-loop process-per-cell)

  # Bench tasks whose `--runtime` resolves against `PortPilot.runtimes/0`. Their
  # help text may name a runtime as an example but may not enumerate the set: a
  # second copy of it goes stale the run after an SDK is added.
  #
  # Filtered to what this checkout compiled. `mix bench.roundtrip` lives outside
  # the tree, so naming it unconditionally made both tests below die on
  # `module_info/1` in every clean clone: a red row about a module that is not
  # there, which says nothing about the runtime set either way.
  @manifest_runtime_tasks Enum.filter(
                            [
                              Mix.Tasks.Bench,
                              Mix.Tasks.Bench.Roundtrip,
                              Mix.Tasks.Bench.Bounded
                            ],
                            &Code.ensure_loaded?/1
                          )

  describe "the administration surface" do
    test "every runtime carries one, and it is the declared set" do
      for runtime <- runtimes() do
        path =
          Map.get(@admin_surfaces, runtime) ||
            fail("#{runtime} declares no administration surface; add one or defer it by name")

        source = File.read!(path)

        for verb <- @admin_verbs, not String.contains?(source, verb) do
          fail("#{path} does not name the administration verb `#{verb}`")
        end
      end
    end

    test "no SDK keeps a list of what it may call" do
      # Reachability follows the role's grants. A verb an SDK spells is a typed
      # wrapper; a verb it refuses on its own side would be a second policy
      # point, drifting the first time a row's grant changed.
      for {_runtime, path} <- @admin_surfaces do
        source = File.read!(path)

        for forbidden <- ["plugin.install", "controller.stop", "role_insufficient"] do
          if String.contains?(source, forbidden) do
            fail("#{path} names `#{forbidden}`; what an SDK may call is the controller's answer")
          end
        end
      end
    end
  end

  defp runtimes, do: PortPilot.runtimes()

  defp reference, do: NodeTestHelper.reference_runtime()

  defp manifest(runtime), do: PortPilot.manifest(runtime)

  defp fail(message), do: flunk(message <> "\n\nSee " <> @docs_section <> ".")

  # The core refuses a declared version that packs to zero, so "names a real
  # version" is the whole question here — the packing itself lives in
  # `flz_core::header::pack_version` and is not restated.
  defp names_a_real_version?(raw) do
    raw
    |> String.split(~r/[^0-9]+/, trim: true)
    |> Enum.any?(&(String.to_integer(&1) > 0))
  end

  defp moduledoc(module) do
    {:docs_v1, _, _, _, %{"en" => doc}, _, _} = Code.fetch_docs(module)
    doc
  end

  defp source(module) do
    module.module_info(:compile)[:source] |> to_string() |> File.read!()
  end

  describe "manifests" do
    test "every SDK test app carries one" do
      missing = Enum.reject(runner_app_dirs(), &File.exists?(Path.join(&1, "conformance.json")))

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

    test "each names only topology surfaces the reference table declares" do
      known = ReferenceTopologies.known_surfaces()

      for runtime <- runtimes(),
          surface <- PortPilot.manifest(runtime)["topology_surfaces"] || [],
          surface not in known do
        fail(
          "#{PortPilot.manifest_path(runtime)} declares topology surface #{inspect(surface)}, " <>
            "which test/fixtures/reference_topologies.txt does not name"
        )
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
        elixir: {"sdk/elixir/lib/flezha/sdk/codec.ex", ~r/@sdk_lang (\d+)/},
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

    test "each SDK's version constant agrees with its own package metadata" do
      # The durable header names the SDK that wrote a record, so each SDK stamps
      # its own released version rather than the core's. The shape is one literal
      # in the language's package metadata, one reader, and this gate failing
      # when the two disagree.
      #
      # Three rows have nothing to compare because the reader *is* the manifest:
      # the Rust compiler expands `env!("CARGO_PKG_VERSION")` against Cargo.toml,
      # Elixir reads `Application.spec(:flezha, :vsn)` from mix.exs, and Ruby's
      # `graph.rb` packs the single `Flezha::VERSION` its gemspec also reads.
      # PHP has nothing to compare *against*: `composer.json` declares no version
      # by design, so its constant is the sole source of truth and is asserted
      # only to parse non-zero.
      reader_is_the_manifest = [:rust, :elixir, :ruby]

      pinned = %{
        java:
          {{"sdk/java/flezha-sdk-runtime/src/main/java/com/flezha/sdk/runtime/core/HostClassDeclaration.java",
            ~r/SDK_VERSION = "([0-9][^"]*)"/},
           {"sdk/java/pom.xml", ~r|<version>([0-9][^<]*)</version>|}},
        node:
          {{"sdk/node/src/graph.ts", ~r/SDK_VERSION = '([0-9][^']*)'/},
           {"sdk/node/package.json", ~r/"version":\s*"([0-9][^"]*)"/}},
        python:
          {{"sdk/python/src/flezha/version.py", ~r/__version__ = "([0-9][^"]*)"/},
           {"sdk/python/pyproject.toml", ~r/^version = "([0-9][^"]*)"/m}}
      }

      php_constant = {"sdk/php/src/Graph/HostClassDecl.php", ~r/SDK_VERSION = '([0-9][^']*)'/}

      read = fn {path, pattern} ->
        case Regex.run(pattern, File.read!(path)) do
          [_, value] -> value
          nil -> fail("#{path} no longer spells a version where this gate reads one")
        end
      end

      for runtime <- runtimes(), runtime not in reader_is_the_manifest do
        if runtime == :php do
          version = read.(php_constant)
          {path, _} = php_constant

          unless names_a_real_version?(version) do
            fail(
              "#{path} stamps SDK_VERSION #{inspect(version)}, which packs to 0 — " <>
                "0 means \"the writing SDK did not say\" and the core refuses it at " <>
                "declaration time. PHP has no package metadata to compare against " <>
                "(composer.json declares no version by design), so this constant is the " <>
                "SDK's sole source of truth and the release checklist owes its bump"
            )
          end
        else
          {constant, metadata} =
            Map.get(pinned, runtime) ||
              fail(
                "runtime #{runtime} has a manifest but no entry in this test's " <>
                  "source-of-truth table; add where its SDK spells its version, or " <>
                  "record it as one whose reader is the manifest itself"
              )

          stamped = read.(constant)
          declared = read.(metadata)
          {constant_path, _} = constant
          {metadata_path, _} = metadata

          unless stamped == declared do
            fail(
              "#{constant_path} stamps version #{inspect(stamped)}, but " <>
                "#{metadata_path} declares #{inspect(declared)} — the durable header " <>
                "names the SDK that wrote the record, so a package cannot ship a " <>
                "version its records disagree with"
            )
          end
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

    test "the required set is exactly the capabilities no manifest defers" do
      required = MapSet.new(NodeTestHelper.required_capabilities())
      deferred = Map.new(runtimes(), &{&1, NodeTestHelper.deferred(&1)})

      for {runtime, capabilities} <- deferred,
          capability <- capabilities,
          MapSet.member?(required, capability) do
        fail(
          "#{PortPilot.manifest_path(runtime)} defers #{capability}, which every SDK carries " <>
            "(the required set is #{inspect(MapSet.to_list(required))})"
        )
      end

      deferred_somewhere = deferred |> Map.values() |> List.flatten() |> MapSet.new()

      for capability <- NodeTestHelper.capabilities(),
          not MapSet.member?(deferred_somewhere, capability),
          not MapSet.member?(required, capability) do
        fail(
          "no manifest defers #{capability}, so every SDK carries it: add it to " <>
            "@required_capabilities in test/helpers/node_test_helper.ex, or defer it by name " <>
            "in the manifest of the SDK that does not carry it"
        )
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

    test "no bench task decides its runtime set in code" do
      for task <- @manifest_runtime_tasks do
        src = source(task)
        named = Enum.filter(runtimes(), &(src =~ ~s("#{&1}")))

        if named != [] do
          fail(
            "#{inspect(task)} names #{inspect(named)} as string literals in its own source, " <>
              "so it decides the accepted `--runtime` set from a clause list rather than from " <>
              "the manifests. Derive it: `Mix.Tasks.Bench.Fleet.parse_runtime/3` reads " <>
              "`PortPilot.runtimes/0` and the manifest's `deferred` key"
          )
        end
      end
    end

    test "no bench task documents a runtime list beside the one it derives" do
      names = Enum.map(runtimes(), &Atom.to_string/1)

      for task <- @manifest_runtime_tasks, line <- String.split(moduledoc(task), "\n") do
        named = Enum.filter(names, &Regex.match?(~r/\b#{&1}\b/, line))

        if length(named) > 1 do
          fail(
            "#{inspect(task)} names #{inspect(named)} on one documentation line, a second " <>
              "copy of the set its `--runtime` reads from the manifests: " <>
              "#{inspect(names)}. Name the manifest path, not the runtimes"
          )
        end
      end
    end
  end

  describe "the contract set" do
    # `test/fixtures/*.txt` and nothing else: the cross-language data tables
    # carry the fixture header, while `declarations/` is this tree's own golden
    # JSON and the `.ex`/`.exs` beside them are Elixir the suite compiles.
    @contract_manifest "test/fixtures/contract.txt"

    test "every fixture under test/fixtures is listed" do
      unlisted = Path.wildcard("test/fixtures/*.txt") -- contract_members()

      if unlisted != [] do
        fail(
          "these fixtures are not listed in #{@contract_manifest}: #{inspect(unlisted)}. " <>
            "An SDK vendoring the set would not receive them, and its tests would fail " <>
            "outside this tree"
        )
      end
    end

    test "every member the list names is present" do
      absent = Enum.reject(contract_members(), &File.regular?/1)

      if absent != [] do
        fail(
          "#{@contract_manifest} names members that are not in the tree: #{inspect(absent)}. " <>
            "The release stages the set from this list, so a row naming nothing stages nothing"
        )
      end
    end

    test "every member says what reads it" do
      for {path, purpose} <- contract_rows() do
        assert String.length(purpose) > 20,
               "#{@contract_manifest} row #{path} says `#{purpose}`; a member with no stated " <>
                 "reader cannot be removed by anyone who did not write it"
      end
    end

    defp contract_members, do: Enum.map(contract_rows(), &elem(&1, 0))

    defp contract_rows do
      @contract_manifest
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Enum.reject(&(String.starts_with?(&1, "#") or String.trim(&1) == ""))
      |> Enum.map(fn row ->
        [path, purpose] = String.split(row, "|", parts: 2)
        {String.trim(path), String.trim(purpose)}
      end)
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
          {kind, declaration} <- tag_declarations(source),
          kind != "module",
          declaration =~ ~r/\bcapability: :\w+/,
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

        # A hand-written runner command is the same bypass, invisible to the
        # literal check: the scenario is green on every runtime for the
        # reference's reasons. `test/apps/#{...}_runner` is the harness
        # selecting, and `\w+` does not match it.
        for [line] <- Regex.scan(~r/^.*test\/apps\/\w+_runner\/.*$/m, source) do
          fail(
            "#{file} spawns a runner by hand (#{String.trim(line)}); go through " <>
              "NodeTestHelper.start_runner_port/2, which spawns the runtime the run selected"
          )
        end
      end
    end

    test "a capability no module is tagged for fails" do
      tagged =
        for {_file, source} <- system_sources(),
            capability <- capabilities_in(source),
            into: MapSet.new(),
            do: String.to_atom(capability)

      for capability <- NodeTestHelper.capabilities(),
          not MapSet.member?(tagged, capability) do
        fail(
          "no test/system module is tagged for #{inspect(capability)}, which every manifest " <>
            "must therefore defer — a capability carried under `fixtures` is one a scenario proves"
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
      source = File.read!("flz_transport/src/utility/socket_dir.rs")

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
              SourceWalk.files(
                ["#{@apps_root}/#{runtime}_runner"],
                ~w(.rs .java .ex .exs .ts .js .mjs .json)
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

  describe "the bounded authoring surface" do
    test "no SDK reintroduces a name the bounded surface retired" do
      files =
        SourceWalk.files(@authoring_roots, ~w(.rs .java .ex .exs .ts .js .mjs .py .rb .php))

      assert length(files) > 100, "read only #{length(files)} files; a root moved"

      for {file, line, number} <- SourceWalk.code_lines(files),
          {name, pattern} <- @retired_bounded_names,
          line =~ pattern do
        fail("#{file}:#{number} reintroduces #{name}: #{String.trim(line)}")
      end
    end
  end

  describe "data heartbeat" do
    test "the controller mirrors every key the shared core's heartbeat builds" do
      core = core_heartbeat_keys()

      refute Enum.empty?(core),
             "no keys were read out of #{@heartbeat_site}: heartbeat_metadata was restructured " <>
               "and this gate no longer measures anything"

      mirrored = MapSet.new(Flezha.Runner.StreamChannel.heartbeat_keys())
      unmirrored = MapSet.difference(MapSet.new(core), mirrored)

      if MapSet.size(unmirrored) > 0 do
        fail(
          "#{@heartbeat_site} builds #{Enum.join(Enum.sort(unmirrored), ", ")}, which the " <>
            "controller holds no liveness slot for: the runner pays for them on the wire and " <>
            "they reach no operations surface"
        )
      end
    end

    test "the Elixir SDK's heartbeat carries exactly the keys the controller mirrors" do
      # The Elixir SDK is its own core and declares its own key set: `flz_core`'s
      # says nothing about what it sends. A key the controller holds a slot for
      # and this runtime omits is an operations surface that is correct for six
      # SDKs and blank for the seventh; a key it sends and nothing mirrors is
      # paid for on the wire and read by no one.
      declared = MapSet.new(Flezha.SDK.Counters.heartbeat_keys())
      sent = Flezha.SDK.RunnerServer.heartbeat_metadata(Flezha.SDK.Counters.new())
      mirrored = MapSet.new(Flezha.Runner.StreamChannel.heartbeat_keys())
      missing = MapSet.difference(mirrored, declared)
      unmirrored = MapSet.difference(declared, mirrored)

      if MapSet.size(missing) + MapSet.size(unmirrored) > 0 do
        fail(
          "the Elixir SDK's data heartbeat omits #{Enum.join(Enum.sort(missing), ", ")} and " <>
            "sends #{Enum.join(Enum.sort(unmirrored), ", ")}, which the controller does not mirror"
        )
      end

      assert MapSet.new(Map.keys(sent)) == declared

      # A runtime that discarded nothing reports zero rather than omitting the
      # key: "nothing was discarded" and "nothing was measured" are different
      # answers, and only the first is honest about a runtime that was asked.
      assert sent["restore_discarded_inputs"] == "0"
    end
  end

  describe "runner diagnostics" do
    test "every runner app built on the Rust SDK installs the default subscriber" do
      for {app, main} <- rust_sdk_runner_apps(),
          not (File.read!(main) =~ "init_default_tracing()") do
        fail(
          "#{app} links flezha_runner and never calls " <>
            "flezha_runner::diag::init_default_tracing(): every `tracing` event the SDK and " <>
            "the shared kernel emit is written to a subscriber that does not exist, so the " <>
            "reconnect, flood-ladder and mailbox-stall diagnostics are unobservable in the " <>
            "drills that exist to test recovery"
        )
      end
    end
  end

  # Runner applications this repository ships that link the Rust SDK, as
  # `{app, main.rs}`. The SDK installs no subscriber of its own — that is its
  # rule — so the application is the caller that rule assumes.
  defp rust_sdk_runner_apps do
    for app <- runner_app_dirs(),
        manifest = Path.join(app, "Cargo.toml"),
        File.exists?(manifest),
        File.read!(manifest) =~ ~r/^\s*flezha_runner\s*=/m,
        main = Path.join(app, "src/main.rs"),
        File.exists?(main),
        do: {app, main}
  end

  # Every SDK test app directory this repository ships.
  defp runner_app_dirs do
    Path.join(@apps_root, "*_runner") |> Path.wildcard() |> Enum.filter(&File.dir?/1)
  end

  # The keys `flz_core`'s `heartbeat_metadata` builds — the wire contract five
  # runtimes share (Rust and Java through the kernel, every FFI veneer through
  # it too). Read from the source because the set lives in another language;
  # the Elixir SDK builds its own and is measured by its own suite.
  defp core_heartbeat_keys do
    body =
      case Regex.run(~r/fn heartbeat_metadata\(\).*?\n\}\n/s, File.read!(@heartbeat_site)) do
        [body] -> body
        nil -> ""
      end

    for [_, key] <- Regex.scan(~r/\(\s*"([a-z_]+)",/, body), uniq: true, do: key
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

  # A tag declaration as ExUnit reads it: the `@tag` line plus every line
  # indented past it. The backreference on the opening indent is what stops the
  # `test` line below a tag from being read as part of it.
  defp tag_declarations(source) do
    for [declaration, _indent, kind] <-
          Regex.scan(~r/^([ \t]*)@(module|describe|)tag\b.*(?:\n\1[ \t]+\S.*)*/m, source),
        do: {kind, declaration}
  end

  # Every capability a file claims: its `@moduletag` plus any per-test
  # `@tag capability:` override, as the capability strings the manifests use.
  # A tag is a keyword list, so `capability:` is read wherever it sits in one.
  defp capabilities_in(source) do
    from_use =
      case Regex.run(~r/use FlezhaTest\.SystemCase, capability: :(\w+)/, source) do
        [_, capability] -> [capability]
        nil -> []
      end

    from_tags =
      for {_kind, declaration} <- tag_declarations(source),
          [_, capability] <- Regex.scan(~r/\bcapability: :(\w+)/, declaration),
          do: capability

    Enum.uniq(from_use ++ from_tags)
  end
end
