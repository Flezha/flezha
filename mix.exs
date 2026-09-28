defmodule Flezha.MixProject do
  use Mix.Project

  def project do
    [
      app: :flezha,
      version: "0.1.0",
      elixir: "~> 1.17",
      description: "Distributed stateful operators — durable unbounded and bounded compute",
      source_url: "https://github.com/flezha/flezha",
      package: [
        licenses: ["MPL-2.0", "Apache-2.0"],
        links: %{"GitHub" => "https://github.com/flezha/flezha"}
      ],
      start_permanent: Mix.env() == :prod,
      releases: releases(),
      elixirc_paths: elixirc_paths(Mix.env()),
      # The test apps under test/apps/ are projects of their own with mix.exs
      # and lib/*.ex; keep `mix test`'s test-file scan from mistaking them for
      # mis-named test files.
      test_ignore_filters: [&String.starts_with?(&1, "test/apps/")],
      deps: deps(),
      aliases: aliases(),
      docs: docs()
    ]
  end

  # `mix docs` renders the guides as a browsable site under `doc/`. The guides
  # carry ExDoc tabsets (`<!-- tabs-open -->`), which are inert HTML comments
  # anywhere else, so one file serves both GitHub and this.
  # `mermaid` code fences render as diagrams. GitHub draws them itself; ExDoc
  # needs the library, injected here so one fence serves both.
  # Picking a tab picks the same-labelled tab in every tabset on the page and
  # on later pages. Tabs switch without ExDoc's click, which focuses the tab and
  # would scroll to it; the scroll correction keeps the clicked tab in place
  # while tabsets above it change height.
  defp guide_scripts(:html) do
    """
    <script defer src="https://cdn.jsdelivr.net/npm/mermaid@10.2.3/dist/mermaid.min.js"></script>
    <script>
      let flezhaMermaidReady = false;

      window.addEventListener("exdoc:loaded", () => {
        if (!flezhaMermaidReady) {
          mermaid.initialize({
            startOnLoad: false,
            theme: document.body.className.includes("dark") ? "dark" : "default"
          });
          flezhaMermaidReady = true;
        }

        let id = 0;

        for (const codeEl of document.querySelectorAll("pre code.mermaid")) {
          const preEl = codeEl.parentElement;
          const graphEl = document.createElement("div");

          mermaid.render("mermaid-graph-" + id++, codeEl.textContent).then(({svg, bindFunctions}) => {
            graphEl.innerHTML = svg;
            bindFunctions?.(graphEl);
            preEl.insertAdjacentElement("afterend", graphEl);
            preEl.remove();
          });
        }
      });
    </script>
    <script>
      {
        const storageKey = "flezha-tab";
        const pages = ["controller_deployment", "runner_deployment", "running_operators", "sources_and_sinks", "stop_everything"];

        const selectTab = (label, except) => {
          for (const tab of document.querySelectorAll(".tabset-tab")) {
            if (tab === except || tab.textContent.trim() !== label) continue;

            for (const other of tab.parentNode.children) {
              const selected = other === tab;
              const panel = document.getElementById(other.getAttribute("aria-controls"));
              other.setAttribute("aria-selected", selected);
              other.tabIndex = selected ? 0 : -1;
              panel.hidden = !selected;
              panel.tabIndex = selected ? 0 : -1;
            }
          }
        };

        // ExDoc swaps pages in place, so the page is checked on every event.
        const onSyncedPage = () => pages.includes(location.pathname.split("/").pop().replace(/\\.html$/, ""));

        const syncTabs = (event) => {
          const tablist = event.target.closest?.(".tabset-tablist");
          if (!tablist || !onSyncedPage()) return;

          const selected = tablist.querySelector("[aria-selected=true]");
          const label = selected.textContent.trim();
          const top = selected.getBoundingClientRect().top;
          selectTab(label, selected);
          window.scrollBy(0, selected.getBoundingClientRect().top - top);
          try { localStorage.setItem(storageKey, label); } catch {}
        };

        document.addEventListener("click", syncTabs);
        document.addEventListener("keydown", syncTabs);

        // ExDoc builds the tabsets in its own `exdoc:loaded` listener, which runs after this one.
        window.addEventListener("exdoc:loaded", () => setTimeout(() => {
          let label = null;
          try { label = localStorage.getItem(storageKey); } catch {}
          if (label && onSyncedPage()) selectTab(label);
        }));
      }
    </script>
    """
  end

  defp guide_scripts(:epub), do: ""

  # A guide step is a one-paragraph blockquote classed `{: .step}`, right above
  # the command or code it introduces, saying what that block does. The `:has`
  # on the paragraph's rule outranks ExDoc's padding under a blockquote's last
  # paragraph. A command's output goes in an `output` fence of its own, drawn as
  # the lower half of the command's block, so the command's copy button copies
  # the command alone. ExDoc highlights an `output` fence in the page's
  # language, so it is left uncoloured.
  defp guide_styles(:html) do
    """
    <style>
      .content-inner blockquote:has(> p.step) {
        margin: var(--baseLineHeight) 0 0.5rem;
        padding: 0.7rem 1rem;
        background-color: hsl(250, 68%, 97%);
        border-left: 4px solid var(--main);
        border-radius: 0 var(--borderRadius-base) var(--borderRadius-base) 0;
      }
      body.dark .content-inner blockquote:has(> p.step) {
        background-color: hsl(250, 25%, 15%);
      }
      .content-inner blockquote:has(> p.step) > p.step {
        position: relative;
        margin: 0;
        padding: 0 0 0 1.7rem;
      }
      .content-inner blockquote > p.step::before {
        content: "";
        position: absolute;
        left: 0;
        top: 0.15em;
        width: 1.15rem;
        height: 1.15rem;
        background-color: var(--main);
        mask: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Cpath d='M3 17.25V21h3.75L17.81 9.94l-3.75-3.75zM20.71 7.04a1 1 0 0 0 0-1.41l-2.34-2.34a1 1 0 0 0-1.41 0l-1.83 1.83 3.75 3.75z'/%3E%3C/svg%3E") no-repeat center / contain;
      }
      .content-inner pre:has(+ pre > code.output) {
        margin-bottom: 0;
      }
      .content-inner pre:has(+ pre > code.output) > code {
        border-bottom-left-radius: 0;
        border-bottom-right-radius: 0;
      }
      .content-inner pre:has(> code.output) {
        margin-top: -1px;
      }
      .content-inner pre code.output {
        margin: 0;
        border-top-style: dashed;
        border-top-left-radius: 0;
        border-top-right-radius: 0;
      }
      .content-inner pre code.output::before {
        content: none;
      }
      .content-inner pre code.output span {
        color: inherit;
      }
      .content-inner pre:has(> code.output) .copy-button {
        display: none;
      }
    </style>
    """
  end

  defp guide_styles(:epub), do: ""

  defp docs do
    [
      main: "what_is_flezha",
      extras: [
        "docs/guides/what_is_flezha.md",
        "docs/guides/guides.md",
        "docs/guides/controller_deployment.md",
        "docs/guides/runner_deployment.md",
        "docs/guides/running_operators.md",
        "docs/guides/sources_and_sinks.md",
        "docs/guides/stop_everything.md",
        "docs/guides/cli.md",
        "docs/guides/tradedesk/authorization.md",
        "docs/guides/tradedesk/namespaces.md",
        "docs/guides/tradedesk/building_an_etl_stream.md",
        "docs/guides/tradedesk/building_a_workflow.md",
        "docs/guides/tradedesk/partitioning.md",
        "docs/guides/tradedesk/plugins.md",
        "docs/guides/tradedesk/durability.md",
        "docs/guides/advanced/clustering.md",
        "docs/guides/advanced/custom_plugins.md",
        "docs/guides/advanced/full_form_tasklets.md",
        "docs/guides/advanced/effectively_once.md",
        "docs/guides/advanced/erlang_vm_tuning.md",
        "docs/guides/advanced/elixir_applications.md",
        "docs/guides/advanced/kubernetes_deployment.md",
        "docs/quickstart.md",
        "docs/sdk/admin.md",
        "docs/plugins/file.md",
        "docs/plugins/http_poll.md",
        "docs/plugins/websocket_subscription.md",
        "docs/plugins/kafka_producer.md",
        "docs/plugins/postgres_sink.md",
        "docs/plugins/operator_forward.md",
        "docs/plugins/installation.md",
        "docs/troubleshooting/controller_will_not_start.md",
        "docs/troubleshooting/runner_cannot_connect.md",
        "docs/configuration.md",
        "docs/operations/authn_authz.md"
      ],
      before_closing_head_tag: &guide_styles/1,
      before_closing_body_tag: &guide_scripts/1,
      groups_for_extras: [
        "Guide Part 0: Start here": [
          "docs/guides/what_is_flezha.md",
          "docs/guides/guides.md"
        ],
        "Guide Part 1: First steps": [
          "docs/guides/controller_deployment.md",
          "docs/guides/runner_deployment.md",
          "docs/guides/running_operators.md",
          "docs/guides/sources_and_sinks.md",
          "docs/guides/stop_everything.md"
        ],
        "Guide Part 2: Going further": [
          "docs/guides/tradedesk/authorization.md",
          "docs/guides/tradedesk/namespaces.md",
          "docs/guides/tradedesk/building_an_etl_stream.md",
          "docs/guides/tradedesk/building_a_workflow.md",
          "docs/guides/tradedesk/partitioning.md",
          "docs/guides/tradedesk/plugins.md",
          "docs/guides/tradedesk/durability.md"
        ],
        "Guide Part 3: Advanced topics": [
          "docs/guides/advanced/clustering.md",
          "docs/guides/advanced/custom_plugins.md",
          "docs/guides/advanced/full_form_tasklets.md",
          "docs/guides/advanced/effectively_once.md",
          "docs/guides/advanced/erlang_vm_tuning.md",
          "docs/guides/advanced/elixir_applications.md",
          "docs/guides/advanced/kubernetes_deployment.md"
        ],
        SDK: ~r"docs/sdk/",
        Plugins: ~r"docs/plugins/",
        Troubleshooting: ~r"docs/troubleshooting/",
        Reference: ~r"docs/(quickstart|configuration|operations|guides/cli)"
      ]
    ]
  end

  # Fence names whose blocks need a lexer other than the one registered for
  # them. ExDoc's shell lexer only keeps a `$ ` prompt from being selected,
  # makeup_syntect's `php` grammar reads only what follows a `<?php` tag, which
  # no snippet here has, `proto` is registered by file extension alone, and a
  # Ruby class name takes three colours by where it stands. ExDoc and
  # makeup_syntect register theirs when ExDoc starts, so it is started here
  # first: the docs task's own start is then a no-op and these stand.
  defp register_highlighters(_args) do
    Mix.Task.run("loadpaths")
    Code.require_file("scripts/docs_lexer.exs")
    {:ok, _} = Application.ensure_all_started(:ex_doc)

    for {names, language} <- [
          {["shell", "console", "sh", "bash", "zsh"], "Bourne Again Shell (bash)"},
          {["php"], "PHP Source"},
          {["proto"], "Protocol Buffer"},
          {["ruby"], "Ruby"}
        ] do
      Makeup.Registry.register_lexer(Flezha.Docs.Lexer,
        options: [language: language],
        names: names,
        extensions: []
      )
    end
  end

  # The testnet is a git submodule, so a clone made without
  # `--recurse-submodules` leaves its directory empty.
  defp testnet_checked_out(_args) do
    unless File.exists?("test/apps/testnet/package.json") do
      Mix.raise("test/apps/testnet is empty: run `git submodule update --init test/apps/testnet`")
    end
  end

  # Aliases are invoked via `mix <alias>`.
  defp aliases do
    [
      # The docs, with the code block lexers `register_highlighters/1` names.
      docs: [&register_highlighters/1, "docs"],
      # Build the WebSocket testnet bundle that several `mix test` cases
      # reach for over WS (`flezha_test.exs` "websocket plugin connection
      # start", "beam msg forward plugin connection start", "operator flow
      # generation -> beam msg forward"). The bundle output at
      # `test/apps/testnet/dist/index.js` is not in git; without this
      # alias those cases skip with a clear message rather than crash
      # inside the WS plugin connect path.
      "test.setup": [
        &testnet_checked_out/1,
        "cmd npm --prefix test/apps/testnet install",
        "cmd npm --prefix test/apps/testnet run build"
      ],
      # Opt-in dev-compose smoke test for the connection-healing change.
      # Brings up the controller + rust runner container, restarts each in turn,
      # asserts the runner <-> controller link re-establishes within
      # REJOIN_TIMEOUT_S seconds. Not part of `mix test` because it needs
      # Docker on the host. See test/scripts/connection_healing.sh.
      "test.connection_healing": [
        "cmd test/scripts/connection_healing.sh",
        "cmd test/scripts/plugin_persistence.sh"
      ],
      # Opt-in dev-compose smoke test for `ship-fresh-state-on-sdk-shutdown`.
      # Boots the controller + rust runner, starts an operator, sends SIGTERM
      # to the rust container, then asserts the controller's
      # `Overwatch.state.snapshot` was populated by the SDK's
      # `RunnerShutdownNotification`. Both periodic-enabled and
      # disabled-snapshot scenarios. Needs Docker on the host. See
      # test/scripts/state_freshness.sh.
      "test.state_freshness": ["cmd test/scripts/state_freshness.sh"]
    ]
  end

  def application do
    [extra_applications: extra_applications(Mix.env()), mod: {Flezha.Application, []}]
  end

  # :wx + :observer are dev-only — they pull wxWidgets shared libs the
  # headless prod release has no use for.
  defp extra_applications(:prod),
    do: [:logger, :runtime_tools, :inets, :crypto, :ssl, :telemetry]

  defp extra_applications(_),
    do: [:logger, :runtime_tools, :wx, :observer, :inets, :crypto, :ssl, :telemetry]

  defp releases() do
    [
      flezha: [
        include_executables_for: [:unix],
        include_erts: true,
        steps: [:assemble, &copy_release_extras/1]
      ]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # The Elixir SDK, developed in the `sdk/elixir` submodule. The controller
      # implements its in-node transport and shares its pure modules.
      {:flezha_sdk, path: "sdk/elixir"},
      # The Elixir test app's bench factories, which the in-process bench and
      # the in-node tests register. Compiled, never started, in no release.
      {:elixir_runner, path: "sdk/elixir/test_app", runtime: false},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.0"},
      # Pinned to the dev branch until 0.7.1 is published: 0.7.0 ignores
      # `cluster_settle_ms` without a word and restarts a replayed row with no
      # metadata. A path dep could not reach the release build, which runs in a
      # container that mounts no sibling checkout, so a release silently shipped
      # 0.7.0. Back to `~> 0.7.0` once the release is on hex.
      {:process_hub, github: "alfetahe/process-hub", branch: "dev"},
      # process_hub declares elector `runtime: false`, which keeps it out of
      # every release. Declared here so the namespace hubs' leader election
      # reaches the assembled release.
      {:elector, "~> 0.3.4"},
      {:blockade, "~> 0.2.2"},
      # fresh 0.4.4's released mix.exs declares charlist elixirc_paths,
      # which Mix >= 1.20 rejects; upstream is unresponsive. Pinned to the
      # PR branch carrying the fix. Pin the branch head, not a fork's default
      # branch commit: the previous pin was rewritten away upstream and no
      # ref reached it any more, so a fresh clone could not resolve deps.
      {:fresh, github: "josevalim/fresh", ref: "d8159bf148df51a15d91c432bf2120ff56d384a0"},
      {:kafka_ex, github: "alfetahe/kafka_ex", ref: "0764363353dc2ffc9996bdc8af11b9fe00561a57"},
      # Compiled in unconditionally; active only when the Postgres sink
      # plugin is configured (same posture as :kafka_ex).
      {:postgrex, "~> 0.22"},
      {:protox, "~> 2.0"},
      {:mimic, "~> 1.11"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      # Syntax highlighting for the guides' code blocks. Makeup ships lexers
      # for Elixir and Erlang only; this one carries the rest.
      {:makeup_syntect, "~> 0.1", only: :dev, runtime: false},
      {:logger_file_backend, "~> 0.0.13"},
      {:logger_backends, "~> 1.0"},
      {:rustler, "~> 0.35"},
      {:recon, "~> 2.5"},
      {:phoenix_pubsub, "~> 2.1"},
      # Hoisted to a top-level dep (already transitively present via
      # :mint_web_socket) so the OTLP exporter calls Mint directly.
      {:mint, "~> 1.7"},
      # IANA time zone database for the schedule evaluator (pure Elixir,
      # zero runtime deps; passed explicitly as Tz.TimeZoneDatabase).
      {:tz, "~> 0.28"}
    ]
  end

  # TODO: Also copy windows binaries
  defp copy_release_extras(release) do
    env =
      case Mix.env() do
        :prod -> "release"
        _ -> "debug"
      end

    File.cp!("target/#{env}/flz", Path.join(release.path, "bin/flz"))
    File.rename!(Path.join(release.path, "bin/flezha"), Path.join(release.path, "bin/flzvm"))

    for file <- ~w(LICENSE NOTICE THIRD_PARTY_NOTICES.md),
        do: File.cp!(file, Path.join(release.path, file))

    File.cp_r!("LICENSES", Path.join(release.path, "LICENSES"))

    announce_binaries(release)
  end

  # Mix's own closing message names `bin/<release name>`, which the rename
  # above has just moved to `bin/flzvm`, so it hands the user a path that
  # does not exist. `:quiet` suppresses it (announcing is all Mix has left to
  # do once the steps return) and this prints the two real binaries instead.
  defp announce_binaries(release) do
    path = Path.relative_to_cwd(release.path)

    if !release.options[:quiet] do
      Mix.shell().info("""

      Release created at #{path}

          # To start the controller
          #{path}/bin/flzvm start

          # The user CLI
          #{path}/bin/flz --help

      Once the controller is running:

          # To connect to it remotely
          #{path}/bin/flzvm remote

          # To stop it gracefully (SIGTERM drains too; SIGINT stops it at once)
          #{path}/bin/flzvm stop

      To list all controller commands:

          #{path}/bin/flzvm
      """)
    end

    %{release | options: Keyword.put(release.options, :quiet, true)}
  end

  # Explicit test dirs (not a recursive "test" glob) so a test app under
  # test/apps/ is never compiled into the controller as source.
  defp elixirc_paths(:prod), do: ["lib"]
  defp elixirc_paths(:test), do: ["lib", "test/fixtures", "test/helpers"]
  defp elixirc_paths(_), do: ["lib", "test/fixtures", "test/helpers"]
end
