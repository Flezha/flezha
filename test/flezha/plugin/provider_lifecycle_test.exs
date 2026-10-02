defmodule Flezha.Plugin.ProviderLifecycleTest do
  @moduledoc """
  The seam: a provider is one declaration, and the extension table drives every
  phase from it.

  Six clauses. Five read the tree and fail naming a file and a line, because the
  scatter this change closed is the kind that comes back one site at a time: a
  probe put back, a phase literal read somewhere else, a worker started inside a
  provider, a default re-derived in a worker, a hand-built dead-letter entry.
  The sixth drives the whole lifecycle over EVERY row of the provider table, so
  a provider that stops honouring the contract fails here rather than in
  production.
  """

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.Providers
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions
  alias Flezha.TestHelpers.PluginPackage

  alias __MODULE__.ConformanceWorker

  @catalog Host.build_catalog(
             [
               {:on_input, :transform, Ctx.IO},
               {:on_output, :transform, Ctx.IO},
               {:on_interval, :timer, Ctx.Tick},
               {:on_message, :request, Ctx.Msg},
               {:on_terminate, :fanout, Ctx.Terminate}
             ],
             %{file: __ENV__.file, line: __ENV__.line}
           )

  # Every provider file, and the two host files that used to probe them.
  @provider_dir "lib/flezha/plugin/provider"
  @probe_roots [
    "lib/flezha/plugin",
    "lib/flezha/operator/plugins.ex",
    "lib/flezha/cli/plugin.ex"
  ]

  # The table owns the phase vocabulary. This file is the seam; every other file
  # reads a predicate.
  @phase_seam ["lib/flezha/plugin/extension/table.ex"]

  # The two modules design §4 names as NOT this lifecycle: the shared broker
  # client registry keys connections rather than plugins, and the supervisor
  # takes its own name. Both read keyword opts that are not provider options.
  @not_the_lifecycle [
    "lib/flezha/plugin/provider/worker_sup.ex",
    "lib/flezha/plugin/provider/kafka/worker_registry.ex"
  ]

  # What stands in a table for a binding that could not come up: no user
  # enables it, so it is no row.
  @not_a_row [Flezha.Plugin.Provider.NotStarted]

  describe "clause 1: every provider is one declaration and one row" do
    test "every module that uses the behaviour is a row of the provider table" do
      declared = Providers.all() |> Map.values() |> Enum.concat(@not_a_row) |> MapSet.new()

      missing =
        for path <- Path.wildcard("#{@provider_dir}/**/*.ex"),
            module <- modules_in(File.read!(path)),
            Code.ensure_loaded?(module),
            function_exported?(module, :__provider__, 0),
            not MapSet.member?(declared, module),
            do: "#{path}: #{inspect(module)}"

      assert missing == [],
             "these providers use the behaviour but are no row of Flezha.Plugin.Providers, " <>
               "so nothing can enable, list or drill them:\n#{Enum.join(missing, "\n")}"
    end

    test "every row answers a declaration, and every on_*/2 it defines is a real extension" do
      known =
        MapSet.new(
          ProviderBehaviour.extension_callbacks() ++ ProviderBehaviour.controller_callbacks()
        )

      for {id, provider} <- Providers.all() do
        assert %ProviderBehaviour.Decl{module: ^provider} = decl = provider.__provider__(),
               "provider `#{id}` answers no declaration"

        # The declaration's `extensions` are derived from what the module
        # defines, so those two cannot disagree. What CAN go wrong is a NAME: a
        # function that looks like an extension callback and is one letter off
        # is dispatched by nothing and refused by nothing.
        misnamed =
          for {name, 2} <- provider.__info__(:functions),
              String.starts_with?(Atom.to_string(name), "on_"),
              not MapSet.member?(known, name),
              do: name

        assert misnamed == [],
               "provider `#{id}` (#{inspect(provider)}) defines #{inspect(misnamed)}/2, which " <>
                 "names no extension point, so nothing will ever dispatch to it"

        assert MapSet.subset?(MapSet.new(decl.extensions), known),
               "provider `#{id}` declares an extension the host does not offer"
      end
    end

    test "every declared option row is well formed" do
      for {id, provider} <- Providers.all(), {name, row} <- provider.__provider__().options do
        assert is_atom(name), "provider `#{id}` declares a non-atom option #{inspect(name)}"

        assert Keyword.has_key?(row, :type),
               "provider `#{id}` option `#{name}` declares no type, so nothing converts it"

        if row[:type] == :enum do
          assert is_list(row[:values]) and row[:values] != [],
                 "provider `#{id}` option `#{name}` is an enum with no values"
        end

        unless Keyword.get(row, :internal, false) do
          assert is_binary(row[:doc]) and row[:doc] != "",
                 "provider `#{id}` option `#{name}` is user-facing and undocumented"
        end
      end
    end
  end

  describe "clause 2: nothing probes a provider's exports" do
    test "no function_exported? on a provider survives in the plugin host" do
      offenders =
        for root <- @probe_roots,
            path <- source_files(root),
            {line, number} <- numbered(path),
            String.contains?(line, "function_exported?"),
            do: "#{path}:#{number}: #{String.trim(line)}"

      assert offenders == [],
             "a provider's shape is its declaration, not an export probe:\n" <>
               Enum.join(offenders, "\n")
    end
  end

  describe "clause 3: no phase literal outside the table" do
    test "the plugin phase vocabulary lives in the extension table alone" do
      offenders =
        for path <- source_files("lib/flezha"),
            path not in @phase_seam,
            {line, number} <- numbered(path),
            code = strip_comment(line),
            Regex.match?(~r/[=!]==?\s*:(prepared|running)\b|:(prepared|running)\s*->/, code),
            do: "#{path}:#{number}: #{String.trim(line)}"

      assert offenders == [],
             "read Table.running?/2, Table.subscribed?/2 or Table.phase/2 instead of the " <>
               "atom, or a fifth notion of running comes back:\n" <> Enum.join(offenders, "\n")
    end
  end

  describe "clause 4: one worker shape" do
    test "no supervisor or ensure call lives beside the one worker shape" do
      offenders =
        for path <- source_files(@provider_dir),
            path not in @not_the_lifecycle,
            {line, number} <- numbered(path),
            code = strip_comment(line),
            Regex.match?(~r/DynamicSupervisor|ensure_worker\(/, code),
            do: "#{path}:#{number}: #{String.trim(line)}"

      assert offenders == [],
             "the table starts every provider worker, through WorkerSup, from the " <>
               "declaration:\n" <> Enum.join(offenders, "\n")
    end

    test "no provider starts a process inside init_provider/1 or activate/1" do
      offenders =
        for path <- source_files(@provider_dir),
            {line, number} <- callback_bodies(path, ["init_provider", "activate"]),
            code = strip_comment(line),
            Regex.match?(~r/\bstart(_link)?\(|spawn(_link|_monitor)?\(/, code),
            do: "#{path}:#{number}: #{String.trim(line)}"

      assert offenders == [],
             "preparation validates and activation acquires; the process is the table's, " <>
               "started from the declared worker:\n" <> Enum.join(offenders, "\n")
    end

    test "every declared worker module can be started with the spec it is given" do
      for {id, provider} <- Providers.all(), worker = provider.__provider__().worker do
        assert Code.ensure_loaded?(worker.module),
               "provider `#{id}` declares worker #{inspect(worker.module)}, which does not load"

        assert function_exported?(worker.module, :start_link, 1),
               "provider `#{id}`'s worker #{inspect(worker.module)} exports no start_link/1"

        assert worker.restart in [:permanent, :transient, :temporary],
               "provider `#{id}` declares restart #{inspect(worker.restart)}"
      end
    end
  end

  describe "clause 5: one default, and the docs carry it" do
    test "no worker re-reads an option with a default of its own" do
      settings = MapSet.new(Enum.flat_map(Providers.all(), &option_names/1))

      offenders =
        for path <- source_files(@provider_dir),
            path not in @not_the_lifecycle,
            {line, number} <- numbered(path),
            key <- option_reads(strip_comment(line)),
            MapSet.member?(settings, key),
            do: "#{path}:#{number}: #{String.trim(line)}"

      assert offenders == [],
             "a worker reads declared fields; an option re-read with a default of its own " <>
               "is a second home for it:\n" <> Enum.join(offenders, "\n")
    end

    test "every provider's page carries the option table its declaration renders" do
      for {id, provider} <- Providers.all(), page = page_for(provider) do
        assert File.exists?(page),
               "provider `#{id}` has no page at #{page}; its options are documented nowhere"

        rendered = ProviderOptions.render_options_table(provider)

        assert documented_table(page, provider) == rendered,
               "#{page}'s option table has drifted from #{inspect(provider)}'s declaration.\n" <>
                 "Replace the block between the provider-options markers with:\n\n" <> rendered
      end
    end
  end

  describe "clause 6: the same drill over every row" do
    # A file plugin resolves its path inside the root folder when it is prepared.
    setup %{tmp: tmp} do
      Application.put_env(:flezha, :file_plugin_root, tmp)
      on_exit(&Providers.reset_installed/0)
      :ok
    end

    test "prepare holds no process, activation starts the declared worker, disable leaves none" do
      for {id, provider} <- Providers.all() do
        drill(id, provider)
      end
    end
  end

  describe "the same drill over a package's row" do
    setup %{tmp: tmp} do
      Application.put_env(:flezha, :file_plugin_root, tmp)
      root = Path.join(tmp, "plugins")
      on_exit(fn -> PluginPackage.unload!(root) end)
      [%{status: :loaded}] = PluginPackage.load!(root, [PluginPackage.example!()])
      :ok
    end

    test "the example package's provider honours the contract a first-party one does" do
      assert {:ok, provider} = Providers.fetch("example_stamp")
      drill("example_stamp", provider)
    end
  end

  describe "clause 7: one dead-letter entry constructor" do
    test "no site hands the store a map it built by hand" do
      offenders =
        for path <- source_files("lib/flezha"),
            {line, number} <- numbered(path),
            Regex.match?(~r/(DlqStore|Store)\.append(_async)?\(%\{/, line),
            do: "#{path}:#{number}: #{String.trim(line)}"

      assert offenders == [],
             "build it through Flezha.Dlq.Entry.new/1, which checks the required " <>
               "fields:\n" <> Enum.join(offenders, "\n")
    end

    test "the constructor refuses an entry that could not be listed or attributed" do
      assert {:error, {:dlq_entry_missing, [:cell_key]}} =
               Flezha.Dlq.Entry.new(%{
                 operator_class: "c",
                 tasklet_id: "p",
                 channel: "ch",
                 error: "boom"
               })

      assert {:ok, %Flezha.Dlq.Entry{error: "{:boom, 1}"}} =
               Flezha.Dlq.Entry.new(%{
                 operator_class: "c",
                 tasklet_id: "p",
                 cell_key: "k",
                 channel: "ch",
                 error: {:boom, 1}
               })
    end
  end

  ## The drill

  # One wrapper per row: the real declaration and the real `init_provider/1`,
  # with the declared worker replaced by a fake that can be told to fail its
  # first start. Wrapping rather than reaching into the table is what keeps the
  # production path free of a test-only branch.
  defp drill(id, provider) do
    wrapper = wrapper_for(provider)
    row_id = "drill-#{:erlang.phash2(provider)}"
    Providers.add(row_id, wrapper)
    start_supervised!({WorkerSup, name: WorkerSup.family(wrapper)}, id: wrapper)

    events = attach_phase_events()
    table = Table.new(catalog(provider))
    plugin = plugin_for(wrapper, provider, row_id)

    assert {:ok, table} = Table.bulk_subscribe(table, [{row_id, plugin}]),
           "provider `#{id}` refused to prepare"

    assert Table.phase(table, row_id) == :prepared, "provider `#{id}` did not reach :prepared"
    refute Table.running?(table, row_id)
    assert Table.subscribed?(table, row_id)

    assert Table.workers(table, row_id) == %{},
           "provider `#{id}` holds a process while :prepared"

    # Activation fails once: the phase stays :prepared and the reason is returned.
    ConformanceWorker.fail_next()
    {table, failed} = Table.bulk_activate(table, %{})

    assert [{^row_id, _reason}] = failed,
           "provider `#{id}` reported no failure for a worker that refused to start"

    assert Table.phase(table, row_id) == :prepared

    # Activation succeeds: the worker is registered under the declared family.
    {table, []} = Table.bulk_activate(table, %{})
    assert Table.running?(table, row_id), "provider `#{id}` did not reach :running"

    family = WorkerSup.family(wrapper)
    assert [_ | _] = keys = WorkerSup.keys(family)
    assert map_size(Table.workers(table, row_id)) == length(keys)
    assert is_pid(Table.owned_pid(table, row_id))

    assert Table.health(table)[row_id] == :healthy,
           "provider `#{id}` is unhealthy on a live worker"

    assert Table.uptimes(table)[row_id] >= 0

    # Disable: the family registry is emptied and nothing is left behind.
    table = Table.unsubscribe(table, row_id)
    assert Table.phase(table, row_id) == :unknown
    assert WorkerSup.keys(family) == [], "provider `#{id}` left a worker registered after disable"

    # Every transition was one event, from the one transition function.
    assert [{:prepared, _} | _] = seen = collect_phase_events(events)

    assert Enum.any?(seen, &match?({:running, _}, &1)),
           "provider `#{id}` reached :running without emitting the event"

    _ = Table.teardown_all(table)
  end

  # A controller plugin's hooks are the controller catalogue's.
  defp catalog(provider) do
    if provider.plugin_type() == :controller,
      do: Flezha.Plugin.Shared.__extensions_catalog__(),
      else: @catalog
  end

  defp plugin_for(wrapper, provider, row_id) do
    %Plugin{
      id: row_id,
      provider: wrapper,
      namespace: :default,
      operator_id: "drill-op",
      opts: minimal_opts(provider),
      hooks: [subscription(hd(wrapper.__provider__().extensions))]
    }
  end

  # A request subscription is keyed and a timer subscription is scheduled; the
  # rest take no sub-options.
  defp subscription(:on_message), do: {:on_message, [key: "drill"]}
  defp subscription(:on_interval), do: {:on_interval, [interval_ms: 60_000]}
  defp subscription(name), do: {name, []}

  # The smallest option map each provider's own `init_provider/1` accepts. A
  # provider whose required options this map does not name fails clause 1's row
  # check first, so this stays a lookup rather than a guess.
  defp minimal_opts(provider) do
    base =
      for {name, row} <- provider.__provider__().options,
          Keyword.get(row, :required, false),
          into: %{},
          do: {name, sample(name, row)}

    Map.merge(base, extra_opts(provider))
  end

  defp sample(:uri, _row), do: "wss://example.invalid/feed"
  defp sample(:url, _row), do: "https://example.invalid/feed"
  defp sample(:recv_pid, _row), do: self()
  defp sample(:namespace, _row), do: :default
  defp sample(:hosts, _row), do: [{"localhost", 9092}]
  defp sample(name, row), do: sample_by_type(name, Keyword.get(row, :type, :any))

  defp sample_by_type(_name, :int), do: 1
  defp sample_by_type(_name, :bool), do: false
  defp sample_by_type(_name, :list), do: []
  defp sample_by_type(_name, :map), do: %{}
  defp sample_by_type(name, _type), do: "drill-#{name}"

  # The providers whose `init_provider/1` refuses the minimal map: a forward must
  # not target its own operator, a poll's request must fit inside its own
  # interval, a poll and a file source must name the entry channel they
  # inject on, and a table poll reads names that are identifiers.
  defp extra_opts(Flezha.Plugin.Provider.OperatorForward),
    do: %{target_operator_id: "drill-target"}

  defp extra_opts(Flezha.Plugin.Provider.HttpPoll),
    do: %{interval_ms: 60_000, timeout_ms: 1_000, channel: "drill-channel"}

  defp extra_opts(Flezha.Plugin.Provider.File.Source), do: %{channel: "drill-channel"}

  defp extra_opts(Flezha.Plugin.Provider.PostgresPoll),
    do: %{channel: "drill-channel", table: "drill_table", id_column: "id"}

  defp extra_opts(_provider), do: %{}

  defp page_for(provider) do
    case provider do
      Flezha.Plugin.Provider.Kafka.Producer -> "docs/plugins/kafka_producer.md"
      Flezha.Plugin.Provider.Postgres -> "docs/plugins/postgres_sink.md"
      Flezha.Plugin.Provider.HttpPoll -> "docs/plugins/http_poll.md"
      Flezha.Plugin.Provider.Websocket -> "docs/plugins/websocket_subscription.md"
      Flezha.Plugin.Provider.OperatorForward -> "docs/plugins/operator_forward.md"
      Flezha.Plugin.Provider.File.Source -> "docs/plugins/file.md"
      Flezha.Plugin.Provider.File.Sink -> "docs/plugins/file.md"
      Flezha.Plugin.Provider.HookWebhook -> "docs/plugins/hook_webhook.md"
      Flezha.Plugin.Provider.HttpPush -> "docs/plugins/http_push.md"
      Flezha.Plugin.Provider.PostgresPoll -> "docs/plugins/postgres_poll.md"
      _ -> nil
    end
  end

  @options_begin "<!-- provider-options:begin -->"
  @options_end "<!-- provider-options:end -->"

  # A page documenting two providers names each block after its provider; a page
  # documenting one opens its block with the bare marker.
  defp documented_table(page, provider) do
    text = File.read!(page)
    named = "<!-- provider-options:begin #{provider.__provider__().name} -->"

    text
    |> String.split(if String.contains?(text, named), do: named, else: @options_begin)
    |> Enum.at(1, "")
    |> String.split(@options_end)
    |> hd()
    |> String.trim_leading("\n")
  end

  ## Tree reading

  defp option_names({_id, provider}),
    do: Enum.map(provider.__provider__().options, &elem(&1, 0))

  # A read of `opts` that names an option AND supplies a fallback for it. A read
  # with no fallback holds no default, so it is not a second home for one.
  defp option_reads(code) do
    for [_, key] <- Regex.scan(~r/(?:Keyword\.get|Map\.get)\(opts, :(\w+),/, code),
        do: String.to_atom(key)
  end

  # The lines of a module's `def <name>(` bodies, from the definition to the
  # next top-level definition. Enough to say what a callback does, without
  # asking the file to be parsed twice.
  defp callback_bodies(path, names) do
    path
    |> numbered()
    |> Enum.reduce({[], false}, fn {line, number}, {acc, inside?} ->
      cond do
        Regex.match?(~r/^  (?:@impl \w+|@doc|@spec)/, line) -> {acc, inside?}
        Regex.match?(~r/^  defp? (#{Enum.join(names, "|")})\(/, line) -> {acc, true}
        Regex.match?(~r/^  defp? \w/, line) -> {acc, false}
        inside? -> {[{line, number} | acc], true}
        true -> {acc, inside?}
      end
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  defp source_files(root) do
    cond do
      String.ends_with?(root, ".ex") -> [root]
      true -> Path.wildcard("#{root}/**/*.ex")
    end
  end

  defp numbered(path) do
    path |> File.read!() |> String.split("\n") |> Enum.with_index(1)
  end

  # A rule about code is a rule about code: prose that names a phase, a probe or
  # a supervisor is documentation, and rewording it to please a regex is how a
  # gate starts costing more than it catches.
  defp strip_comment(line) do
    case String.split(line, "#", parts: 2) do
      [code, _comment] -> code
      [code] -> code
    end
  end

  defp modules_in(source) do
    for [_, name] <- Regex.scan(~r/^\s*defmodule\s+([A-Za-z0-9_.]+)\s+do/m, source),
        do: Module.concat([name])
  end

  ## Telemetry

  defp attach_phase_events do
    ref = make_ref()
    parent = self()

    :telemetry.attach(
      "drill-#{inspect(ref)}",
      Table.phase_event(),
      fn _event, _measure, meta, _config -> send(parent, {ref, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("drill-#{inspect(ref)}") end)
    ref
  end

  defp collect_phase_events(ref), do: collect_phase_events(ref, [])

  defp collect_phase_events(ref, acc) do
    receive do
      {^ref, meta} -> collect_phase_events(ref, [{meta.to, meta.reason} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  ## The wrapper providers

  defmodule ConformanceWorker do
    @moduledoc """
    The drill's worker: it starts, it stays up, and it refuses exactly once when
    the drill asks it to. Standing in for the real worker is what lets the drill
    run over every row without a broker, a database or a socket.
    """

    use GenServer

    @fail_key {__MODULE__, :fail_next}

    def fail_next, do: :persistent_term.put(@fail_key, true)

    def start_link(opts) when is_map(opts) do
      if :persistent_term.get(@fail_key, false) do
        :persistent_term.put(@fail_key, false)
        {:error, :refused_once}
      else
        GenServer.start_link(__MODULE__, opts, name: Map.fetch!(opts, :name))
      end
    end

    @impl true
    def init(opts), do: {:ok, opts}
  end

  # One per row, defined on first use: the real option rows and the real
  # `init_provider/1`, with the fake worker in place of the declared one. On
  # first use rather than at compile time, because a package's row exists only
  # once the package is loaded.
  defp wrapper_for(provider) do
    wrapper = Module.concat(__MODULE__, "Wrap#{:erlang.phash2(provider)}")
    unless Code.ensure_loaded?(wrapper), do: define_wrapper(wrapper, provider)
    wrapper
  end

  defp define_wrapper(wrapper, provider) do
    decl = provider.__provider__()

    callbacks =
      for callback <- decl.extensions do
        quote do
          @impl true
          def unquote(callback)(ctx, opts), do: unquote(provider).unquote(callback)(ctx, opts)
        end
      end

    body =
      quote do
        @moduledoc false

        use Flezha.Plugin.ProviderBehaviour,
          type: unquote(decl.type),
          options: unquote(Macro.escape(decl.options)),
          feeds: unquote(decl.feeds),
          worker: [
            module: Flezha.Plugin.ProviderLifecycleTest.ConformanceWorker,
            restart: :temporary
          ]

        @impl true
        def init_provider(opts), do: unquote(provider).init_provider(opts)

        @impl true
        def reconfigure(_old, _new), do: :ok

        unquote_splicing(callbacks)
      end

    {:module, ^wrapper, _, _} = Module.create(wrapper, body, Macro.Env.location(__ENV__))
  end
end
