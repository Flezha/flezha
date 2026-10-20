defmodule Flezha.Plugin.ProviderBehaviour.Declaration do
  @moduledoc """
  The macro half of the provider contract: what `use ProviderBehaviour` writes
  into a provider, and what `__before_compile__` reads back out of it.

  It is split from `Flezha.Plugin.ProviderBehaviour` because the two are
  different jobs at different times. The contract is a set of callbacks and the
  run-time helpers that honour them; this is the generator that turns one
  declaration into a struct, a strict allow-list, a conversion table, a help
  screen and a `%Decl{}`. Nothing outside a provider's own `use` line names this
  module.
  """

  alias Flezha.Observability.SignalWords
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Plugin.ProviderBehaviour.Options

  @lifecycle_callbacks ProviderBehaviour.lifecycle_callbacks()
  @extension_callbacks ProviderBehaviour.extension_callbacks()
  @controller_callbacks ProviderBehaviour.controller_callbacks()

  # Each type's hooks; a provider that defines the other type's is refused.
  @type_hooks %{relay: @extension_callbacks, controller: @controller_callbacks}

  @doc """
  Write the declaration into the provider: the attributes that hold it, the
  struct it derives, the contract functions it generates, and the default body
  for every lifecycle callback the provider does not write itself.

  `feeds: true` declares that the provider's own worker injects payloads into
  the relay. It defaults to false and only the providers whose worker calls
  `Flezha.Plugin.Host.inject/4` write it. `shareable: true` declares that one
  running copy can serve several bound relays; it defaults to false.
  `namespace: name` declares the one namespace the provider may be enabled in.

  `source: [...]` declares a source, a provider whose worker reads something
  outside and feeds the relay (it implies `feeds: true`), and generates what
  every source shares:

    * `init_provider/1` refuses a plugin naming no entry channel, then, with
      `partitioned: :refuse`, one on a partition group member (`:own_copy`
      serves a partitioned relay, one copy per member), fills `plugin_id` from
      the id the plugin was enabled under, and only then runs the provider's own;
    * `worker_specs/1`, one worker per binding started with the worker's
      `args/1`, and `health/1`, which probes a worker written with
      `Flezha.Plugin.Provider.Worker` at twice the probe budget
      (`{:fail_open, :probe_timeout}` when it does not answer) and judges its
      facts by the provider's own `verdict/2` clauses first, then
      `failure_threshold` failures in a row as `{failure_reason, last_error}`
      (`failure_reason:` defaults to `:read_failed`), then, with
      `idle_interval: <option>`, `ProviderBehaviour.idle_verdict/3` over that
      option. `absent:` and `dead:` name a missing and a dead worker
      (`:no_reader`, `:reader_dead`); `probe: false` leaves both callbacks to the
      provider.

  Every relay provider answers `on_terminate/2` with `:ok` unless it writes
  one, so a binding naming that hook binds whichever provider it names.
  """
  defmacro __using__(opts) do
    [
      attributes(opts),
      struct(),
      contract_functions(),
      lifecycle_defaults(),
      own_tracking()
    ]
  end

  # What the declaration IS, held as attributes `__before_compile__` reads back.
  defp attributes(opts) do
    contract = ProviderBehaviour
    options = Keyword.get(opts, :options, [])
    worker = Keyword.get(opts, :worker)
    type = Keyword.get(opts, :type, :relay)
    name = Keyword.get(opts, :name)
    source = Keyword.get(opts, :source)
    feeds = if source, do: true, else: Keyword.get(opts, :feeds, false)
    shareable = Keyword.get(opts, :shareable, false)
    telemetry = Keyword.get(opts, :telemetry, [])
    namespace = Keyword.get(opts, :namespace)

    quote do
      @behaviour unquote(contract)
      @before_compile unquote(__MODULE__)

      @__provider_options__ unquote(options)
      @__provider_worker__ unquote(__MODULE__).__worker_decl__(unquote(worker))
      @__provider_type__ unquote(type)
      @__provider_name__ unquote(name) || unquote(Options).provider_name(__MODULE__)
      @__provider_feeds__ unquote(feeds)
      @__provider_shareable__ unquote(shareable)
      @__provider_telemetry__ unquote(telemetry)
      @__provider_namespace__ unquote(namespace)
      @__provider_source__ unquote(source)
    end
  end

  # The struct the rows describe. It is generated HERE rather than in
  # `__before_compile__` because `defstruct` must precede any `%__MODULE__{}`
  # literal in the module body, and four providers pattern-match their own.
  defp struct do
    quote do
      @enforce_keys for {name, row} <- @__provider_options__,
                        Keyword.get(row, :required, false),
                        do: name

      defstruct Enum.map(@__provider_options__, fn {name, row} ->
                  if Keyword.get(row, :required, false), do: name, else: {name, row[:default]}
                end)
    end
  end

  # What every provider answers the host and the CLI with, derived from the rows.
  defp contract_functions do
    contract = ProviderBehaviour

    quote do
      @impl unquote(contract)
      def plugin_type(), do: @__provider_type__

      # `convert_value/2` is the provider's own coercion table for the shapes it
      # converts by hand; a capture expands in its context, so the shared builder
      # can call it and the injected last clause falls through to the rows.
      @impl unquote(contract)
      def build(string_map),
        do: unquote(Options).build_struct(__MODULE__, string_map, &convert_value/2)

      @impl unquote(contract)
      def help(provider_id), do: unquote(Options).render_help(__MODULE__, provider_id)

      @impl unquote(contract)
      def provider_info(), do: unquote(Options).describe(__MODULE__)

      @doc """
      Reject options this provider does not declare rather than ignoring them.
      `build/1` silently drops any key that is not a declared row, which would
      let a config ask for a capability the provider deliberately does not offer
      and read as accepted. `Plugin.SpecDecoder` hands a `--plugins` spec
      straight to `init_provider/1` without ever constructing the struct, so the
      check has to live on both paths.
      """
      @spec validate_opts(map()) :: :ok | {:error, term()}
      def validate_opts(opts) do
        unquote(contract).reject_unknown(
          Map.keys(opts),
          Enum.map(@__provider_options__, &elem(&1, 0)) ++ unquote(contract).host_opts(),
          @__provider_name__
        )
      end
    end
  end

  # A body for every lifecycle callback, so the host calls all of them and a
  # provider writes only what it actually does.
  defp lifecycle_defaults do
    contract = ProviderBehaviour

    quote do
      @impl unquote(contract)
      def init_provider(opts), do: {:ok, opts}

      @impl unquote(contract)
      def activate(_opts), do: :ok

      @impl unquote(contract)
      def worker_specs(opts), do: [{Flezha.Plugin.Provider.WorkerSup.key(opts), opts}]

      @impl unquote(contract)
      def health(opts), do: unquote(contract).default_health(opts)

      @impl unquote(contract)
      def inspect_state(_opts), do: nil

      @impl unquote(contract)
      def terminate_provider(_opts), do: :ok

      defoverridable build: 1,
                     help: 1,
                     plugin_type: 0,
                     provider_info: 0,
                     init_provider: 1,
                     activate: 1,
                     worker_specs: 1,
                     health: 1,
                     inspect_state: 1,
                     terminate_provider: 1
    end
  end

  # Set LAST, so the default bodies above do not trigger it: from here on, a
  # lifecycle definition is one the provider wrote itself. That is what
  # `Decl.own` means, and the conformance test reads it to find a callback a
  # provider declared and never needed.
  defp own_tracking do
    quote do
      Module.register_attribute(__MODULE__, :__provider_own__, accumulate: true)
      @on_definition {unquote(__MODULE__), :__on_definition__}
    end
  end

  @doc false
  def __on_definition__(env, :def, name, args, _guards, _body) do
    if {name, length(args)} in @lifecycle_callbacks do
      Module.put_attribute(env.module, :__provider_own__, name)
    end
  end

  def __on_definition__(_env, _kind, _name, _args, _guards, _body), do: :ok

  @doc false
  @spec __worker_decl__(keyword() | nil) :: Decl.worker() | nil
  def __worker_decl__(nil), do: nil

  def __worker_decl__(spec) when is_list(spec) do
    %{
      module: Keyword.fetch!(spec, :module),
      restart: Keyword.get(spec, :restart, :temporary),
      reusable: Keyword.get(spec, :reusable)
    }
  end

  @doc """
  Read the declaration back out of the finished module: what it implements, what
  it overrode, and the two clauses that can only be written once the provider's
  own are in place.
  """
  defmacro __before_compile__(env) do
    rows = Module.get_attribute(env.module, :__provider_options__) || []
    validate_rows!(env, rows)
    require_reconfigure!(env, rows)
    validate_type!(env, Module.get_attribute(env.module, :__provider_type__))
    validate_telemetry!(env, Module.get_attribute(env.module, :__provider_telemetry__))

    [
      decl(env.module),
      source_kit(env),
      default_terminate(env.module),
      strict_build(),
      fall_through()
    ]
  end

  # `extensions` and `own` are read from the module while it compiles, which is
  # the same source a run-time export probe reads and one the host can trust
  # without asking the code loader.
  defp decl(module) do
    extensions =
      Enum.filter(
        @extension_callbacks ++ @controller_callbacks,
        &(Module.defines?(module, {&1, 2}) or (&1 == :on_terminate and default_terminate?(module)))
      )

    own = module |> Module.get_attribute(:__provider_own__, []) |> Enum.uniq() |> Enum.sort()

    quote do
      @doc false
      @impl unquote(ProviderBehaviour)
      def __provider__ do
        %unquote(ProviderBehaviour.Decl){
          module: __MODULE__,
          type: @__provider_type__,
          name: @__provider_name__,
          options: @__provider_options__,
          extensions: unquote(extensions),
          own: unquote(own),
          worker: @__provider_worker__,
          telemetry: @__provider_telemetry__,
          feeds: @__provider_feeds__,
          shareable: @__provider_shareable__,
          namespace: @__provider_namespace__
        }
      end
    end
  end

  defp default_terminate?(module),
    do:
      Module.get_attribute(module, :__provider_type__) == :relay and
        not Module.defines?(module, {:on_terminate, 2})

  defp default_terminate(module) do
    if default_terminate?(module) do
      quote do
        @impl unquote(ProviderBehaviour)
        def on_terminate(_ctx, _opts), do: :ok
      end
    end
  end

  @source_keys [:partitioned, :failure_reason, :idle_interval, :absent, :dead, :probe]

  # What every source shares, generated from its `source:` declaration.
  defp source_kit(env) do
    case Module.get_attribute(env.module, :__provider_source__) do
      nil ->
        nil

      source ->
        validate_source!(env, source)
        own = Module.get_attribute(env.module, :__provider_own__, [])
        worker = Module.get_attribute(env.module, :__provider_worker__)[:module]
        probe? = Keyword.get(source, :probe, true)

        [
          source_init(source),
          if(probe? and :worker_specs not in own, do: source_specs(worker)),
          if(probe? and :health not in own, do: source_health(source, worker))
        ]
    end
  end

  defp validate_source!(env, source) do
    refuse_row_key!(env, :source, Keyword.keys(source) -- @source_keys)

    if source[:partitioned] not in [:refuse, :own_copy],
      do: raise_row!(env, "source: needs `partitioned: :refuse | :own_copy`")

    if Keyword.get(source, :probe, true) and
         is_nil(Module.get_attribute(env.module, :__provider_worker__)),
       do: raise_row!(env, "source: needs a `worker:` it can probe, or `probe: false`")
  end

  defp source_init(source) do
    partitioned =
      if source[:partitioned] == :refuse do
        quote do
          defp __source_partition__(%{partition_count: count} = opts) when not is_nil(count),
            do:
              {:error,
               {:partitioned_host, %{provider: @__provider_name__, relay: opts[:source_relay_id]}}}
        end
      end

    quote do
      unquote(partitioned)
      defp __source_partition__(_opts), do: :ok

      # The resolved `:entry_channel`, which either spelling names and a row's
      # `required:` cannot say.
      defp __source_entry__(opts) do
        if opts[:entry_channel],
          do: :ok,
          else: {:error, {:entry_channel_required, @__provider_name__}}
      end

      defp __source_plugin_id__(%{plugin_id: nil} = opts),
        do: %{opts | plugin_id: opts[:source_plugin_id]}

      defp __source_plugin_id__(opts), do: opts

      defoverridable init_provider: 1

      @impl unquote(ProviderBehaviour)
      def init_provider(opts) do
        with :ok <- __source_entry__(opts),
             :ok <- __source_partition__(opts),
             do: super(__source_plugin_id__(opts))
      end
    end
  end

  defp source_specs(worker) do
    quote do
      defoverridable worker_specs: 1

      @impl unquote(ProviderBehaviour)
      def worker_specs(opts),
        do: [{Flezha.Plugin.Provider.WorkerSup.key(opts), unquote(worker).args(opts)}]
    end
  end

  defp source_health(source, worker) do
    contract = ProviderBehaviour
    reason = Keyword.get(source, :failure_reason, :read_failed)
    last = source_last_verdict(source[:idle_interval])

    quote do
      defoverridable health: 1

      @impl unquote(contract)
      def health(opts) do
        unquote(contract).worker_health(
          opts,
          # The outer budget exceeds the probe's own, so the precise
          # `:probe_timeout` is not replaced by the generic budget verdict.
          [
            absent: unquote(Keyword.get(source, :absent, :no_reader)),
            dead: unquote(Keyword.get(source, :dead, :reader_dead)),
            budget_ms: 2 * unquote(contract).probe_budget_ms()
          ],
          fn pid ->
            case unquote(worker).probe(pid) do
              {:ok, facts} -> verdict(facts, opts)
              :timeout -> {:fail_open, :probe_timeout}
            end
          end
        )
      end

      defp verdict(%{consecutive_failures: failures, last_error: error}, opts)
           when failures >= opts.failure_threshold,
           do: {:unhealthy, {unquote(reason), error}}

      unquote(last)
    end
  end

  defp source_last_verdict(nil), do: quote(do: defp(verdict(_facts, _opts), do: :healthy))

  defp source_last_verdict(interval) do
    quote do
      defp verdict(%{last_settled_at: settled_at}, opts),
        do:
          unquote(ProviderBehaviour).idle_verdict(
            settled_at,
            opts.idle_after_ms,
            Map.fetch!(opts, unquote(interval))
          )
    end
  end

  # The CLI half of the strict-option contract, wrapped around whatever `build/1`
  # the provider ended up with.
  defp strict_build do
    quote do
      defoverridable build: 1

      def build(string_map) when is_map(string_map) do
        allowed =
          for {name, row} <- @__provider_options__,
              not Keyword.get(row, :internal, false),
              do: Atom.to_string(name)

        case unquote(ProviderBehaviour).reject_unknown(
               Map.keys(string_map),
               allowed,
               @__provider_name__
             ) do
          :ok -> super(string_map)
          err -> err
        end
      end
    end
  end

  # The one clause every provider's private table falls through to: the rows
  # decide, so a value none of the provider's own clauses matched is converted by
  # its declared type and refused by its declared bounds. A table that read as a
  # validator and answered `{:ok, anything}` is what KI-116 was.
  defp fall_through do
    quote do
      defp convert_value(field, value),
        do: unquote(Options).convert_row(field, value, @__provider_options__, @__provider_name__)

      @doc false
      @spec __convert__(atom(), term()) :: {:ok, term()} | {:error, term()}
      def __convert__(field, value), do: convert_value(field, value)
    end
  end

  @row_keys [:type, :default, :required, :min, :max, :values, :internal, :transient, :live, :doc]
  @row_types [:int, :bool, :string, :names, :enum, :pid, :json, :headers, :map, :list, :any]

  defp validate_rows!(env, rows) do
    Enum.each(rows, fn
      {name, row} when is_atom(name) and is_list(row) ->
        refuse_row_key!(env, name, Keyword.keys(row) -- @row_keys)
        refuse_row_type!(env, name, Keyword.get(row, :type, :any))

      other ->
        raise_row!(env, "option row #{inspect(other)} is not `{name, keyword()}`")
    end)
  end

  # A live option is applied by the provider's own `reconfigure/2`; without it
  # a change the declaration promises in place could only be dropped.
  defp require_reconfigure!(env, rows) do
    case for({name, row} <- rows, Keyword.get(row, :live, false), do: name) do
      [] ->
        :ok

      live ->
        unless Module.defines?(env.module, {:reconfigure, 2}),
          do: raise_row!(env, "live option(s) #{inspect(live)} need a reconfigure/2")
    end
  end

  defp validate_type!(env, type) when is_map_key(@type_hooks, type) do
    other = for {other, hooks} <- @type_hooks, other != type, hook <- hooks, do: hook

    case Enum.find(other, &Module.defines?(env.module, {&1, 2})) do
      nil -> :ok
      hook -> raise_row!(env, "a #{type} plugin cannot define #{hook}/2")
    end
  end

  defp validate_type!(env, type),
    do: raise_row!(env, "type #{inspect(type)} must be :relay or :controller")

  @verb_form "`verb: :metric | :event`, `verb: {:metric | :event, \"what it says\"}` or " <>
               "`verb: {:metric | :event, meaning: \"what it says\", label:, outcome:, sentence:}`"

  # An own verb that repeats a shared one would be a second account of the same
  # fact under a kind the vocabulary already fixes.
  defp validate_telemetry!(env, verbs) when is_list(verbs) do
    Enum.each(verbs, fn
      {verb, declared} when is_atom(verb) ->
        if declared_kind(declared) not in [:metric, :event],
          do: raise_row!(env, "telemetry entry #{inspect({verb, declared})} is not #{@verb_form}")

        if verb in ProviderBehaviour.verbs(),
          do: raise_row!(env, "telemetry verb #{inspect(verb)} is one of the shared verbs")

      other ->
        raise_row!(env, "telemetry entry #{inspect(other)} is not #{@verb_form}")
    end)
  end

  defp validate_telemetry!(env, other),
    do: raise_row!(env, "telemetry #{inspect(other)} is not a keyword list")

  # A verb's meaning is optional, so a package built before it existed loads.
  defp declared_kind({kind, meaning}) when is_binary(meaning),
    do: if(SignalWords.valid_word?(:meaning, meaning), do: kind)

  defp declared_kind({kind, words}) when is_list(words) do
    valid? =
      Keyword.keyword?(words) and Keyword.has_key?(words, :meaning) and
        Enum.all?(words, fn {key, value} -> SignalWords.valid_word?(key, value) end)

    if valid?, do: kind
  end

  defp declared_kind(kind) when is_atom(kind), do: kind
  defp declared_kind(_other), do: nil

  defp refuse_row_key!(_env, _name, []), do: :ok

  defp refuse_row_key!(env, name, unknown),
    do: raise_row!(env, "option #{inspect(name)} declares unknown key(s) #{inspect(unknown)}")

  defp refuse_row_type!(env, name, type) when type not in @row_types,
    do: raise_row!(env, "option #{inspect(name)} declares unknown type #{inspect(type)}")

  defp refuse_row_type!(_env, _name, _type), do: :ok

  defp raise_row!(env, description) do
    raise CompileError,
      file: env.file,
      line: env.line,
      description: "#{inspect(env.module)}: #{description}"
  end
end
