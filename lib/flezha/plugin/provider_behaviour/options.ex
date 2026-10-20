defmodule Flezha.Plugin.ProviderBehaviour.Options do
  @moduledoc """
  A provider's option rows, read.

  Everything that happens to a plugin's options between the spec a user
  wrote and the callback that reads them: the declared names and defaults, the
  one conversion table, the one refusal grammar, and the two renderings (the
  help screen and the documented table) that are derived from the same rows
  rather than written twice.

  The rows several providers take are declared here once, as fragments a
  declaration splices into its `options:`: the channel, `plugin_id`, health,
  queue and retry rows, and the credential row `secret/2`.

  `Flezha.Plugin.ProviderBehaviour` delegates its public surface here, so a
  provider and its callers still name one module.
  """

  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Plugin.ProviderBehaviour.Decl

  # Every row that can name the channel a plugin is attached to, with the job it
  # names: `channel` names whichever jobs the plugin has, the explicit pair one
  # each. Every reading below derives from this one table. A provider declares
  # only the spellings it can honour, so a spelling for a direction a provider
  # never works in is refused as an unknown option.
  @channel_rows [channel: :either, channel_in: :in, channel_out: :out]
  @explicit_key for {key, job} <- @channel_rows, job != :either, do: {job, key}
  @channel_keys Keyword.keys(@channel_rows)

  import ProviderBehaviour,
    only: [
      parse_bool: 2,
      parse_enum: 3,
      parse_int: 2,
      parse_pid: 2,
      reject_unknown: 3,
      refuse_value: 2
    ]

  @doc """
  `module`'s option names as `{required, all}`, both in declaration order;
  `required` is the rows marked `required: true`.

  The order is the declaration's, because this list is rendered into a
  user-facing help screen and into the order `build_struct/3` reports the
  first unconvertible value in.
  """
  @spec fields(module()) :: {[atom()], [atom()]}
  def fields(module) do
    rows = rows(module)

    {for({name, row} <- rows, Keyword.get(row, :required, false), do: name),
     Enum.map(rows, &elem(&1, 0))}
  end

  @doc "The option rows `module` declared, in declaration order."
  @spec rows(module()) :: [{atom(), keyword()}]
  def rows(module), do: module.__provider__().options

  @doc "Every declared option's default, by name."
  @spec defaults(module()) :: %{atom() => term()}
  def defaults(module), do: Map.new(rows(module), fn {name, row} -> {name, row[:default]} end)

  # ---------- rows several providers share ----------

  @queue_rows [
    queue_max: [
      type: :int,
      default: 10_000,
      min: 1,
      doc: "Outputs the queue holds before it is full"
    ],
    queue_max_bytes: [
      type: :int,
      default: 67_108_864,
      min: 1,
      doc: "Output bytes the queue holds before it is full"
    ],
    queue_overflow: [
      type: :enum,
      values: [:drop_nothing, :drop_oldest, :drop_newest],
      default: :drop_nothing,
      doc: "What a full queue does: make the dispatch wait, drop the oldest, or drop the newest"
    ],
    queue_block_timeout_ms: [
      type: :int,
      default: 5_000,
      min: 1,
      doc: "How long a dispatch waits on a full queue before its output overflows"
    ]
  ]

  @redacted "**redacted**"

  @doc """
  The rows naming the channel a plugin is attached to, for the jobs it does:
  `:in` (`channel`, `channel_in`), `:out` (`channel`, `channel_out`) or `:both`.
  `noun` is what one record is to this plugin, as its help says it.
  """
  @spec channel(:in | :out | :both, binary()) :: keyword()
  def channel(:in, noun) do
    [
      channel:
        channel_row(
          "The channel this plugin is attached to: the entry channel every #{noun} is injected on, and one name. Required, unless `channel_in` names it"
        ),
      channel_in:
        channel_row(
          "Entry channel every #{noun} is injected on, and one name; the explicit spelling of `channel`. Required, unless `channel` names it"
        )
    ]
  end

  def channel(:out, noun) do
    [
      channel:
        channel_row(
          "The channel this plugin is attached to: only a #{noun} naming it reaches the plugin. Several names, comma-separated, take them all"
        ),
      channel_out:
        channel_row(
          "Only a #{noun} whose metadata channel matches reaches the plugin; the explicit spelling of `channel`. Several names, comma-separated, take them all"
        )
    ]
  end

  def channel(:both, noun) do
    [
      channel:
        channel_row(
          "The channel this plugin is attached to: every inbound #{noun} is injected on it and only an output naming it reaches the plugin. One name, since it names the entry channel too"
        ),
      channel_in:
        channel_row(
          "Entry channel every inbound #{noun} is injected on, for a plugin whose two names differ. One name"
        ),
      channel_out:
        channel_row(
          "Only an output whose metadata channel matches reaches the plugin, for a plugin whose two names differ. Several names, comma-separated, take them all"
        )
    ]
  end

  defp channel_row(doc), do: [type: :names, doc: doc]

  @doc "The `plugin_id` row; `default` says what an unset id becomes."
  @spec plugin_id(binary()) :: keyword()
  def plugin_id(default),
    do: [plugin_id: [type: :string, doc: "Plugin id; defaults to #{default}"]]

  @doc "The `failure_threshold` row."
  @spec failure_threshold() :: keyword()
  def failure_threshold do
    [
      failure_threshold: [
        type: :int,
        default: 3,
        min: 1,
        doc: "Failures in a row before the plugin reports unhealthy"
      ]
    ]
  end

  @doc "The `idle_after_ms` row of a source."
  @spec idle_after() :: keyword()
  def idle_after do
    [
      idle_after_ms: [
        type: :int,
        default: 0,
        min: 0,
        doc:
          "Age of the last settled read past which the source is idle; 0 derives it from the plugin's read interval"
      ]
    ]
  end

  @doc "The queue rows named by `keys`, in their declared order; every one by default."
  @spec queue([atom()]) :: keyword()
  def queue(keys \\ Keyword.keys(@queue_rows)), do: Keyword.take(@queue_rows, keys)

  @doc """
  The retry rows, `retry_backoff_ms` and `retry_max_backoff_ms`, with `defaults`
  (keyed by those names) replacing 250 and 5 000.
  """
  @spec retry(keyword()) :: keyword()
  def retry(defaults \\ []) do
    defaults = Keyword.validate!(defaults, retry_backoff_ms: 250, retry_max_backoff_ms: 5_000)

    [
      retry_backoff_ms: [
        type: :int,
        default: defaults[:retry_backoff_ms],
        min: 1,
        doc: "First delay before a failed attempt is made again"
      ],
      retry_max_backoff_ms: [
        type: :int,
        default: defaults[:retry_max_backoff_ms],
        min: 1,
        doc: "Ceiling for that delay"
      ]
    ]
  end

  @doc """
  A credential's rows: `name`, a literal value redacted once prepared;
  `<name>_env`, an environment variable read at every connect and preferred over
  the literal; and `<name>_secret`, the transient row `hold_secret/2` keeps the
  literal in. `what` names the credential in the help.
  """
  @spec secret(atom(), binary()) :: keyword()
  def secret(name, what) do
    [
      {name, [type: :string, doc: "The #{what}, redacted once the plugin is prepared"]},
      {env_row(name),
       [
         type: :string,
         doc:
           "Environment variable holding the #{what}, read at every connect and preferred over an inline one"
       ]},
      {held_row(name), [type: :any, internal: true, transient: true]}
    ]
  end

  @doc """
  `opts` with a literal `name` moved into its transient row, inside a closure,
  and replaced by `**redacted**`. A plugin prepared again from these opts keeps
  the secret it holds.
  """
  @spec hold_secret(map(), atom()) :: map()
  def hold_secret(opts, name) do
    case Map.get(opts, name) do
      value when is_binary(value) and value not in ["", @redacted] ->
        opts |> Map.put(name, @redacted) |> Map.put(held_row(name), fn -> value end)

      _none_or_placeholder ->
        opts
    end
  end

  @doc """
  The credential `name` of prepared `opts` as a closure, resolved when called:
  `<name>_env` read then, else the held literal, else `nil`. The environment
  wins because it is all a plugin restarted from its persisted opts still has.
  """
  @spec secret_fun(map(), atom()) :: (-> binary() | nil)
  def secret_fun(opts, name) do
    case {Map.get(opts, env_row(name)), Map.get(opts, held_row(name))} do
      {env, _} when is_binary(env) and env != "" -> fn -> System.get_env(env) end
      {_, held} when is_function(held, 0) -> held
      _ -> fn -> nil end
    end
  end

  defp env_row(name), do: :"#{name}_env"
  defp held_row(name), do: :"#{name}_secret"

  @doc """
  Build `module`'s struct from a CLI string map, coercing each field through
  the provider's own `convert`. Answers the first value `convert` refuses.
  """
  @spec build_struct(module(), map(), (atom(), term() -> {:ok, term()} | {:error, term()})) ::
          {:ok, struct()} | {:error, term()}
  def build_struct(module, string_map, convert) do
    {required, all} = fields(module)

    # A halted reduce answers the refusal, never the accumulated params, so the
    # required-field check runs only on the map the success arm built. `convert`
    # has already formatted the reason; wrapping it again names the field twice.
    case Enum.reduce_while(all, %{}, &coerce_field(&1, &2, string_map, convert)) do
      {:error, reason} ->
        {:error, reason}

      params ->
        case Enum.reject(required, &Map.has_key?(params, &1)) do
          [] -> new_struct(module, params)
          missing -> refuse_required(module.__provider__().name, missing)
        end
    end
  end

  defp coerce_field(field, params, string_map, convert) do
    case Map.get(string_map, Atom.to_string(field)) do
      nil -> {:cont, params}
      value -> put_converted(params, field, convert.(field, value))
    end
  end

  @doc """
  The option names `provider` accepts: every declared row, and the options the
  host injects. Any other name is refused as unknown.
  """
  @spec known_names(module()) :: [atom()]
  def known_names(provider) do
    %Decl{options: rows} = provider.__provider__()
    Enum.map(rows, &elem(&1, 0)) ++ ProviderBehaviour.host_opts()
  end

  @doc """
  Every declared option present, converted and defaulted, with the required ones
  checked — the ONE thing that happens to a provider's options between the spec
  a user wrote and the callback that reads them.

  `Flezha.Plugin.Extension.Table` calls it before `init_provider/1`, so a
  provider and its worker read declared fields and never re-derive a default of
  their own. An option the declaration does not name is refused here rather than
  ignored; an option it names and the user did not write takes its row's
  default; a value the row's type or bounds refuse answers the one refusal the
  builder gives.
  """
  @spec normalise_opts(module(), map()) :: {:ok, map()} | {:error, term()}
  def normalise_opts(provider, opts) when is_map(opts) do
    %Decl{name: name, options: rows} = decl = provider.__provider__()

    with :ok <- reject_unknown(Map.keys(opts), known_names(provider), name),
         {:ok, converted} <- convert_declared(provider, rows, opts),
         {:ok, converted} <- resolve_channels(decl, opts, converted),
         :ok <- require_declared(name, rows, converted) do
      {:ok, Map.merge(opts, converted)}
    end
  end

  defp convert_declared(provider, rows, opts) do
    case Enum.reduce_while(rows, %{}, &convert_declared_row(&1, &2, provider, opts)) do
      {:error, reason} -> {:error, reason}
      params -> {:ok, params}
    end
  end

  # An option that is absent, or written `nil`, takes its row's default and is
  # not converted: only a value a user supplied can be wrong.
  defp convert_declared_row({field, row}, params, provider, opts) do
    case Map.get(opts, field) do
      nil -> {:cont, Map.put(params, field, row[:default])}
      value -> put_converted(params, field, provider.__convert__(field, value))
    end
  end

  defp require_declared(name, rows, params) do
    case for(
           {field, row} <- rows,
           Keyword.get(row, :required, false),
           is_nil(params[field]),
           do: field
         ) do
      [] -> :ok
      missing -> refuse_required(name, missing)
    end
  end

  # The channel a plugin is attached to, resolved once from the three rows that
  # can name it.
  #
  # `channel` names it in BOTH directions: the entry channel a plugin whose
  # provider feeds the relay injects on, and the filter over records passing
  # through the relay a plugin hooked on a chain acts on.
  # `channel_in` and `channel_out` name one each, for a plugin whose two names
  # differ. The answer is the two host-side values the readers take —
  # `:entry_channel` for the injection sites, `:filter_channel` for
  # `ProviderBehaviour.in_channel?/2` — so no provider re-derives the meaning
  # and dispatch pays nothing.
  #
  # It resolves BEFORE the required check, so a provider that requires an entry
  # channel is satisfied by either spelling, and it reads only the rows an
  # user wrote, so re-preparing a plugin resolves the same answer again.
  defp resolve_channels(%Decl{name: name} = decl, opts, converted) do
    jobs = ProviderBehaviour.channel_jobs(decl, opts)

    with :ok <- refuse_two_spellings(name, converted),
         :ok <- refuse_unreadable_channel(name, converted, jobs),
         :ok <- refuse_entry_set(name, converted, jobs) do
      {:ok,
       Map.merge(converted, %{
         entry_channel: attached(:in, jobs, converted),
         filter_channel: attached(:out, jobs, converted)
       })}
    end
  end

  # The name that reaches one job: its own spelling, else the one `channel`
  # gives both jobs at once — and nothing at all when the plugin does not do it.
  defp attached(job, jobs, converted) do
    if job in jobs do
      names = converted[@explicit_key[job]] || converted[:channel]
      if names, do: shaped(job, names)
    end
  end

  # The two halves differ in shape and this is where that is settled: the entry
  # side is the ONE name an injection writes into a record — a set there is
  # refused above, so there is never a second name to lose — and the filter side
  # the set `ProviderBehaviour.in_channel?/2` tests membership in. `List.wrap/1`
  # makes both true of a row typed as a plain string too, so the per-record
  # reading never asks.
  defp shaped(:in, names), do: names |> List.wrap() |> List.first()
  defp shaped(:out, names), do: List.wrap(names)

  # A payload is injected on exactly one channel, so a set of names has nothing
  # to mean on the entry side however many the filter side carries.
  defp refuse_entry_set(name, converted, jobs) do
    entry = named(converted, &(:in in jobs_named(&1, jobs)))

    case Enum.find(entry, &match?([_, _ | _], converted[&1])) do
      nil ->
        :ok

      key ->
        {:error, {:entry_channel_set, name, key}}
    end
  end

  # `channel` already names both directions, so a second spelling beside it is a
  # mistake rather than a precedence question.
  defp refuse_two_spellings(name, converted) do
    with [_ | _] <- named(converted, &(&1 == :either)),
         [_ | _] = keys <- named(converted, &(&1 != :either)) do
      {:error, {:channel_spelled_twice, name, keys}}
    else
      _ -> :ok
    end
  end

  # Every channel key names a job. A key naming a job this plugin does not do
  # could never take effect, and a feed that goes nowhere or a filter that
  # matches nothing is not something to discover in flight.
  defp refuse_unreadable_channel(name, converted, jobs) do
    case named(converted, &(jobs_named(&1, jobs) == [])) do
      [] -> :ok
      [key | _] -> {:error, {:channel_unused, name, key, @channel_rows[key]}}
    end
  end

  # The channel keys this plugin wrote whose job answers `predicate`.
  defp named(converted, predicate) do
    for {key, job} <- @channel_rows, converted[key], predicate.(job), do: key
  end

  # The jobs a channel key really names on this plugin: its own when the plugin
  # does it, and every job the plugin has where `channel` names them all. One
  # table-aware reading, and every question above is asked of it — a key that
  # names nothing could never take effect, and a key naming the entry job takes
  # one name.
  defp jobs_named(:either, jobs), do: jobs
  defp jobs_named(job, jobs), do: if(job in jobs, do: [job], else: [])

  defp put_converted(params, field, {:ok, converted}),
    do: {:cont, Map.put(params, field, converted)}

  defp put_converted(_params, _field, {:error, reason}), do: {:halt, {:error, reason}}

  defp new_struct(module, params) do
    {:ok, struct!(module, params)}
  rescue
    e -> {:error, {:struct_build_failed, module, Exception.message(e)}}
  end

  @doc """
  The screen `flz plugin help <provider_id>` renders for `module`, registered as
  `provider_id`.
  """
  @spec render_help(module(), binary()) :: binary()
  def render_help(module, provider_id) do
    {required, optional} =
      Enum.split_with(Decl.public_options(module.__provider__()), fn {_name, row} ->
        Keyword.get(row, :required, false)
      end)

    name = module |> Module.split() |> List.last()

    """
    #{name} Plugin Help

    USAGE:
        flz plugin enable <name> #{provider_id} [OPTIONS]
    """ <>
      param_block("REQUIRED", required) <>
      param_block("OPTIONAL", optional) <>
      """

      COMMON OPTIONS:
          --namespace <namespace>    Namespace (default: default)
          --relay_id <id>        Target relay ID
          --hook=<name>              Call the plugin at this hook; repeatable
          --<name>.<key>=<value>     A sub-option of the hook <name>
      """
  end

  # The REQUIRED and OPTIONAL sections are one rendering over two lists.
  defp param_block(heading, rows) do
    "\n#{heading} PARAMETERS (X-prefixed):\n" <> param_lines(rows)
  end

  defp param_lines([]), do: "    None\n"

  # The row's own `doc:` is what describes an option, so the help screen and the
  # documented table render the same sentence. A row that declares none falls
  # back to its name, which is all this screen ever said.
  defp param_lines(rows) do
    Enum.map_join(rows, "", fn {field, row} ->
      name = Atom.to_string(field)
      doc = Keyword.get(row, :doc) || String.capitalize(name)
      "    --X#{name} <value>    #{doc}\n"
    end)
  end

  @doc """
  `module`'s public option rows as the Markdown table its page under
  `docs/public/plugins/` carries. Rendered from the declaration, and gated against the
  page by the conformance test, so a documented key that is not a row, a row
  that is not documented and a default that differs each fail the suite.
  """
  @spec render_options_table(module()) :: binary()
  def render_options_table(module) do
    rows = Decl.public_options(module.__provider__())

    [
      "| Option | Type | Default | Required | Description |\n",
      "| --- | --- | --- | --- | --- |\n"
      | Enum.map(rows, &option_row/1)
    ]
    |> IO.iodata_to_binary()
  end

  defp option_row(row) do
    described = describe_row(row)

    [
      "| `",
      described[:name],
      "` | ",
      Atom.to_string(described[:type]),
      " | ",
      render_default(described[:default]),
      " | ",
      if(described[:required], do: "yes", else: "no"),
      " | ",
      described[:doc] || "",
      " |\n"
    ]
  end

  @doc """
  One row as the published description of it, in the order it is read: `name`,
  `type` (`:any` when the row names none), `required`, then each of `default`,
  `values`, `min`, `max`, `live` and `transient` the row declares, then `doc`.

  The documented table, `Flezha.Service.Fleet.provider/2`'s `options` and the
  catalogue's operation arguments all read a row through this. A transient row's
  default is never answered: a transient row is where a resolved credential lives.
  """
  @spec describe_row({atom(), keyword()}) :: keyword()
  def describe_row({name, row}) do
    transient? = Keyword.get(row, :transient, false)

    [
      name: Atom.to_string(name),
      type: Keyword.get(row, :type, :any),
      required: Keyword.get(row, :required, false)
    ] ++
      for(
        key <- [:default, :values, :min, :max, :live, :transient],
        Keyword.has_key?(row, key),
        not (key == :default and transient?),
        do: {key, row[key]}
      ) ++
      [doc: row[:doc]]
  end

  defp render_default(nil), do: "none"
  defp render_default(value), do: "`#{inspect(value)}`"

  @doc """
  Declare a provider: its option rows, its worker, and the defaults for every
  callback it does not override. See
  `Flezha.Plugin.ProviderBehaviour.Declaration`, which generates all of it.
  """
  defmacro __using__(opts) do
    quote do
      use Flezha.Plugin.ProviderBehaviour.Declaration, unquote(opts)
    end
  end

  @doc "`module`'s shape, as `flz plugin list` reports it."
  @spec describe(module()) :: map()
  def describe(module) do
    {required, _all} = fields(module)
    public = for {name, _row} <- Decl.public_options(module.__provider__()), do: name

    %{
      module: inspect(module),
      type: module.plugin_type(),
      required_fields: Enum.map(required, &Atom.to_string/1),
      optional_fields: Enum.map(public -- required, &Atom.to_string/1)
    }
  end

  @doc """
  Coerce `value` for `field` from the option row that declares it, then check
  the row's bounds. This is the whole conversion table: a provider names its
  option's TYPE and this answers every shape that type accepts and refuses every
  shape it does not.

  An option with no row — a host-injected key, a value the provider deliberately
  does not interpret — passes through: only a value a user supplied for a
  declared option can be wrong.
  """
  @spec convert_row(atom(), term(), [{atom(), keyword()}], binary() | nil) ::
          {:ok, term()} | {:error, term()}
  def convert_row(field, value, rows, provider \\ nil) do
    case Keyword.fetch(rows, field) do
      :error ->
        {:ok, value}

      {:ok, row} ->
        if row[:type] == :headers,
          do: headers(field, value, provider),
          else:
            with(
              {:ok, coerced} <- coerce(row[:type], field, value, row),
              do: bound(coerced, field, row)
            )
    end
  end

  # A header map, or the JSON object a CLI flag delivers it as, with names and
  # values as strings. Its refusal names the provider and never echoes a value,
  # which may be a credential; only a string that is not JSON at all is echoed.
  defp headers(field, value, provider) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, decoded} -> headers(field, decoded, provider)
      {:error, _} -> refuse_value(field, value)
    end
  end

  defp headers(field, value, provider) when is_map(value) and not is_struct(value) do
    if Enum.all?(value, fn {_name, v} -> is_binary(v) or is_number(v) end),
      do: {:ok, Map.new(value, fn {name, v} -> {to_string(name), to_string(v)} end)},
      else: {:error, {:headers_invalid, provider, field}}
  end

  defp headers(field, _value, provider), do: {:error, {:headers_invalid, provider, field}}

  defp coerce(:int, field, value, _row), do: parse_int(value, field)
  defp coerce(:bool, field, value, _row), do: parse_bool(value, field)
  defp coerce(:pid, field, value, _row), do: parse_pid(value, field)

  defp coerce(:enum, field, value, row),
    do: parse_enum(value, field, Keyword.fetch!(row, :values))

  defp coerce(:string, _field, value, _row) when is_binary(value), do: {:ok, value}
  defp coerce(:string, field, value, _row), do: refuse_value(field, value)
  defp coerce(:list, _field, value, _row) when is_list(value), do: {:ok, value}
  defp coerce(:list, field, value, _row), do: refuse_value(field, value)

  # A set of names: the comma-separated spelling a user writes where a flag
  # carries only a string, and the already-built list a manifest — or a plugin
  # re-prepared from the options it already holds — carries. `:list` cannot do
  # the first and this cannot replace it: its two rows are fed programmatically
  # and neither wants a string split.
  # A channel written as an atom, which an Elixir-side spec may carry, is the
  # name its string spells.
  defp coerce(:names, field, value, row) when field in @channel_keys and is_atom(value),
    do: coerce(:names, field, Atom.to_string(value), row)

  defp coerce(:names, field, value, _row) when is_binary(value),
    do: names(field, value, String.split(value, ","))

  # The empty set is a resolved `[]` default; only a written spelling can be empty by mistake.
  defp coerce(:names, _field, [], _row), do: {:ok, []}
  defp coerce(:names, field, value, _row) when is_list(value), do: names(field, value, value)
  defp coerce(:names, field, value, _row), do: refuse_value(field, value)
  defp coerce(:map, _field, value, _row) when is_map(value), do: {:ok, value}
  defp coerce(:map, field, value, _row), do: refuse_value(field, value)

  # A word naming something this node already knows, never minted: an unknown
  # word is refused naming the word.
  defp coerce(:atom, _field, value, _row) when is_atom(value), do: {:ok, value}
  defp coerce(:atom, field, value, _row) when is_binary(value), do: existing_atom(field, value)
  defp coerce(:atom, field, value, _row), do: refuse_value(field, value)

  # Named fields, written as an object whose keys this node already knows; what
  # is nested inside stays as it was written. An unknown key is refused naming
  # the key.
  defp coerce(:keyword, field, value, _row) when is_list(value) do
    if Keyword.keyword?(value), do: {:ok, value}, else: refuse_value(field, value)
  end

  defp coerce(:keyword, field, %{} = value, _row) when not is_struct(value) do
    Enum.reduce_while(value, {:ok, []}, fn {key, nested}, {:ok, done} ->
      case existing_atom(field, to_string(key)) do
        {:ok, atom} -> {:cont, {:ok, [{atom, nested} | done]}}
        refused -> {:halt, refused}
      end
    end)
    |> case do
      {:ok, done} -> {:ok, Enum.reverse(done)}
      refused -> refused
    end
  end

  defp coerce(:keyword, field, value, _row), do: refuse_value(field, value)

  # A JSON option is a map a user may also write as the string a CLI flag
  # delivers. A string that does not decode is refused as a string, never as the
  # decoder's own error struct: the callback promises `String.t()`.
  defp coerce(:json, field, value, row) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, decoded} -> coerce(:json, field, decoded, row)
      {:error, _} -> refuse_value(field, value)
    end
  end

  defp coerce(:json, _field, value, _row) when is_map(value) or is_list(value), do: {:ok, value}
  defp coerce(:json, field, value, _row), do: refuse_value(field, value)

  # `:any` is a deliberate verdict, not an omission: the value is whatever the
  # provider's own clause made of it, or a shape no type describes.
  defp coerce(_type, _field, value, _row), do: {:ok, value}

  defp existing_atom(field, written) do
    {:ok, String.to_existing_atom(written)}
  rescue
    ArgumentError -> refuse_value(field, written)
  end

  # An empty member is a typo — `alerts,,audit`, `alerts,` — and dropping it
  # would select a set nobody wrote. A member that is not a string at all is
  # refused by the same reading.
  defp names(field, value, parts) do
    trimmed = Enum.map(parts, fn part -> if is_binary(part), do: String.trim(part) end)

    if trimmed != [] and Enum.all?(trimmed, &(&1 not in [nil, ""])),
      do: {:ok, trimmed},
      else: refuse_value(field, value)
  end

  defp bound(value, field, row) when is_integer(value) do
    cond do
      is_integer(row[:min]) and value < row[:min] -> refuse_below(field, value, row[:min])
      is_integer(row[:max]) and value > row[:max] -> refuse_above(field, value, row[:max])
      true -> {:ok, value}
    end
  end

  defp bound(value, _field, _row), do: {:ok, value}

  @doc "The refusal for a value under its row's minimum."
  @spec refuse_below(atom() | binary(), term(), term()) :: {:error, tuple()}
  def refuse_below(field, value, minimum),
    do: {:error, {:option_below_minimum, field, value, minimum}}

  @doc "The refusal for a value over its row's maximum."
  @spec refuse_above(atom() | binary(), term(), term()) :: {:error, tuple()}
  def refuse_above(field, value, maximum),
    do: {:error, {:option_above_maximum, field, value, maximum}}

  @doc """
  The refusal for a required option that was not supplied. Named separately from
  `refuse_value/2` because nothing was given to echo back.
  """
  @spec refuse_required(binary(), [atom()]) :: {:error, {:options_required, binary(), [atom()]}}
  def refuse_required(provider, fields), do: {:error, {:options_required, provider, fields}}

  @doc """
  The name a provider answers refusals under: the last segment of its module,
  underscored — `Flezha.Plugin.Provider.HttpPoll` is `http_poll`.
  """
  @spec provider_name(module()) :: binary()
  def provider_name(module), do: module |> Module.split() |> List.last() |> Macro.underscore()
end
