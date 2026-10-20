defmodule Flezha.Config.Environment do
  @moduledoc """
  What the operating system's environment means for the table.

  One job, two entry points: `parse/2` reads one variable's text by its row's
  rule, and `runtime_config/2` reads a whole environment — every variable turned
  into the application-env key its row names, every per-run resource derived
  from the one run token, every environment's own default applied. `check!/1`
  then holds the application environment the boot ended with to the table. A
  scalar type parses by the Elixir SDK's own reader, `Flezha.SDK.Knob.parse/2`,
  which is what makes a runner and a boot answer one text the same way.

  The parse rule is the same one every runtime repeats;
  `test/fixtures/knob_parse_cases.txt` is it written down, and each runtime's
  suite feeds that file to its own reader. `:positive_int` and `:bool` fall
  back to the row's default on text that is not a value of the type, because
  `"0"` already means "unset" everywhere and a flag nobody spelled is not a
  flag. Every other type refuses at boot, naming the variable, the value and
  the type: a user who sets a knob and silently gets the default back has
  been told a knob exists that does nothing.
  """

  alias Flezha.Config
  alias Flezha.Config.Keys
  alias Flezha.Config.Reference
  alias Flezha.SDK.Knob

  @doc """
  Read one row out of `env_map`, applying the same precedence
  `runtime_config/2` does: the variable, else this environment's default, else
  the row's own.
  """
  @spec env_value(map(), atom(), Keys.name()) :: term()
  def env_value(env_map, config_env, name) do
    row = Keys.fetch!(name)

    case chosen(row, env_map, config_env, %{}) do
      :none -> Config.default(row.name)
      {:ok, value} -> value
    end
  end

  @doc """
  The whole `:flezha` application environment `config/runtime.exs` writes at
  boot: every row a user's variable, this environment's default or the
  run's own derivation supplies a value for. A row nothing supplied keeps its
  declared default, answered by `value/1` on read rather than copied here.
  """
  @spec runtime_config(map(), atom()) :: keyword()
  def runtime_config(env_map, config_env) do
    token = env_value(env_map, config_env, :run_token)
    derived = if token, do: per_run(token), else: %{}

    Keys.rows()
    |> Map.values()
    |> Enum.filter(& &1.key)
    |> Enum.reduce([], &write(&1, &2, chosen(&1, env_map, config_env, derived)))
  end

  # Keys OTP or Mix put in an application's environment themselves.
  @runtime_keys [:included_applications]

  @doc """
  Refuse a `:flezha` application environment the table cannot read, once at
  boot after `runtime_config/2` has filled it: a key that is no row's key and
  no compound group's root, named with the nearest row, and a value its row's
  type refuses, named with the key, the type and the value. A misspelt or
  retired key otherwise configures nothing and says nothing.
  """
  @spec check!(keyword()) :: :ok
  def check!(env \\ Application.get_all_env(:flezha)) do
    case unknown_keys(env) ++ mistyped(env) do
      [] ->
        :ok

      found ->
        raise ArgumentError, "the :flezha configuration refuses:\n  " <> Enum.join(found, "\n  ")
    end
  end

  defp unknown_keys(env) do
    known = known_roots()

    for {key, _value} <- env, key not in known do
      "#{inspect(key)} is no setting; the nearest is #{nearest(key)}"
    end
  end

  defp known_roots() do
    roots = for {_name, %{key: key}} <- Keys.rows(), key, do: root(key)
    Enum.uniq(roots ++ @runtime_keys)
  end

  defp root(key) when is_tuple(key), do: elem(key, 0)
  defp root(key), do: key

  # Judged on the row's name, which says what it sets, a name holding the key
  # whole first (a retired map's global row); answered with the key a
  # configuration file writes.
  defp nearest(key) do
    key = to_string(key)

    row =
      Keys.rows()
      |> Map.values()
      |> Enum.filter(& &1.key)
      |> Enum.max_by(fn %{name: name} ->
        name = to_string(name)
        {String.contains?(name, key), String.jaro_distance(name, key)}
      end)

    if row.key == row.name,
      do: inspect(row.key),
      else: "#{inspect(row.key)} (row #{inspect(row.name)})"
  end

  defp mistyped(env) do
    for {_name, %{key: key} = row} <- Keys.rows(),
        key,
        {:ok, value} <- [at(env, key)],
        not typed?(row, value) do
      "#{inspect(key)} is #{inspect(value)}, not a #{inspect(row.type)}" <> at_least(row)
    end
  end

  defp at_least(%{min: nil}), do: ""
  defp at_least(%{min: min}), do: " of at least #{min}"

  defp at(env, key) when is_atom(key), do: Keyword.fetch(env, key)

  defp at(env, key) when is_tuple(key) do
    Enum.reduce_while(Tuple.to_list(key), {:ok, env}, fn part, {:ok, level} ->
      with true <- Keyword.keyword?(level),
           {:ok, _} = found <- Keyword.fetch(level, part),
           do: {:cont, found},
           else: (_ -> {:halt, :error})
    end)
  end

  # Unset is a value of every type; a `:keep_raw` row answers for its own text.
  defp typed?(_row, nil), do: true
  defp typed?(%{on_bad_value: :keep_raw}, value) when is_binary(value), do: true
  defp typed?(row, value), do: of_type?(row.type, value) and (row.min == nil or value >= row.min)

  defp of_type?(type, v) when type in [:ms, {:custom, :days_to_seconds}],
    do: is_integer(v) and v >= 0

  defp of_type?(:int, v), do: is_integer(v)
  defp of_type?(:positive_int, v), do: is_integer(v) and v > 0
  defp of_type?(:bool, v), do: is_boolean(v)
  defp of_type?(type, v) when type in [:string, :path, :address], do: is_binary(v)
  defp of_type?(:atom, v), do: is_atom(v)
  defp of_type?(:term, _v), do: true
  defp of_type?({:enum, members}, v), do: v in members
  defp of_type?({:list, inner}, v), do: is_list(v) and Enum.all?(v, &of_type?(inner, &1))
  defp of_type?({:custom, :pem_files}, v), do: is_list(v) and Enum.all?(v, &is_binary/1)

  defp write(row, config, {:ok, value}), do: nest(config, row.key, value)

  defp write(row, config, :none) do
    if writable?(row), do: nest(config, row.key, row.default), else: config
  end

  @doc """
  The resources one controller instance owns exclusively, derived from one run
  token so two suites on one checkout cannot meet: a state root, a socket
  directory, a data-plane port, the testnet port, the node's base name, and the
  hub registry root, dead-letter directory and plugin root under the state root. `peer` is the index the harness gave a second BEAM on this
  host, and shifts every one of them.
  """
  @spec per_run(binary() | integer(), pos_integer() | nil) :: %{Keys.name() => term()}
  def per_run(token, peer \\ nil) when is_nil(peer) or (is_integer(peer) and peer > 0) do
    token = to_string(token)
    slice = rem(String.to_integer(token) + peer_offset(peer), 10_000)
    scope = scoped(token, peer, "-")
    state_root = Path.join([System.tmp_dir!(), "flezha_test_state", scope])

    %{
      state_root: state_root,
      socket_files_path: Path.join([System.tmp_dir!(), "flezha_sockets", scope]),
      quic_listen_address: "0.0.0.0:#{20_000 + slice}",
      testnet_ws_port: 10_000 + slice,
      # A node name is read back as an atom by the harness and by `rel/`, so it
      # joins on an underscore rather than the hyphen a directory takes.
      node_base_name: "flezha_test_" <> scoped(token, peer, "_"),
      process_hub_registry_root: Path.join(state_root, "process_hub"),
      dlq_dir: Path.join(state_root, "dlq"),
      plugin_root: Path.join(state_root, "plugins")
    }
  end

  # A peer is scoped by its index, never its node name: the harness mints each
  # index once in a run, so no two peers share a resource, and a 20-digit index
  # still leaves the socket directory within the 60 bytes a runner's data socket
  # allows it, where a node name may run to 255.
  defp scoped(token, nil, _join), do: token
  defp scoped(token, peer, join), do: "#{token}#{join}#{peer}"

  @doc """
  Parse `raw` as this row's type. `:error` when the text is not one — the
  caller decides whether that is a refusal or a fall back to the default, which
  is the one parse rule every runtime's reader repeats.
  """
  @spec parse(Keys.name() | Keys.row(), binary()) :: {:ok, term()} | :error
  def parse(name, raw) when is_atom(name), do: parse(Keys.fetch!(name), raw)
  def parse(%{type: type}, raw), do: parse_type(type, String.trim(raw))

  # What the environment supplies for one row, in precedence order: the
  # user's variable, then this run's own derivation, then this
  # environment's default. `:none` means nothing did — the row keeps its
  # declared default and no key is written.
  defp chosen(row, env_map, config_env, derived) do
    cond do
      raw = supplied(env_map, row.env, row.blank) -> {:ok, from_env!(row, raw)}
      Map.has_key?(derived, row.name) -> {:ok, Map.fetch!(derived, row.name)}
      Keyword.has_key?(row.env_default, config_env) -> env_default(row, config_env)
      true -> fallback_variable(row, env_map)
    end
  end

  # One row's default IS another variable's value: an unset cluster name must
  # leave the domain empty rather than take the name row's default word.
  defp fallback_variable(%{default: {:env, var, _}} = row, env_map) do
    case supplied(env_map, var) do
      nil -> :none
      raw -> {:ok, from_env!(row, raw)}
    end
  end

  defp fallback_variable(_row, _env_map), do: :none

  # A variable set to nothing is a variable nobody set: an empty value cannot
  # be a path, a port or a flag, and every runtime's reader reads it as unset.
  # A row declaring `blank: :value` is the exception: set and empty is a value
  # of its own.
  defp supplied(env_map, var, blank \\ nil)
  defp supplied(_env_map, nil, _blank), do: nil
  defp supplied(env_map, var, :value), do: env_map[var]

  defp supplied(env_map, var, nil) do
    case env_map[var] do
      nil -> nil
      value -> if String.trim(value) == "", do: nil, else: value
    end
  end

  # A row whose value the boot translation writes even when nothing supplied
  # one, so the key a user inspects is the value the code reads. A
  # derivation stays a derivation (its parent may still move), a `{:late, …}`
  # is answered when it is read rather than frozen at boot, and a key another
  # config file fills is left alone.
  defp writable?(%{owner: owner}) when owner != nil, do: false
  defp writable?(%{derived: derived}) when derived != nil, do: false
  defp writable?(%{default: {:late, _}}), do: false
  defp writable?(%{default: {:env, _, _}}), do: false
  defp writable?(%{default: default}), do: default != nil

  defp env_default(row, config_env) do
    case Keyword.fetch!(row.env_default, config_env) do
      {:custom, name} -> {:ok, built(name)}
      value -> {:ok, Config.literal(value)}
    end
  end

  defp built(:test_runner_commands), do: test_runner_commands()

  # `:positive_int` and `:bool` fall back to the default: a pool size of `"0"`
  # already means "unset" in every runtime (`flz_core::util::env_parse_positive`,
  # `FlezhaEnv.parsePositive`), and a flag nobody spelled is not a flag. Every
  # other type refuses at boot, naming the variable, the value and the type —
  # a user who sets a knob and silently gets the default back has been
  # told a knob exists that does nothing.
  defp from_env!(row, raw) do
    case parse(row, raw) do
      {:ok, value} ->
        value

      :error when row.type in [:positive_int, :bool] ->
        Config.default(row.name)

      # A row whose reader must not turn a typo into a crash loop keeps the
      # text instead of refusing it, and answers for the value itself: the
      # durability backend resolves an unrecognised name to `none` and names
      # it, which it can only do if the name survives this parse.
      :error when row.on_bad_value == :keep_raw ->
        raw

      :error ->
        raise ArgumentError,
              "#{row.env}=#{inspect(raw)} is not a #{inspect(row.type)}; " <>
                "#{row.name} keeps #{Reference.render_default(row)} when it is unset"
    end
  end

  # The scalar types are the Elixir SDK's reader's own rule, so a runner and a
  # boot answer one text the same way.
  defp parse_type(type, raw) when type in [:bool, :ms, :positive_int, :int, :string, :path],
    do: Knob.parse(type, raw)

  defp parse_type(:atom, raw), do: {:ok, String.to_atom(raw)}
  defp parse_type(:term, raw), do: {:ok, raw}

  defp parse_type({:enum, members}, raw) do
    case Enum.find(members, &(to_string(&1) == raw)) do
      nil -> :error
      member -> {:ok, member}
    end
  end

  defp parse_type({:list, inner}, raw) do
    raw
    |> String.split(",", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.reduce_while({:ok, []}, fn part, {:ok, acc} ->
      case parse_type(inner, part) do
        {:ok, value} -> {:cont, {:ok, acc ++ [value]}}
        :error -> {:halt, :error}
      end
    end)
  end

  defp parse_type(:address, raw) do
    case String.split(raw, ":") do
      [_host, port] -> if match?({_, ""}, Integer.parse(port)), do: {:ok, raw}, else: :error
      _ -> :error
    end
  end

  defp parse_type({:custom, :days_to_seconds}, raw) do
    case Knob.parse(:ms, raw) do
      {:ok, days} -> {:ok, days * 86_400}
      :error -> :error
    end
  end

  # The variable names PEM files on disk; the row's value is their bytes.
  defp parse_type({:custom, :pem_files}, raw) do
    keys =
      raw
      |> String.split(",", trim: true)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.map(&File.read!/1)

    {:ok, keys}
  end

  # One slice of a thousand-wide band per contested port, so the next one is a
  # base and a line. A peer takes a slice of its own so two BEAMs on one host
  # never bid for one port.
  defp peer_offset(nil), do: 0
  defp peer_offset(peer), do: :erlang.phash2(peer, 1_000) + 1

  defp nest(config, key, value) when is_atom(key), do: Keyword.put(config, key, value)

  defp nest(config, key, value) when is_tuple(key),
    do: put_in_path(config, Tuple.to_list(key), value)

  defp put_in_path(config, [leaf], value), do: Keyword.put(config, leaf, value)

  defp put_in_path(config, [head | rest], value),
    do: Keyword.put(config, head, put_in_path(Keyword.get(config, head, []), rest, value))

  @doc """
  The two commands `:test` runs as external processes: one reference runner per
  data channel the suite expects to be connected before it starts.

  `--quiet` suppresses the `Finished dev profile […] / Running […]` banners
  cargo emits each time a runner is re-spawned for an ad-hoc test namespace.
  Build cache hits keep the calls essentially free; only the noise is gone.
  """
  @spec test_runner_commands() :: [binary()]
  def test_runner_commands() do
    {:ok, cwd} = :file.get_cwd()

    command =
      "cargo run --quiet --manifest-path=#{cwd}/sdk/rust/test_app/Cargo.toml " <>
        "--target-dir=./target"

    [command, command]
  end
end
