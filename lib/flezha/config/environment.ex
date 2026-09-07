defmodule Flezha.Config.Environment do
  @moduledoc """
  What the operating system's environment means for the table.

  One job, two entry points: `parse/2` reads one variable's text by its row's
  rule, and `runtime_config/2` reads a whole environment — every variable turned
  into the application-env key its row names, every per-run resource derived
  from the one run token, every environment's own default applied. The Elixir
  SDK translates its runner's environment through the same `parse/2`, which is
  what makes a runner and a boot answer one text the same way.

  The parse rule is the same one every runtime repeats;
  `test/fixtures/knob_parse_cases.txt` is it written down, and each runtime's
  suite feeds that file to its own reader. `:positive_int` and `:bool` fall
  back to the row's default on text that is not a value of the type, because
  `"0"` already means "unset" everywhere and a flag nobody spelled is not a
  flag. Every other type refuses at boot, naming the variable, the value and
  the type: an operator who sets a knob and silently gets the default back has
  been told a knob exists that does nothing.
  """

  alias Flezha.Config
  alias Flezha.Config.Keys

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
  boot: every row an operator's variable, this environment's default or the
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

  defp write(row, config, {:ok, value}), do: nest(config, row.key, value)

  defp write(row, config, :none) do
    if writable?(row), do: nest(config, row.key, row.default), else: config
  end

  @doc """
  The resources one controller instance owns exclusively, derived from one run
  token so two suites on one checkout cannot meet: a state root, a socket
  directory, a data-plane port, the testnet port, the node's base name and the
  hub registry root. `peer` names a second BEAM on this host and shifts every
  one of them.
  """
  @spec per_run(binary() | integer(), term()) :: %{Keys.name() => term()}
  def per_run(token, peer \\ nil) do
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
      process_hub_registry_root: Path.join(state_root, "process_hub")
    }
  end

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
  # operator's variable, then this run's own derivation, then this
  # environment's default. `:none` means nothing did — the row keeps its
  # declared default and no key is written.
  defp chosen(row, env_map, config_env, derived) do
    cond do
      raw = supplied(env_map, row.env) -> {:ok, from_env!(row, raw)}
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
  defp supplied(_env_map, nil), do: nil

  defp supplied(env_map, var) do
    case env_map[var] do
      nil -> nil
      value -> if String.trim(value) == "", do: nil, else: value
    end
  end

  # A row whose value the boot translation writes even when nothing supplied
  # one, so the key an operator inspects is the value the code reads. A
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
  # an operator who sets a knob and silently gets the default back has been
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
                "#{row.name} keeps #{Keys.render_default(row)} when it is unset"
    end
  end

  defp parse_type(:bool, raw) do
    case String.downcase(raw) do
      truthy when truthy in ["1", "true", "yes", "on"] -> {:ok, true}
      falsy when falsy in ["0", "false", "no", "off"] -> {:ok, false}
      _ -> :error
    end
  end

  defp parse_type(:ms, raw), do: whole(raw, 0)
  defp parse_type(:positive_int, raw), do: whole(raw, 1)
  defp parse_type(:int, raw), do: whole(raw, nil)
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
    case whole(raw, 0) do
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

  # `:string` and `:path` take the text as it stands.
  defp parse_type(_text, raw), do: {:ok, raw}

  defp whole(raw, floor) do
    case Integer.parse(raw) do
      {n, ""} when floor == nil -> {:ok, n}
      {n, ""} when n >= floor -> {:ok, n}
      _ -> :error
    end
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
      "cargo run --quiet --manifest-path=#{cwd}/test/apps/rust_runner/Cargo.toml " <>
        "--target-dir=./target"

    [command, command]
  end
end
