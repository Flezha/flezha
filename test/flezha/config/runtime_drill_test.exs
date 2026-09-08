defmodule Flezha.Config.RuntimeDrillTest do
  @moduledoc """
  Every documented variable, set at once, observed on the key it fills.

  The drill is table-driven: it builds a non-default value from each row's own
  type, sets every one of them in the environment, and evaluates the file the
  release evaluates at boot — `config/runtime.exs`, entered the way the release
  enters it. A knob added to the table is covered here with no test edit; a
  knob an operator can set and never observe fails.
  """
  # async: false: the OS environment is process-global. The drill sets every
  # documented variable at once, so `:system_env` is what puts the run's own
  # environment back around it.
  use FlezhaTest.Case, fixtures: [:system_env], async: false

  alias Flezha.Config.Environment
  alias Flezha.Config.Keys

  test "every variable reaches the key its row names, in prod and in test" do
    {values, keyed_rows} = environment()
    System.put_env(values)

    for config_env <- [:prod, :test] do
      config = read!(config_env)

      unobserved =
        for row <- keyed_rows,
            expected = expected(row, Map.fetch!(values, row.env)),
            actual = get_in(config[:flezha], path(row.key)),
            actual != expected,
            do:
              "#{row.env} in #{config_env}: the key holds #{inspect(actual)}, " <>
                "and the variable says #{inspect(expected)}"

      assert unobserved == []
    end
  end

  test "a run in test derives its own resources, and an explicit variable still wins" do
    # The derived half only holds when nothing states the value, and a session
    # that isolates its runs exports these two. `:system_env` puts them back.
    System.delete_env("FLEZHA_STATE_DIR")
    System.delete_env("FLEZHA_SOCKET_FILES_PATH")

    config = read!(:test)
    token = config[:flezha][:run_token]

    assert token == System.pid()
    assert config[:flezha][:state_root] == Environment.per_run(token).state_root
    assert config[:flezha][:socket_files_path] == Environment.per_run(token).socket_files_path

    System.put_env("FLEZHA_STATE_DIR", "/tmp/an-operator-said-so")
    assert read!(:test)[:flezha][:state_root] == "/tmp/an-operator-said-so"
  end

  test "a production boot with no durability backend named resolves to the refusal" do
    assert read!(:prod)[:flezha][:durability_tier][:backend] == :unset
    assert read!(:test)[:flezha][:durability_tier][:backend] == :none
  end

  test "the file log takes its four rows" do
    config = read!(:prod)

    assert config[:logger][:file_log][:path] == Flezha.Config.default(:log_path)
    assert config[:logger][:file_log][:level] == Flezha.Config.default(:log_level, :controller)
    assert config[:logger][:file_log][:rotate].keep == Flezha.Config.default(:log_keep_files)
  end

  # ---------- the environment this drill sets ----------

  # One non-default value per row, built from the row's own type — so a new row
  # is driven by its declaration and nothing here.
  defp environment() do
    rows =
      for {_name, row} <- Keys.rows(),
          row.env,
          row.key,
          is_nil(row.per_run),
          row.type != :term,
          do: row

    {Map.new(rows, &{&1.env, other_value(&1)}), rows}
  end

  defp other_value(%{type: :bool} = row), do: to_string(not truthy_default(row))
  defp other_value(%{type: {:enum, members}} = row), do: to_string(next_member(members, row))

  defp other_value(%{type: {:list, {:enum, members}}}),
    do: Enum.map_join(Enum.take(members, 2), ",", &to_string/1)

  defp other_value(%{type: {:list, _}}), do: "one,two"
  defp other_value(%{type: :address}), do: "0.0.0.0:24433"
  defp other_value(%{type: {:custom, :days_to_seconds}}), do: "11"
  defp other_value(%{type: {:custom, :pem_files}}), do: pem_file()
  defp other_value(%{type: type} = row) when type in [:int, :ms], do: to_string(number(row) + 1)
  defp other_value(%{type: :positive_int} = row), do: to_string(number(row) + 1)
  defp other_value(%{name: name}), do: "drill-#{name}"

  defp expected(row, raw) do
    {:ok, value} = Environment.parse(row, raw)
    value
  end

  defp number(row) do
    case Flezha.Config.default(row.name) do
      value when is_integer(value) -> value
      _ -> 1
    end
  end

  defp truthy_default(row), do: Flezha.Config.default(row.name) == true

  defp next_member(members, row) do
    default = Flezha.Config.default(row.name)
    Enum.find(members, hd(members), &(&1 != default))
  end

  # The trusted-keys row's parser reads the files it is pointed at.
  defp pem_file() do
    path = Path.join(System.tmp_dir!(), "knob_drill_#{System.unique_integer([:positive])}.pem")
    File.write!(path, "-----BEGIN PUBLIC KEY-----\ndrill\n-----END PUBLIC KEY-----\n")
    ExUnit.Callbacks.on_exit(fn -> File.rm(path) end)
    path
  end

  defp read!(config_env),
    do: Config.Reader.read!("config/runtime.exs", env: config_env, target: :host)

  defp path(key) when is_atom(key), do: [key]
  defp path(key) when is_tuple(key), do: Tuple.to_list(key)
end
