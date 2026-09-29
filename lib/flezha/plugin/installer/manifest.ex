defmodule Flezha.Plugin.Installer.Manifest do
  @moduledoc """
  A plugin package's `manifest.exs`: a literal term, parsed without evaluating
  code, naming the package, the runtime it was built on, the applications its
  `lib/` holds and the providers it declares.

      %{
        id: "flezha_example_plugin",
        version: "0.1.0",
        built_with: %{flezha: "0.1.0", otp: "28", elixir: "1.20.2"},
        apps: [{:flezha_example_plugin, "0.1.0"}, {:stamp_lib, "0.1.0"}],
        providers: %{"example_stamp" => FlezhaExamplePlugin.Stamp}
      }

  The first application of `apps` is the plugin application: `id` is its name
  and `version` its `vsn`.
  """

  alias Flezha.Identificator

  @type t :: %{
          required(:id) => binary(),
          required(:version) => binary(),
          required(:built_with) => %{flezha: binary(), otp: binary(), elixir: binary()},
          required(:apps) => [{atom(), binary()}],
          required(:providers) => %{binary() => module()},
          optional(:description) => binary()
        }

  @required [:id, :version, :built_with, :apps, :providers]

  @doc "Read and parse the `manifest.exs` of the package extracted in `dir`."
  @spec read(Path.t()) :: {:ok, term()} | {:error, term()}
  def read(dir), do: dir |> Path.join("manifest.exs") |> File.read!() |> parse()

  @doc """
  Parse manifest source into a term. The syntax tree must hold only literals
  (maps, lists, tuples, atoms, binaries, numbers, module aliases), so a
  manifest carrying a call or a variable is refused without being evaluated.
  """
  @spec parse(binary()) :: {:ok, term()} | {:error, term()}
  def parse(source) when is_binary(source) do
    case Code.string_to_quoted(source) do
      {:ok, ast} ->
        if literal_ast?(ast),
          do: {:ok, ast |> Code.eval_quoted([]) |> elem(0)},
          else: invalid(:manifest_not_literal, %{})

      {:error, _reason} ->
        invalid(:manifest_unparsable, %{})
    end
  end

  @doc "The manifest as the literal source `parse/1` reads back."
  @spec render(t()) :: binary()
  def render(manifest), do: inspect(manifest, pretty: true, limit: :infinity) <> "\n"

  @doc """
  Check `manifest` against the schema and against the applications the
  package's `lib/` holds (as `Package.read_lib/1` read them).
  """
  @spec validate(term(), [map()]) :: :ok | {:error, term()}
  def validate(manifest, apps) when is_map(manifest) do
    with :ok <- required(manifest),
         :ok <- field(:apps, manifest.apps, &apps_shape?/1),
         {plugin_app, plugin_vsn} = hd(manifest.apps),
         :ok <- field(:id, manifest.id, &(Identificator.validate(:plugin, &1) == :ok)),
         :ok <- field(:id, manifest.id, &(&1 == Atom.to_string(plugin_app))),
         :ok <- field(:version, manifest.version, &(Version.parse(&1) != :error)),
         :ok <- field(:version, manifest.version, &(&1 == plugin_vsn)),
         :ok <- field(:built_with, manifest.built_with, &built_with?/1),
         :ok <- field(:description, Map.get(manifest, :description, ""), &is_binary/1),
         :ok <- same_apps(manifest.apps, apps) do
      providers(manifest.providers, Enum.find(apps, &(&1.name == plugin_app)))
    end
  end

  def validate(_manifest, _apps), do: invalid(:manifest_not_a_map, %{})

  @doc """
  The package's `built_with` against `runtime`: the same OTP release, the same
  Elixir `major.minor`, and a Flezha version satisfying `~> built_with.flezha`.
  """
  @spec check_runtime(t(), map()) :: :ok | {:error, term()}
  def check_runtime(%{built_with: built}, runtime) do
    if built.otp == runtime.otp and minor(built.elixir) == minor(runtime.elixir) and
         Version.match?(runtime.flezha, "~> " <> built.flezha) do
      :ok
    else
      {:error,
       {:runtime_mismatch,
        %{
          package_otp: built.otp,
          package_elixir: built.elixir,
          package_flezha: built.flezha,
          otp: runtime.otp,
          elixir: runtime.elixir,
          flezha: runtime.flezha
        }}}
    end
  end

  @doc "The runtime this VM runs: its OTP release, Elixir version and Flezha version."
  @spec runtime() :: %{otp: binary(), elixir: binary(), flezha: binary()}
  def runtime() do
    _ = Application.load(:flezha)

    %{
      otp: System.otp_release(),
      elixir: System.version(),
      flezha: to_string(Application.spec(:flezha, :vsn))
    }
  end

  ## Schema

  defp required(manifest) do
    case Enum.find(@required, &(not Map.has_key?(manifest, &1))) do
      nil -> :ok
      missing -> invalid(:missing_field, %{field: missing})
    end
  end

  defp field(name, value, valid?) do
    if valid?.(value), do: :ok, else: invalid(:invalid_field, %{field: name, value: value})
  end

  defp apps_shape?([_ | _] = apps),
    do: Enum.all?(apps, &match?({name, vsn} when is_atom(name) and is_binary(vsn), &1))

  defp apps_shape?(_), do: false

  defp built_with?(%{flezha: flezha, otp: otp, elixir: elixir})
       when is_binary(flezha) and is_binary(otp) and is_binary(elixir) do
    Version.parse(flezha) != :error and Version.parse(elixir) != :error and
      Regex.match?(~r/^\d+$/, otp)
  end

  defp built_with?(_), do: false

  # Every `{app, vsn}` is exactly one `lib/<app>-<vsn>`, and `lib/` holds no other.
  defp same_apps(declared, apps) do
    found = MapSet.new(apps, &{&1.name, &1.vsn})

    case Enum.sort(MapSet.symmetric_difference(MapSet.new(declared), found)) do
      [] -> :ok
      [{name, vsn} | _] -> invalid(:apps_mismatch, %{application: name, vsn: vsn})
    end
  end

  defp providers(providers, plugin_app) when is_map(providers) and map_size(providers) > 0 do
    Enum.find_value(Enum.sort(providers), :ok, fn {id, module} ->
      cond do
        Identificator.validate(:plugin, id) != :ok ->
          invalid(:invalid_field, %{field: :providers, value: id})

        module not in plugin_app.modules ->
          invalid(:provider_not_in_app, %{provider: id, module: module})

        true ->
          nil
      end
    end)
  end

  defp providers(providers, _plugin_app),
    do: invalid(:invalid_field, %{field: :providers, value: providers})

  defp minor(version) do
    {:ok, %Version{major: major, minor: minor}} = Version.parse(version)
    {major, minor}
  end

  ## Literal AST

  defp literal_ast?(node) when is_atom(node) or is_binary(node) or is_number(node), do: true
  defp literal_ast?({a, b}), do: literal_ast?(a) and literal_ast?(b)
  defp literal_ast?(list) when is_list(list), do: Enum.all?(list, &literal_ast?/1)

  defp literal_ast?({:__aliases__, _meta, segments}) when is_list(segments),
    do: Enum.all?(segments, &is_atom/1)

  defp literal_ast?({:%{}, _meta, pairs}) when is_list(pairs),
    do: Enum.all?(pairs, &match?({_, _}, &1)) and Enum.all?(pairs, &literal_ast?/1)

  defp literal_ast?({:{}, _meta, elements}) when is_list(elements),
    do: Enum.all?(elements, &literal_ast?/1)

  defp literal_ast?({:-, _meta, [n]}) when is_number(n), do: true
  defp literal_ast?(_), do: false

  defp invalid(problem, found), do: {:error, {:package_invalid, problem, found}}
end
