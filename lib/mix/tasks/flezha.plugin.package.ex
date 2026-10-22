defmodule Mix.Tasks.Flezha.Plugin.Package do
  @shortdoc "Build a plugin package (.fpz) from this project"

  @moduledoc """
  Builds the plugin package a Flezha controller loads at boot, from the plugin
  project this task runs in:

      MIX_ENV=prod mix flezha.plugin.package [--output DIR] [--sign KEY.pem]

  The archive holds the compiled plugin application and every application of its
  runtime closure that the controller release does not ship, laid out like a
  release's `lib/`, with a manifest recording the Flezha, OTP and Elixir versions
  it was built on. A controller accepts it only on that same runtime line, so
  build it with the controller's toolchain (the `plugin-builder` stage of the
  controller's Dockerfile).

  The plugin application names its providers in its environment:

      def application do
        [env: [flezha_providers: %{"my_provider" => MyPlugin.Provider}]]
      end

  The project's configuration of each library it bundles, as the `MIX_ENV` the
  task runs in loads it (build with `MIX_ENV=prod`), is written into that
  library's `.app`, which is where the library reads its defaults on the
  controller. The plugin application's own configuration is not written, a
  library the release ships keeps the controller's settings, and every setting
  written is public to whoever holds the package.

  The task refuses, writing nothing, a library the release ships resolved at
  another version than Flezha's own `mix.lock`, native code, an application
  depending on Flezha that declares an application callback, a provider entry
  that does not name a provider module of the plugin application, and a library
  setting an `.app` file cannot hold (a function other than `&Mod.fun/arity`, a
  pid or a reference).

  ## Options

    * `--output` - the directory the archive is written to (default `_build/`)
    * `--sign` - an Ed25519 private key in PEM form; writes `signature.sig`
  """

  use Mix.Task

  alias Flezha.Plugin.Installer.Checksums
  alias Flezha.Plugin.Installer.Manifest
  alias Flezha.Plugin.Installer.Package
  alias Flezha.Plugin.Installer.Signature
  alias Flezha.Plugin.Providers

  # The applications `mix release` adds to every release beside its own closure.
  @release_extras [:sasl, :iex]

  @impl Mix.Task
  def run(args) do
    {opts, _rest} = OptionParser.parse!(args, strict: [output: :string, sign: :string])

    Mix.Task.run("compile", [])
    flezha_lock = Mix.Dep.Lock.read(Path.join(Mix.Project.deps_paths()[:flezha], "mix.lock"))
    check_locks!(flezha_lock, Mix.Dep.cached())

    # One per OS process: two packagings of a project share its build path.
    stage = Path.join(Mix.Project.build_path(), "flezha_plugin_package_#{System.pid()}")
    output = opts[:output] || Path.dirname(Mix.Project.build_path())
    archive = build!(Mix.Project.config()[:app], stage, output, opts[:sign])
    Mix.shell().info("Wrote #{archive}")
  end

  @doc false
  # Stage the loaded application `plugin` with the part of its runtime closure
  # the release does not ship in `stage`, signed with the key file `key` when
  # given, and write its archive into `output`. Answers the archive's path.
  @spec build!(atom(), Path.t(), Path.t(), Path.t() | nil) :: Path.t()
  def build!(plugin, stage, output, key) do
    release = MapSet.new(closure([:flezha | @release_extras]))
    bundled = Enum.reject(closure([plugin]), &MapSet.member?(release, &1))
    vsn = &to_string(Application.spec(&1, :vsn))

    manifest = %{
      id: Atom.to_string(plugin),
      version: vsn.(plugin),
      built_with: Manifest.runtime(),
      apps: Enum.map(bundled, &{&1, vsn.(&1)}),
      providers: providers!(plugin, Application.get_env(plugin, :flezha_providers))
    }

    File.rm_rf!(stage)

    try do
      Enum.each(bundled, &copy_app(&1, stage, plugin))
      File.write!(Path.join(stage, "manifest.exs"), Manifest.render(manifest))
      ok!(Package.check_contents(stage, manifest))
      checksums = Checksums.write(stage)
      if key, do: sign!(stage, checksums, key)

      File.mkdir_p!(output)
      archive = Path.join(output, "#{manifest.id}-#{manifest.version}.fpz")
      :ok = Package.create(stage, archive)
      archive
    after
      File.rm_rf!(stage)
    end
  end

  @doc false
  # A library the release ships at one version cannot be loaded at another: the
  # boot would refuse the package, so the build refuses `deps` against Flezha's
  # lock first.
  @spec check_locks!(map(), [Mix.Dep.t()]) :: :ok
  def check_locks!(flezha_lock, deps) do
    offenders =
      for dep <- deps,
          locked = flezha_lock[dep.app],
          resolved(dep) != pinned(locked),
          do:
            "  #{dep.app}: this project resolves #{resolved(dep)}, " <>
              "Flezha's mix.lock has #{pinned(locked)}; " <> pin_hint(dep.app, locked)

    if offenders != [] do
      Mix.raise("""
      the plugin resolves libraries the controller release ships at other versions:
      #{Enum.join(offenders, "\n")}
      """)
    end

    :ok
  end

  # A path dependency is what was built from it, whatever lock entry shares its
  # name; any other is its lock entry.
  defp resolved(%{scm: Mix.SCM.Path, app: app}) do
    _ = Application.load(app)
    to_string(Application.spec(app, :vsn))
  end

  defp resolved(%{opts: opts}), do: pinned(opts[:lock])

  defp pinned({:hex, _name, vsn, _hash, _managers, _deps, _repo, _outer}), do: vsn
  defp pinned({:git, _url, ref, _opts}), do: ref
  defp pinned(other), do: inspect(other)

  defp pin_hint(app, {:hex, _, vsn, _, _, _, _, _}),
    do: "pin it with {#{inspect(app)}, \"#{vsn}\", override: true}"

  defp pin_hint(app, {:git, url, ref, _}),
    do: "pin it with {#{inspect(app)}, git: \"#{url}\", ref: \"#{ref}\", override: true}"

  defp pin_hint(_app, _locked), do: "use the version Flezha's mix.lock names"

  # Every application `roots` need at run time, each after the one naming it.
  defp closure(roots) do
    {order, _seen} = Enum.reduce(roots, {[], MapSet.new()}, &visit/2)
    Enum.reverse(order)
  end

  defp visit(app, {order, seen}) do
    if MapSet.member?(seen, app) or not loadable?(app) do
      {order, seen}
    else
      spec = Application.spec(app)

      Enum.reduce(
        Keyword.get(spec, :applications, []) ++ Keyword.get(spec, :included_applications, []),
        {[app | order], MapSet.put(seen, app)},
        &visit/2
      )
    end
  end

  defp loadable?(app), do: Application.load(app) in [:ok, {:error, {:already_loaded, app}}]

  @doc false
  # Membership in the plugin application is the manifest's check, which the
  # staged package goes through; this one says what `providers`, the plugin's
  # `:flezha_providers`, must be.
  @spec providers!(atom(), term()) :: map()
  def providers!(plugin, providers) do
    case providers do
      %{} = providers when map_size(providers) > 0 ->
        for {id, module} <- providers, not is_binary(id) or not Providers.provider?(module) do
          Mix.raise(
            "the provider #{inspect(id)} => #{inspect(module)} of :flezha_providers is not a " <>
              "provider module of #{inspect(plugin)}: it must be one of its modules and " <>
              "`use Flezha.Plugin.ProviderBehaviour`"
          )
        end

        providers

      other ->
        Mix.raise(
          "#{inspect(plugin)} names no providers: its application environment must set " <>
            ":flezha_providers to a map of provider id to module, got #{inspect(other)}"
        )
    end
  end

  # `lib/<app>-<vsn>/ebin` and `priv/` as Mix built them, `priv/` followed
  # through the link Mix makes to the project's own, and a library's `.app`
  # carrying the project's configuration of it.
  defp copy_app(app, stage, plugin) do
    source = to_string(:code.lib_dir(app))
    target = Path.join([stage, "lib", "#{app}-#{Application.spec(app, :vsn)}"])
    ebin = Path.join(target, "ebin")
    File.mkdir_p!(ebin)

    for file <- File.ls!(Path.join(source, "ebin")),
        Path.extname(file) in [".beam", ".app"],
        do: File.cp!(Path.join([source, "ebin", file]), Path.join(ebin, file))

    if app != plugin,
      do: write_env!(app, Application.get_all_env(app), Path.join(ebin, "#{app}.app"))

    priv = Path.join(source, "priv")

    if File.dir?(priv),
      do: File.cp_r!(priv, Path.join(target, "priv"), dereference_symlinks: true)
  end

  @doc false
  # A library reads its defaults from its `.app`, and nothing else of the
  # project's configuration reaches the controller, so the loaded environment
  # `env` (the library's defaults under the project's settings) is written there.
  @spec write_env!(atom(), keyword(), Path.t()) :: :ok
  def write_env!(app, env, path) do
    env = Enum.sort(env)

    for {key, value} <- env, {:error, _term, reason} <- [Mix.Utils.consultable(value)] do
      Mix.raise(
        "the setting #{inspect(key)} of #{inspect(app)} cannot be written to its .app, " <>
          "where a package carries a library's settings: #{reason}"
      )
    end

    update_app(path, app, fn props ->
      if env == Enum.sort(Keyword.get(props, :env, [])),
        do: props,
        else: Keyword.put(props, :env, env)
    end)
  end

  @doc false
  # Pass the properties of the `.app` file at `path`, which describes `app`,
  # through `fun`, and write the file again, as Mix writes one, when they changed.
  @spec update_app(Path.t(), atom(), (keyword() -> keyword())) :: :ok
  def update_app(path, app, fun) do
    {:ok, [{:application, ^app, props}]} = :file.consult(String.to_charlist(path))

    with changed when changed != props <- fun.(props) do
      {:ok, contents} = Mix.Utils.consultable({:application, app, changed})
      File.write!(path, IO.chardata_to_string([contents, ".\n"]))
    end

    :ok
  end

  defp sign!(stage, checksums, key) do
    case Signature.sign(checksums, File.read!(key)) do
      {:ok, signature} -> File.write!(Path.join(stage, "signature.sig"), signature)
      {:error, _} -> Mix.raise("#{key} is not an Ed25519 private key in PEM form")
    end
  end

  defp ok!({:ok, value}), do: value

  defp ok!({:error, reason}),
    do: Mix.raise(Flezha.Error.render(Flezha.Error.from_reason(reason, nil), :text))
end
