defmodule Flezha.TestHelpers.PluginPackage do
  @moduledoc """
  The example plugin (`examples/elixir/plugin/`) as a real package, edits of it,
  and loading it into the test VM and out again.

    * `example!/0` packages the example once per test run with
      `mix flezha.plugin.package` in a subprocess (`MIX_ENV=prod`, the
      repository's `_build`, so only the two example apps compile) and answers
      the archive.
    * `rewrite/3` writes a copy of an archive with edits and a regenerated
      `CHECKSUMS.sha256`, so a refusal is one edit of the real package rather
      than a hand-built fixture.
    * `load!/2` places archives in a plugin root and runs the boot loader in this
      VM; `unload!/1` takes back what it loaded, which only a test may do.
  """

  alias Flezha.Plugin.Installer.Checksums
  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Installer.Manifest
  alias Flezha.Plugin.Installer.Package
  alias Flezha.Plugin.Providers

  @example "examples/elixir/plugin"
  @built {__MODULE__, :example}

  @doc "The example plugin's archive, packaged on first use in this test run."
  @spec example!() :: Path.t()
  def example!() do
    :global.trans({__MODULE__, self()}, fn ->
      case :persistent_term.get(@built, nil) do
        nil -> tap(package!(Path.expand(@example)), &:persistent_term.put(@built, &1))
        archive -> archive
      end
    end)
  end

  @doc """
  Package the plugin project in `dir` with the task, into a fresh directory, and
  answer `{exit_status, output, archive_or_nil}`. `args` are the task's options;
  `build_root` is the build root the project compiles in, the repository's by
  default.
  """
  @spec package(Path.t(), [binary()], Path.t()) :: {non_neg_integer(), binary(), Path.t() | nil}
  def package(dir, args \\ [], build_root \\ Path.expand("_build")) do
    output = Path.join(System.tmp_dir!(), "flz_pkg_#{System.unique_integer([:positive])}")
    # The integer repeats across test runs, and an archive an earlier run left
    # there would read as this run's.
    File.rm_rf!(output)

    {out, status} =
      System.cmd("mix", ["flezha.plugin.package", "--output", output | args],
        cd: dir,
        env: [{"MIX_ENV", "prod"}, {"MIX_BUILD_ROOT", build_root}],
        stderr_to_stdout: true
      )

    {status, out, output |> Path.join("*.fpz") |> Path.wildcard() |> List.first()}
  end

  defp package!(dir) do
    case package(dir) do
      {0, _out, archive} when is_binary(archive) -> archive
      {status, out, _} -> raise "packaging #{dir} failed (#{status}):\n#{out}"
    end
  end

  @doc """
  Write `archive` with `edits` applied to `target`, then regenerate its
  checksums (unless an edit is `:keep_checksums`). An edit is one of:

    * `{:manifest, fun}` - the manifest map through `fun`
    * `{:app, app, fun}` - the `lib/<app>-<vsn>/ebin/<app>.app` properties through `fun`
    * `{:put, path, bytes}` - a file written or replaced
    * `{:delete, path}` - a file or directory removed
    * `{:beam, app, module, bytes}` - a beam added to `app` and to its `modules`
    * `{:app_version, app, vsn}` - `app` moved to `lib/<app>-<vsn>` at that version
    * `:keep_checksums` - the original `CHECKSUMS.sha256`, unregenerated
  """
  @spec rewrite(Path.t(), Path.t(), list()) :: Path.t()
  def rewrite(archive, target, edits) do
    dir = Path.join(System.tmp_dir!(), "flz_rewrite_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    :ok =
      :erl_tar.extract(String.to_charlist(archive), [:compressed, {:cwd, String.to_charlist(dir)}])

    Enum.each(edits, &edit(dir, &1))

    unless :keep_checksums in edits, do: Checksums.write(dir)

    File.mkdir_p!(Path.dirname(target))
    :ok = Package.create(dir, target)
    File.rm_rf!(dir)
    target
  end

  defp edit(_dir, :keep_checksums), do: :ok

  defp edit(dir, {:manifest, fun}) do
    {:ok, manifest} = Manifest.read(dir)
    File.write!(Path.join(dir, "manifest.exs"), Manifest.render(fun.(manifest)))
  end

  defp edit(dir, {:app, app, fun}),
    do: Mix.Tasks.Flezha.Plugin.Package.update_app(app_file(dir, app), app, fun)

  defp edit(dir, {:put, path, bytes}) do
    full = Path.join(dir, path)
    File.mkdir_p!(Path.dirname(full))
    File.write!(full, bytes)
  end

  defp edit(dir, {:delete, path}), do: File.rm_rf!(Path.join(dir, path))

  defp edit(dir, {:beam, app, module, bytes}) do
    File.write!(Path.join(Path.dirname(app_file(dir, app)), "#{module}.beam"), bytes)
    edit(dir, {:app, app, &Keyword.update!(&1, :modules, fn mods -> mods ++ [module] end)})
  end

  defp edit(dir, {:app_version, app, vsn}) do
    [from] = Path.wildcard(Path.join([dir, "lib", "#{app}-*"]))
    File.rename!(from, Path.join([dir, "lib", "#{app}-#{vsn}"]))
    edit(dir, {:app, app, &Keyword.put(&1, :vsn, String.to_charlist(vsn))})

    edit(
      dir,
      {:manifest,
       fn m ->
         m = %{m | apps: List.keyreplace(m.apps, app, 0, {app, vsn})}
         if hd(m.apps) == {app, vsn}, do: %{m | version: vsn}, else: m
       end}
    )
  end

  defp app_file(dir, app) do
    [path] = Path.wildcard(Path.join([dir, "lib", "#{app}-*", "ebin", "#{app}.app"]))
    path
  end

  @doc """
  A second package written to `target`: plugin application `id` with one
  provider, `"<id>_stamp"`, and the example's `stamp_lib` at the same version,
  with `edits` applied after. `body` is the provider module's code after its
  `use`, an `on_output/2` that passes the payload through by default.
  """
  @spec sibling(Path.t(), binary(), list(), binary()) :: Path.t()
  def sibling(target, id, edits \\ [], body \\ "def on_output(ctx, _opts), do: {:ok, ctx}") do
    app = String.to_atom(id)
    provider = Module.concat([Macro.camelize(id), Provider])

    [{^provider, beam}] =
      compile_unloaded("""
      defmodule #{inspect(provider)} do
        use Flezha.Plugin.ProviderBehaviour

        #{body}
      end
      """)

    spec =
      {:application, app,
       [
         vsn: ~c"0.1.0",
         modules: [provider],
         applications: [:kernel, :stdlib, :elixir, :flezha, :stamp_lib]
       ]}

    rewrite(
      example!(),
      target,
      [
        {:delete, "lib/flezha_example_plugin-0.1.0"},
        {:put, "lib/#{id}-0.1.0/ebin/#{id}.app", :io_lib.format(~c"~p.~n", [spec])},
        {:put, "lib/#{id}-0.1.0/ebin/#{provider}.beam", beam},
        {:manifest,
         &%{
           &1
           | id: id,
             apps: [{app, "0.1.0"}, {:stamp_lib, "0.1.0"}],
             providers: %{"#{id}_stamp" => provider}
         }}
      ] ++ edits
    )
  end

  @doc """
  A beam of an Erlang module named `module`, compiled and never loaded. With
  `on_load: true` it declares an `on_load` function; `start_error: reason` makes
  it an application callback whose `start/2` answers `{:error, reason}`.
  """
  @spec beam(module(), keyword()) :: binary()
  def beam(module, opts \\ []) do
    forms =
      [{:attribute, 1, :module, module}, {:attribute, 1, :export, [init: 0, start: 2, stop: 1]}] ++
        if(opts[:on_load], do: [{:attribute, 1, :on_load, {:init, 0}}], else: []) ++
        [
          {:function, 1, :init, 0, [{:clause, 1, [], [], [{:atom, 1, :ok}]}]},
          {:function, 1, :start, 2,
           [
             {:clause, 1, [{:var, 1, :_}, {:var, 1, :_}], [],
              [{:tuple, 1, [{:atom, 1, :error}, {:atom, 1, opts[:start_error] || :refused}]}]}
           ]},
          {:function, 1, :stop, 1, [{:clause, 1, [{:var, 1, :_}], [], [{:atom, 1, :ok}]}]}
        ]

    {:ok, ^module, binary} = :compile.forms(forms, [:binary])
    binary
  end

  @doc "The beam of `module` with its code chunk corrupted: readable, but no loader takes it."
  @spec corrupt_beam(module()) :: binary()
  def corrupt_beam(module) do
    {:ok, ^module, chunks} = :beam_lib.all_chunks(beam(module))

    {:ok, binary} =
      :beam_lib.build_module(
        for({~c"Code", _} <- chunks, do: {~c"Code", "junk"}) ++
          Enum.reject(chunks, &match?({~c"Code", _}, &1))
      )

    binary
  end

  @doc """
  Every Elixir module `source` defines, compiled and then taken back out of the
  VM, as `[{module, beam}]`: code a package carries and the VM does not hold.
  """
  @spec compile_unloaded(binary()) :: [{module(), binary()}]
  def compile_unloaded(source) do
    for {module, binary} <- Code.compile_string(source) do
      :code.purge(module)
      :code.delete(module)
      :code.purge(module)
      {module, binary}
    end
  end

  @doc """
  Place `archives` in the plugin root `root` (configured as the plugin root) and
  run the boot loader. Answers its outcomes.
  """
  @spec load!(Path.t(), [Path.t()]) :: [Loader.outcome()]
  def load!(root, archives) do
    Application.put_env(
      :flezha,
      :plugins,
      Keyword.put(Application.get_env(:flezha, :plugins, []), :plugin_root, root)
    )

    File.mkdir_p!(root)
    for archive <- archives, do: File.cp!(archive, Path.join(root, Path.basename(archive)))
    Loader.load_all()
  end

  @doc """
  Take back everything a `load!/2` over `root` added to this VM, whether its
  package was loaded or refused after its modules loaded: started applications,
  application specs, modules, code paths, re-consolidated protocols, provider
  rows and outcomes.
  """
  @spec unload!(Path.t()) :: :ok
  def unload!(root) do
    under? = &List.starts_with?(&1, String.to_charlist(Path.expand(root)))
    modules = for {module, path} <- :code.all_loaded(), is_list(path), under?.(path), do: module

    protocols =
      for module <- modules,
          {:__impl__, impl} <- module.module_info(:attributes),
          uniq: true,
          do: impl[:protocol]

    for {app, _, _} <- Application.loaded_applications(),
        {:ok, owned} <- [:application.get_key(app, :modules)],
        Enum.any?(owned, &(&1 in modules)) do
      _ = Application.stop(app)
      _ = :application.unload(app)
    end

    for path <- :code.get_path(), under?.(path), do: :code.del_path(path)

    for module <- modules do
      :code.purge(module)
      :code.delete(module)
      :code.purge(module)
    end

    for protocol <- protocols do
      :code.purge(protocol)
      {:module, ^protocol} = :code.load_file(protocol)
    end

    Providers.reset_installed()
    Loader.reset()
  end
end
