defmodule Flezha.TestHelpers.PluginPackage do
  @moduledoc """
  The example plugin (`test/apps/example_plugin/`) as a real package, edits of it,
  and loading it into the test VM and out again.

    * `example!/0` packages the example once per test run and answers the
      archive; `package!/3` packages a plugin project the way
      `mix flezha.plugin.package` does after it compiles, in this VM.
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
  alias FlezhaTest.Helper.NodeTestHelper
  alias Mix.Tasks.Flezha.Plugin.Package, as: PackageTask

  @example "test/apps/example_plugin"
  @built {__MODULE__, :example}

  @doc "The example plugin's archive, packaged on first use in this test run."
  @spec example!() :: Path.t()
  def example!() do
    :global.trans({__MODULE__, self()}, fn ->
      case :persistent_term.get(@built, nil) do
        nil ->
          out = NodeTestHelper.claim_tmp_dir("flz_example")

          ExUnit.after_suite(fn _ ->
            :persistent_term.erase(@built)
            File.rm_rf!(out)
          end)

          tap(package!(@example, out), &:persistent_term.put(@built, &1))

        archive ->
          archive
      end
    end)
  end

  @doc """
  Package the plugin project in `dir` into `output` with the task's own
  staging, signed with the key file `key` when given, and answer the archive.
  The project and the path dependencies this VM does not run are compiled into
  a scratch directory and loaded with the project's configuration of them, as
  the task finds them after `compile`, then taken back out of the VM.
  """
  @spec package!(Path.t(), Path.t(), Path.t() | nil) :: Path.t()
  def package!(dir, output, key \\ nil),
    do: :global.trans({__MODULE__, self()}, fn -> stage_package!(dir, output, key) end)

  # The project's modules and applications are the VM's while it is packaged,
  # so one packaging runs at a time.
  defp stage_package!(dir, output, key) do
    root = NodeTestHelper.claim_tmp_dir("flz_pkg")
    dir = Path.expand(dir)
    apps = compile_project!(dir, Path.join(root, "lib"))
    plugin = List.last(apps)

    try do
      for {app, _} <- apps, do: :ok = Application.load(app)

      for {app, env} <- Config.Reader.read!(Path.join(dir, "config/config.exs"), env: :prod),
          List.keymember?(apps, app, 0),
          do: Application.put_all_env([{app, env}])

      PackageTask.build!(elem(plugin, 0), Path.join(root, "stage"), output, key)
    after
      for {app, modules} <- apps do
        _ = :application.unload(app)
        :code.del_path(String.to_charlist(Path.join([root, "lib", "#{app}", "ebin"])))
        Enum.each(modules, &drop_module/1)
      end

      File.rm_rf!(root)
    end
  end

  # The project in `dir` and, first, each path dependency the VM does not
  # already run, compiled into `lib/<app>/ebin` under `lib` with the `.app`
  # Mix writes and the project's `priv/`. Answers `[{app, modules}]`, the
  # project last.
  defp compile_project!(dir, lib) do
    {project, application} = mix_project(dir)

    deps =
      for {app, opts} <- Keyword.get(project, :deps, []),
          opts[:path],
          do: {app, Path.expand(opts[:path], dir)}

    built =
      for {app, path} <- deps, Application.spec(app) == nil, reduce: [] do
        acc -> acc ++ compile_project!(path, lib)
      end

    app = project[:app]
    target = Path.join(lib, "#{app}")
    ebin = Path.join(target, "ebin")
    File.mkdir_p!(ebin)
    sources = Path.wildcard(Path.join(dir, "lib/**/*.ex"))

    modules = compile_to!(sources, ebin)

    spec =
      {:application, app,
       [
         modules: Enum.sort(modules),
         applications:
           [:kernel, :stdlib, :elixir] ++
             Keyword.get(application, :extra_applications, []) ++ Keyword.keys(deps),
         vsn: String.to_charlist(project[:version])
       ] ++ Keyword.take(application, [:mod, :env])}

    File.write!(Path.join(ebin, "#{app}.app"), :io_lib.format(~c"~p.~n", [spec]))

    if File.dir?(Path.join(dir, "priv")),
      do: File.ln_s!(Path.join(dir, "priv"), Path.join(target, "priv"))

    true = :code.add_patha(String.to_charlist(ebin))
    built ++ [{app, modules}]
  end

  # The VM's protocols are consolidated; a package's implementations are
  # consolidated again when it loads.
  defp compile_to!(sources, ebin) do
    ignored = Code.get_compiler_option(:ignore_already_consolidated)
    Code.put_compiler_option(:ignore_already_consolidated, true)

    try do
      {:ok, modules, _} =
        Kernel.ParallelCompiler.compile_to_path(sources, ebin, return_diagnostics: true)

      modules
    after
      Code.put_compiler_option(:ignore_already_consolidated, ignored)
    end
  end

  # `project/0` and `application/0` of the `mix.exs` in `dir`, compiled without
  # `use Mix.Project`, which would push it onto Mix's project stack.
  defp mix_project(dir) do
    file = Path.join(dir, "mix.exs")
    {:defmodule, meta, [_name, [do: body]]} = file |> File.read!() |> Code.string_to_quoted!()

    body =
      Macro.prewalk(body, fn
        {:use, _, [{:__aliases__, _, [:Mix, :Project]}]} -> nil
        node -> node
      end)

    module = Module.concat(__MODULE__, "Project#{System.unique_integer([:positive])}")
    [{^module, _}] = Code.compile_quoted({:defmodule, meta, [module, [do: body]]}, file)

    try do
      {module.project(), module.application()}
    after
      drop_module(module)
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
    do: PackageTask.update_app(app_file(dir, app), app, fun)

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
      drop_module(module)
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

    Enum.each(modules, &drop_module/1)

    for protocol <- protocols do
      :code.purge(protocol)
      {:module, ^protocol} = :code.load_file(protocol)
    end

    Providers.reset_installed()
    Loader.reset()
  end

  defp drop_module(module) do
    :code.purge(module)
    :code.delete(module)
    :code.purge(module)
  end
end
