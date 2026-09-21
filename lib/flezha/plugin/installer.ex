defmodule Flezha.Plugin.Installer do
  @moduledoc """
  Public service API for installable plugins. Backed by a GenServer that
  serializes every mutating tasklet (install, upgrade, uninstall,
  enable, disable) so the code server and DETS catalog stay consistent.

  Read-only tasklets (`list/0`, `info/1`, `installed_ids/0`) bypass the
  GenServer — they only read the Loader's in-memory registry or the Store
  DETS, both of which are safe for concurrent readers.
  """

  use GenServer

  require Logger

  alias Flezha.Config
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Plugin
  alias Flezha.Plugin.Installer.Checksums
  alias Flezha.Plugin.Installer.Compiler
  alias Flezha.Plugin.Installer.DependencyResolver
  alias Flezha.Plugin.Installer.InstalledPlugin
  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Installer.Manifest
  alias Flezha.Plugin.Installer.Package
  alias Flezha.Plugin.Installer.Signature
  alias Flezha.Plugin.Installer.Store
  alias Flezha.Service.PluginManager

  @type install_result ::
          {:ok, InstalledPlugin.t()} | {:error, term()}

  ## Public API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Install a plugin from a `.fpz` archive."
  @spec install(Path.t(), keyword()) :: install_result()
  def install(path, opts \\ []) do
    GenServer.call(__MODULE__, {:install, path, opts}, 120_000)
  end

  @doc "Upgrade an installed plugin to a new version in a `.fpz`."
  @spec upgrade(binary(), Path.t(), keyword()) :: install_result()
  def upgrade(plugin_id, new_path, opts \\ []) do
    GenServer.call(__MODULE__, {:upgrade, plugin_id, new_path, opts}, 120_000)
  end

  @doc "Uninstall a plugin. Refuses when enabled or when dependents exist."
  @spec uninstall(binary(), keyword()) :: :ok | {:error, term()}
  def uninstall(plugin_id, opts \\ []) do
    GenServer.call(__MODULE__, {:uninstall, plugin_id, opts}, 60_000)
  end

  @doc "Enable a disabled installed plugin. Subscribes every provision."
  @spec enable(binary(), keyword()) :: :ok | {:error, term()}
  def enable(plugin_id, opts \\ []) do
    GenServer.call(__MODULE__, {:enable, plugin_id, opts}, 30_000)
  end

  @doc "Disable an enabled installed plugin. Unsubscribes; keeps modules loaded."
  @spec disable(binary(), keyword()) :: :ok | {:error, term()}
  def disable(plugin_id, opts \\ []) do
    GenServer.call(__MODULE__, {:disable, plugin_id, opts}, 30_000)
  end

  @doc "All installed plugins projected to `%InstalledPlugin{}`."
  @spec list() :: [InstalledPlugin.t()]
  def list do
    Store.all() |> Enum.map(&InstalledPlugin.from_row/1)
  end

  @doc "Single installed plugin or `{:error, :not_found}`."
  @spec info(binary()) :: {:ok, InstalledPlugin.t()} | {:error, :not_found}
  def info(plugin_id) when is_binary(plugin_id) do
    case Store.get(plugin_id) do
      nil -> {:error, :not_found}
      row -> {:ok, InstalledPlugin.from_row({plugin_id, row})}
    end
  end

  @doc "Set of ids the installer owns in the VM. Returns empty set if not started."
  @spec installed_ids() :: MapSet.t(binary())
  def installed_ids do
    case Process.whereis(Loader) do
      nil ->
        MapSet.new()

      _ ->
        Loader.installed_ids()
        |> Enum.into(MapSet.new())
    end
  end

  ## GenServer

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_call({:install, path, opts}, _from, state) do
    {:reply, do_install(path, opts), state}
  end

  def handle_call({:upgrade, id, path, opts}, _from, state) do
    {:reply, do_upgrade(id, path, opts), state}
  end

  def handle_call({:uninstall, id, opts}, _from, state) do
    {:reply, do_uninstall(id, opts), state}
  end

  def handle_call({:enable, id, opts}, _from, state) do
    {:reply, do_enable(id, opts), state}
  end

  def handle_call({:disable, id, opts}, _from, state) do
    {:reply, do_disable(id, opts), state}
  end

  ## Install pipeline

  # Each install stores rollback actions in the process dictionary under a
  # unique key so the `else` branch of the `with` block — which can't see
  # `with`-block bindings in Elixir — can still recover the full list.
  @rollback_key :flz_installer_rollback

  defp do_install(path, opts) do
    authorise = &authorise_install(&1, opts)

    with_staging(fn staging ->
      with {:ok, staged} <- stage_and_vet(path, staging, &noop_vet/1, authorise),
           {:ok, plugin} <- finish_install(staged) do
        {:ok, plugin}
      else
        {:error, reason} ->
          run_rollback()
          {:error, reason}
      end
    end)
  end

  defp authorise_install(manifest, opts) do
    with :ok <- refuse_if_already_installed(manifest.id, opts) do
      others = Enum.reject(DependencyResolver.live_catalog(), fn {id, _} -> id == manifest.id end)
      DependencyResolver.validate_install(manifest, others)
    end
  end

  defp finish_install(staged) do
    %{manifest: manifest, manifest_src: manifest_src, beams: beams} = staged
    plugin_id = manifest.id
    ebin_dir = Path.join(staged.install_dir, "ebin")

    with :ok <- Loader.load_beams(plugin_id, manifest.version, ebin_dir, beams),
         :ok <- push_rollback({:purge, plugin_id}),
         row = build_row(manifest, staged.install_dir, beams, manifest_src, staged.staging),
         :ok <- Store.put(plugin_id, row),
         :ok <- push_rollback({:delete_store_row, plugin_id}) do
      {:ok, InstalledPlugin.from_row({plugin_id, row})}
    end
  end

  defp noop_vet(_manifest), do: :ok

  # The staging bracket both pipelines run inside: one rollback stack per
  # attempt, and the staging tree removed however the attempt ends.
  defp with_staging(fun) do
    staging = stage_path()
    Process.put(@rollback_key, [])
    push_rollback({:rm_dir, staging})

    result = fun.(staging)

    File.rm_rf(staging)
    Process.delete(@rollback_key)
    result
  end

  # Extract, read, validate, verify, resolve, compile and write — the fifteen
  # steps install and upgrade share. `vet` runs against the parsed manifest,
  # `authorise` after the signature is accepted; the two pipelines differ only
  # in what they put there. Both run at the position they occupied before, so
  # a package that fails two checks still reports the same one.
  defp stage_and_vet(path, staging, vet, authorise) do
    with :ok <- Package.extract(path, staging, package_opts()),
         :ok <- Package.validate_layout(staging),
         {:ok, manifest_src} <- File.read(Path.join(staging, "manifest.exs")),
         {:ok, manifest} <- Manifest.parse(manifest_src),
         :ok <- Manifest.validate(manifest),
         :ok <- vet.(manifest),
         :ok <- verify_checksums(staging),
         {:ok, signature_bytes, signature_present?} <- load_signature(staging),
         :ok <-
           Signature.gate(
             signature_present?: signature_present?,
             require_signature: Config.get(:plugin_require_signature),
             allow_unsigned: Config.get(:plugin_allow_unsigned),
             plugin_id: manifest.id
           ),
         :ok <- maybe_verify_signature(staging, signature_bytes),
         :ok <- authorise.(manifest),
         :ok <- check_module_collisions(manifest.modules, manifest.id),
         {:ok, beams} <- Compiler.compile(Path.join(staging, "src"), manifest.modules),
         install_dir = install_dir_for(manifest.id, manifest.version),
         :ok <- push_rollback({:rm_dir, install_dir}),
         :ok <- write_install_dir(staging, install_dir) do
      {:ok,
       %{
         manifest: manifest,
         manifest_src: manifest_src,
         beams: beams,
         install_dir: install_dir,
         staging: staging
       }}
    end
  end

  defp refuse_if_already_installed(plugin_id, opts) do
    if Keyword.get(opts, :allow_reinstall, false) do
      :ok
    else
      case Store.get(plugin_id) do
        nil -> :ok
        _ -> {:error, {:already_installed, plugin_id}}
      end
    end
  end

  ## Upgrade

  defp do_upgrade(plugin_id, new_path, opts) do
    case Store.get(plugin_id) do
      nil ->
        {:error, :not_found}

      old_row ->
        old_modules = Loader.modules_of(plugin_id)
        old_status = old_row[:status]

        with_staging(fn staging ->
          stage_upgrade(new_path, staging, plugin_id, old_row, old_status, old_modules, opts)
        end)
    end
  end

  defp stage_upgrade(new_path, staging, plugin_id, old_row, old_status, old_modules, opts) do
    vet = &ensure_ids_match(&1.id, plugin_id)

    authorise =
      &DependencyResolver.validate_upgrade(&1, plugin_id, DependencyResolver.live_catalog())

    case stage_and_vet(new_path, staging, vet, authorise) do
      {:ok, staged} ->
        upgraded(staged, plugin_id, old_row, old_status, old_modules, opts)

      {:error, reason} ->
        restore_old_version(plugin_id, old_row, old_status, old_modules)
        {:error, reason}
    end
  end

  defp upgraded(staged, plugin_id, old_row, old_status, old_modules, opts) do
    case finish_upgrade(staged, plugin_id, old_row, old_status, old_modules, opts) do
      {:ok, plugin} ->
        {:ok, plugin}

      {:error, reason} ->
        restore_old_version(plugin_id, old_row, old_status, old_modules)
        {:error, reason}
    end
  end

  defp restore_old_version(plugin_id, old_row, old_status, old_modules) do
    run_rollback()
    _ = Store.put(plugin_id, old_row)

    if old_status == :enabled and old_modules != [] do
      reload_old_modules(plugin_id, old_row, old_modules)
      _ = maybe_reenable(plugin_id, old_row)
    end

    :ok
  end

  defp finish_upgrade(staged, plugin_id, old_row, old_status, old_modules, opts) do
    %{manifest: manifest, manifest_src: manifest_src, beams: beams} = staged
    new_dir = staged.install_dir

    pending_row =
      manifest
      |> build_row(new_dir, beams, manifest_src, staged.staging)
      |> Map.put(:status, :pending_upgrade)

    with :ok <- Store.put(plugin_id, pending_row),
         :ok <- hot_swap_modules(plugin_id, old_status, old_modules, beams, new_dir, opts) do
      # Upgrade succeeded. Collapse pending_upgrade to the real status.
      final_status = if old_status == :enabled, do: :enabled, else: :disabled
      :ok = Store.update_status(plugin_id, final_status)

      # Delete old version directory.
      old_dir = old_row[:path]
      if old_dir && old_dir != new_dir, do: File.rm_rf(old_dir)

      {:ok, InstalledPlugin.from_row({plugin_id, Store.get(plugin_id)})}
    end
  end

  defp ensure_ids_match(new_id, existing_id) when new_id == existing_id, do: :ok

  defp ensure_ids_match(new_id, existing_id),
    do: {:error, {:id_mismatch, new_id, existing_id}}

  # Hot-swap sequence: disable the live plugins, quiesce in-flight tasks,
  # purge old modules, load new modules, re-subscribe with new manifest.
  defp hot_swap_modules(plugin_id, old_status, _old_modules, new_beams, new_dir, _opts) do
    if old_status == :enabled do
      _ = disable_subscriptions(plugin_id)
      quiesce()
    end

    _ = Loader.purge_modules(plugin_id)

    ebin_dir = Path.join(new_dir, "ebin")

    case Loader.load_beams(plugin_id, Store.get(plugin_id)[:version], ebin_dir, new_beams) do
      :ok ->
        if old_status == :enabled do
          # Re-subscribe according to the NEW manifest's provides.
          new_row = Store.get(plugin_id)
          enable_provisions(plugin_id, new_row)
        else
          :ok
        end

      err ->
        err
    end
  catch
    kind, reason -> {:error, {:upgrade_crash, kind, reason}}
  end

  defp reload_old_modules(plugin_id, old_row, old_modules) do
    ebin_dir = Path.join(old_row[:path] || "", "ebin")

    Enum.each(old_modules, fn module ->
      beam_path = Path.join(ebin_dir, "#{module}.beam")

      case File.read(beam_path) do
        {:ok, bin} ->
          _ = :code.load_binary(module, String.to_charlist(beam_path), bin)

        _ ->
          Logger.warning(
            "[plugin_installer] could not reload old beam for #{plugin_id}/#{module} during rollback"
          )
      end
    end)

    # Record the restored set in the Loader registry.
    Loader.load_beams(plugin_id, old_row[:version], ebin_dir, [])
  end

  # Wait up to 5s for in-flight :request tasks to finish. Snapshot the
  # current children of Flezha.Operative.MessageHandlerTaskSup, monitor
  # each, then drain `:DOWN` messages — no polling. New tasks spawned
  # after the snapshot are not waited on (callers serialize plugin
  # mutations upstream, so this window is small in practice).
  defp quiesce do
    case Process.whereis(Flezha.Operative.MessageHandlerTaskSup) do
      nil ->
        :ok

      _pid ->
        try do
          Flezha.Operative.MessageHandlerTaskSup
          |> Task.Supervisor.children()
          |> Enum.map(&Process.monitor/1)
          |> drain_downs(System.monotonic_time(:millisecond) + 5_000)
        catch
          _, _ -> :ok
        end
    end
  end

  defp drain_downs([], _deadline), do: :ok

  defp drain_downs(refs, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:DOWN, ref, :process, _pid, _reason} ->
        drain_downs(List.delete(refs, ref), deadline)
    after
      remaining ->
        Enum.each(refs, &Process.demonitor(&1, [:flush]))
        :ok
    end
  end

  ## Uninstall

  defp do_uninstall(plugin_id, _opts) do
    case Store.get(plugin_id) do
      nil ->
        {:error, :not_found}

      %{status: :enabled} ->
        {:error, :plugin_enabled}

      row ->
        dependents = DependencyResolver.find_dependents(plugin_id, Store.all())

        if dependents != [] do
          {:error, {:dependents_block, dependents}}
        else
          _ = Loader.purge_modules(plugin_id)
          _ = row[:path] && File.rm_rf(plugin_root_for(plugin_id))
          :ok = Store.delete(plugin_id)
          :ok
        end
    end
  end

  ## Enable / disable

  defp do_enable(plugin_id, opts) do
    case Store.get(plugin_id) do
      nil ->
        {:error, :not_found}

      %{status: :enabled} ->
        {:error, :already_enabled}

      %{status: :load_failed} ->
        {:error, :load_failed}

      row ->
        case enable_provisions(plugin_id, row, opts) do
          :ok ->
            :ok = Store.update_status(plugin_id, :enabled)
            :ok

          err ->
            err
        end
    end
  end

  defp do_disable(plugin_id, _opts) do
    case Store.get(plugin_id) do
      nil ->
        {:error, :not_found}

      %{status: :disabled} ->
        :ok

      %{status: :load_failed} ->
        :ok

      _row ->
        _ = disable_subscriptions(plugin_id)
        :ok = Store.update_status(plugin_id, :disabled)
        :ok
    end
  end

  defp enable_provisions(plugin_id, row, opts \\ []) do
    overrides = Keyword.get(opts, :overrides, %{})
    provisions = Map.get(row, :provides, [])

    provisions
    |> Enum.reduce_while({:ok, []}, fn provision, {:ok, subscribed} ->
      plugin_struct = build_plugin_struct(plugin_id, provision, overrides, opts)
      live_id = provision_live_id(plugin_id, provision)

      case safe_enable_plugin(live_id, plugin_struct) do
        {:ok, _msg} ->
          {:cont, {:ok, [live_id | subscribed]}}

        err ->
          Enum.each(subscribed, &safe_disable_plugin/1)
          {:halt, err}
      end
    end)
    |> case do
      {:ok, _} -> :ok
      other -> other
    end
  end

  # PluginManager.enable_plugin/2 may crash when downstream services
  # (OperativeManager, ProcessHub) aren't running — that's a configuration
  # bug, not an install bug, but we owe callers a structured error.
  defp safe_enable_plugin(live_id, plugin_struct) do
    PluginManager.enable_plugin(live_id, plugin_struct)
  catch
    :exit, reason -> {:error, {:plugin_manager_unavailable, reason}}
    kind, reason -> {:error, {:plugin_manager_crash, kind, reason}}
  end

  defp safe_disable_plugin(live_id) do
    PluginManager.disable_plugin(live_id)
  catch
    _kind, _reason -> :ok
  end

  defp disable_subscriptions(plugin_id) do
    provisions =
      case Store.get(plugin_id) do
        %{} = row -> Map.get(row, :provides, [])
        _ -> []
      end

    Enum.each(provisions, fn provision ->
      safe_disable_plugin(provision_live_id(plugin_id, provision))
    end)

    :ok
  end

  defp maybe_reenable(plugin_id, old_row) do
    enable_provisions(plugin_id, old_row)
  end

  # Stable identifier for a provision subscription in PluginManager. We use
  # `<plugin_id>:<provision_id>` so multiple provisions coexist per package.
  defp provision_live_id(plugin_id, %{id: provision_id}) when is_binary(provision_id),
    do: "#{plugin_id}:#{provision_id}"

  defp provision_live_id(plugin_id, _), do: plugin_id

  defp build_plugin_struct(plugin_id, provision, overrides, opts) do
    default_opts = Map.get(provision, :default_opts, %{})
    merged_opts = Map.merge(default_opts, Map.get(overrides, provision[:id], %{}))

    %Plugin{
      id: provision_live_id(plugin_id, provision),
      provider: provision[:provider],
      namespace: Keyword.get(opts, :namespace) || Map.get(merged_opts, :namespace),
      operative_id: Keyword.get(opts, :operative_id) || Map.get(merged_opts, :operative_id),
      opts: merged_opts,
      hooks: provision[:hooks]
    }
  end

  ## Row construction

  defp build_row(manifest, install_dir, beams, manifest_src, staging_dir) do
    modules = Enum.map(beams, fn {m, _} -> m end)

    %{
      version: manifest.version,
      status: :disabled,
      path: install_dir,
      manifest_digest: :crypto.hash(:sha256, manifest_src) |> Base.encode16(case: :lower),
      modules: modules,
      installed_at: System.monotonic_time(),
      source_checksums: read_checksums_map(staging_dir),
      description: Map.get(manifest, :description),
      author: Map.get(manifest, :author),
      provides: Map.get(manifest, :provides, []),
      dependencies: Map.get(manifest, :dependencies, [])
    }
  end

  defp read_checksums_map(staging_dir) do
    path = Path.join(staging_dir, "CHECKSUMS.sha256")

    with {:ok, body} <- File.read(path),
         {:ok, entries} <- Checksums.parse(body) do
      Map.new(entries, fn %{path: p, digest: d} -> {p, d} end)
    else
      _ -> %{}
    end
  end

  ## Filesystem

  defp write_install_dir(staging, install_dir) do
    if File.exists?(install_dir) do
      File.rm_rf!(install_dir)
    end

    File.mkdir_p!(install_dir)

    for entry <- ["manifest.exs", "CHECKSUMS.sha256", "signature.sig"] do
      from = Path.join(staging, entry)

      if File.regular?(from) do
        File.cp!(from, Path.join(install_dir, entry))
      end
    end

    src_from = Path.join(staging, "src")

    if File.dir?(src_from) do
      File.cp_r!(src_from, Path.join(install_dir, "src"))
    end

    File.write!(
      Path.join(install_dir, "installed_at"),
      Integer.to_string(System.monotonic_time())
    )

    :ok
  end

  defp plugin_root_for(plugin_id) do
    Layout.file(:plugins, ["installed", plugin_id])
  end

  defp install_dir_for(plugin_id, version) do
    Layout.file(:plugins, ["installed", plugin_id, version])
  end

  defp stage_path() do
    Layout.file(:plugins, [
      "staging",
      Base.url_encode64(:crypto.strong_rand_bytes(8), padding: false)
    ])
  end

  defp package_opts() do
    [
      max_package_bytes: Config.get(:plugin_max_package_bytes),
      max_source_files: Config.get(:plugin_max_source_files)
    ]
  end

  defp verify_checksums(staging_dir) do
    with {:ok, body} <- File.read(Path.join(staging_dir, "CHECKSUMS.sha256")),
         {:ok, entries} <- Checksums.parse(body) do
      Checksums.verify(entries, staging_dir)
    end
  end

  defp load_signature(staging_dir) do
    path = Path.join(staging_dir, "signature.sig")

    case File.read(path) do
      {:ok, bin} when byte_size(bin) == 64 -> {:ok, bin, true}
      {:ok, _bad} -> {:error, :signature_invalid}
      {:error, :enoent} -> {:ok, nil, false}
      {:error, reason} -> {:error, {:signature_io, reason}}
    end
  end

  defp maybe_verify_signature(_staging, nil), do: :ok

  defp maybe_verify_signature(staging_dir, sig) do
    checksums_path = Path.join(staging_dir, "CHECKSUMS.sha256")

    with {:ok, msg} <- File.read(checksums_path) do
      Signature.verify(msg, sig, Config.get(:plugin_trusted_keys))
    end
  end

  ## Module-collision check — runs BEFORE compile
  #
  # `Code.compile_file/1` loads its output into the code server. If a plugin
  # tries to shadow a module already present (stdlib like `Enum`, or another
  # plugin's module, or a release module), the compile would silently
  # replace the live module — and our post-compile purge would then unload
  # the wrong one. Catch every shadow attempt up front.

  defp check_module_collisions(modules, plugin_id) do
    Enum.reduce_while(modules, :ok, fn module, _ ->
      cond do
        owned_by_other_plugin?(module, plugin_id) ->
          {:halt, {:error, {:module_collision, module}}}

        Loader.module_from_release?(module) ->
          {:halt, {:error, {:module_collision, module}}}

        true ->
          {:cont, :ok}
      end
    end)
  end

  defp owned_by_other_plugin?(module, plugin_id) do
    case Process.whereis(Loader) do
      nil ->
        false

      _ ->
        Loader.all()
        |> Enum.any?(fn
          {^plugin_id, _} -> false
          {_other, {_ver, mods}} -> module in mods
        end)
    end
  end

  ## Rollback

  defp push_rollback(action) do
    Process.put(@rollback_key, [action | Process.get(@rollback_key, [])])
    :ok
  end

  defp run_rollback do
    Process.get(@rollback_key, [])
    |> Enum.each(fn
      {:rm_dir, dir} -> File.rm_rf(dir)
      {:purge, plugin_id} -> Loader.purge_modules(plugin_id)
      {:delete_store_row, plugin_id} -> Store.delete(plugin_id)
      _ -> :ok
    end)

    Process.put(@rollback_key, [])
    :ok
  end
end
