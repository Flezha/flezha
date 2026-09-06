defmodule FlezhaTest.InstallerCase do
  @moduledoc """
  The case template the plugin-installer modules share:

      use FlezhaTest.InstallerCase, async: false

  Two rows of the `FlezhaTest.Case` fixture table. `:tmp_dir` gives each test a
  directory of its own, arriving in the context as `:tmp` and pointed at by
  `:plugin_installer_data_dir`; `:env` puts back every `:flezha` key the test
  changed, which is both installer keys and anything `start_installer_tree/1`
  set.

  `start_installer_tree/1` runs `Store`, `Loader` and `Installer` over that
  directory at the test defaults; a module needing a different option set, or
  fewer than three of them, starts its own from `stop_if_running/1` and
  `on_exit_stop/1`.

  `async:` stays at the call site: every user of this template names the
  installer singletons.
  """

  use ExUnit.CaseTemplate

  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Installer.Store
  alias FlezhaTest.Helper.NodeTestHelper

  using do
    quote do
      import FlezhaTest.InstallerCase,
        only: [
          on_exit_stop: 1,
          start_installer_tree: 1,
          stop_if_running: 1,
          stop_installer_tree: 0
        ]
    end
  end

  setup do
    context = %{tmp: tmp} = FlezhaTest.Case.install([:tmp_dir, :env])
    Application.put_env(:flezha, :plugin_installer_data_dir, tmp)

    context
  end

  @doc """
  Put `:plugin_installer` at the test defaults under `tmp` — every option the
  installer reads, so nothing another module left can leak in — and run
  `Store`, `Loader` and `Installer` over it, in that order, stopping any that
  were already running first and again when the test ends.
  """
  @spec start_installer_tree(Path.t()) :: :ok
  def start_installer_tree(tmp) do
    Application.put_env(:flezha, :plugin_installer,
      plugin_root: Path.join(tmp, "plugins"),
      trusted_keys: [],
      require_signature: false,
      allow_unsigned: true,
      max_package_bytes: 50_000_000,
      max_source_files: 200
    )

    stop_installer_tree()
    ExUnit.Callbacks.on_exit(&stop_installer_tree/0)

    for mod <- [Store, Loader, Installer], do: {:ok, _} = mod.start_link([])

    :ok
  end

  @doc "Stop `Installer`, `Loader` and `Store`, innermost dependant first."
  @spec stop_installer_tree() :: :ok
  def stop_installer_tree do
    Enum.each([Installer, Loader, Store], &stop_if_running/1)
  end

  @doc """
  Stop the process registered under `mod`, if one is running. An exit racing
  the stop is not a failure: the process the test is trying to be rid of is
  gone either way.
  """
  @spec stop_if_running(module()) :: :ok
  def stop_if_running(mod) do
    case Process.whereis(mod) do
      nil -> :ok
      pid -> stop(pid)
    end
  end

  @doc "Stop `pid` when the test ends."
  @spec on_exit_stop(pid()) :: :ok
  def on_exit_stop(pid), do: ExUnit.Callbacks.on_exit(fn -> stop(pid) end)

  defp stop(pid) do
    NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
    :ok
  catch
    :exit, _ -> :ok
  end
end
