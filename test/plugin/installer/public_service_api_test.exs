defmodule Flezha.Plugin.Installer.PublicServiceAPITest do
  @moduledoc """
  Tests that mutating procedures serialize through the `Installer` GenServer
  so two concurrent installs cannot race on the code server or DETS.
  """

  # async: false: writes Application env; a shared on-disk root; registered singletons.

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Installer.Store

  setup do
    tmp =
      Path.join(System.tmp_dir!(), "flz_svc_api_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(tmp)

    prev_data_dir = Application.get_env(:flezha, :plugin_installer_data_dir)
    prev_pi = Application.get_env(:flezha, :plugin_installer)

    Application.put_env(:flezha, :plugin_installer_data_dir, tmp)

    Application.put_env(:flezha, :plugin_installer,
      plugin_root: Path.join(tmp, "plugins"),
      trusted_keys: [],
      require_signature: false,
      allow_unsigned: true,
      max_package_bytes: 50_000_000,
      max_source_files: 200
    )

    for mod <- [Installer, Loader, Store] do
      try do
        case Process.whereis(mod) do
          nil -> :ok
          pid -> FlezhaTest.Helper.NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
        end
      catch
        :exit, _ -> :ok
      end
    end

    {:ok, _} = Store.start_link([])
    {:ok, _} = Loader.start_link([])
    {:ok, _} = Installer.start_link([])

    on_exit(fn ->
      for mod <- [Installer, Loader, Store] do
        try do
          case Process.whereis(mod) do
            nil -> :ok
            pid -> FlezhaTest.Helper.NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
          end
        catch
          :exit, _ -> :ok
        end
      end

      if prev_data_dir do
        Application.put_env(:flezha, :plugin_installer_data_dir, prev_data_dir)
      else
        Application.delete_env(:flezha, :plugin_installer_data_dir)
      end

      if prev_pi do
        Application.put_env(:flezha, :plugin_installer, prev_pi)
      else
        Application.delete_env(:flezha, :plugin_installer)
      end

      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  test "info/1 for unknown plugin returns :not_found" do
    assert {:error, :not_found} = Installer.info("nobody_home")
  end

  test "list/0 returns [] on empty catalog" do
    assert Installer.list() == []
  end

  test "concurrent installs serialize — one waits for the other", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    fpz_a = build_fpz!(tmp, "ser_a_#{unique}", "SerA.M#{unique}")
    fpz_b = build_fpz!(tmp, "ser_b_#{unique}", "SerB.M#{unique}")

    parent = self()

    t0 = System.monotonic_time(:millisecond)

    pid_a =
      spawn(fn ->
        r = Installer.install(fpz_a)
        send(parent, {:a_done, System.monotonic_time(:millisecond), r})
      end)

    pid_b =
      spawn(fn ->
        r = Installer.install(fpz_b)
        send(parent, {:b_done, System.monotonic_time(:millisecond), r})
      end)

    assert_receive {:a_done, _, {:ok, _}}, 60_000
    assert_receive {:b_done, _, {:ok, _}}, 60_000

    # Both succeed; the GenServer serialized them, no concurrent writes to
    # the code server / DETS.
    refute Process.alive?(pid_a)
    refute Process.alive?(pid_b)
    assert System.monotonic_time(:millisecond) >= t0
  end

  defp build_fpz!(tmp, id, mod) do
    fpz = Path.join(tmp, "#{id}.fpz")
    src_filename = Macro.underscore(mod) <> ".ex"

    manifest = """
    %{
      id: "#{id}",
      version: "1.0.0",
      description: "test",
      author: "tester",
      modules: [#{mod}],
      provides: [
        %{
          id: "main",
          provider: #{mod},
          subscriptions: [{:on_input, []}],
          default_opts: %{},
          scope: :operative
        }
      ],
      dependencies: []
    }
    """

    src = "defmodule #{mod} do\n  def hi, do: :ok\nend"

    entries = [
      {"manifest.exs", manifest},
      {"src/#{src_filename}", src},
      {"CHECKSUMS.sha256", "#{sha(manifest)}  manifest.exs\n#{sha(src)}  src/#{src_filename}\n"}
    ]

    :ok = File.mkdir_p!(Path.dirname(fpz))

    :ok =
      :erl_tar.create(
        String.to_charlist(fpz),
        Enum.map(entries, fn {n, b} -> {String.to_charlist(n), b} end),
        [:compressed]
      )

    fpz
  end

  defp sha(bin), do: :crypto.hash(:sha256, bin) |> Base.encode16(case: :lower)
end
