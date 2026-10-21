defmodule FlezhaTest.System.LayoutAndIdE2ETest do
  @moduledoc """
  One layout, read by both runtimes, and one steps id, proved on disk.

  The controller resolves where a file lands from `Flezha.Layout.Table`;
  `flz reset` resolves the same thing from `crates/cli/src/reset/plan.rs`. Two
  declarations of one layout is how a user ends up with a directory that
  is never cleared, or one that is cleared and should not have been. This
  drives the real binary against a data root of its own and compares, row by
  row, the directory the controller uses with the one reset reports.

  The id half is the colocation failure driven through the entry point rather
  than a wildcard: a runner attaches under a namespace long enough that the old
  mint exceeded the socket bound on 92.3 % of draws, and the assertion is that
  the file on disk carries the id the controller reports.

  Tagged `:system`; run with
  `mix test --include system test/system/layout_and_id_e2e_test.exs`.
  """

  # `reference_runtime: true`: controller mechanics. The scenario needs a live
  # controller, its own listeners and one attached runner, not a particular SDK.
  use FlezhaTest.SystemCase, capability: :connect_discovery

  @moduletag reference_runtime: true
  @moduletag :capture_log
  @moduletag timeout: 180_000

  alias Flezha.Config.Keys
  alias Flezha.Identificator
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Runner.FleetManager
  alias Flezha.Socket.Support
  alias Flezha.Transport.Native
  alias FlezhaTest.Helper.NodeTestHelper

  # Long enough that the old mint exceeded the socket bound on 92.3 % of draws
  # (100 000 draws, recorded in the colocation finding).
  @long_namespace "colocation_ns"

  setup do
    FlezhaTest.Case.install([:env])
  end

  describe "the layout both runtimes read" do
    test "reset plans every row where the controller resolves it, with the row's own mode" do
      root = scratch_root()
      point_every_row_at(root)

      json = reset_json(root, ["--dry-run"])
      planned = Map.new(planned_rows(json))

      for subtree <- Layout.subtrees(), subtree != :sockets do
        dir = Path.expand(Layout.dir(subtree))
        mode = Layout.reset_mode(subtree)

        assert planned[dir] == mode,
               "the controller uses #{dir} for #{subtree}, which declares #{mode}; " <>
                 "reset planned #{inspect(planned[dir])} there, and #{inspect(Map.keys(planned))}"
      end

      # The ladder's row is planned by name, never as a subtree of the data root.
      assert Enum.any?(json["kept"], &(&1["root"] == "socket directory")),
             "the socket row is not planned through the ladder"
    end

    test "reset clears the rows that declare it and leaves every other row standing" do
      root = scratch_root()

      for subtree <- Layout.subtrees(), subtree != :sockets do
        File.mkdir_p!(row_dir(root, subtree))
        File.write!(witness(root, subtree), "x")
      end

      _ = reset_json(root, ["--yes"])

      for subtree <- Layout.subtrees(), subtree != :sockets do
        case Layout.reset_mode(subtree) do
          :default ->
            refute File.exists?(witness(root, subtree)),
                   "#{subtree} declares :default and survived a reset"

          mode ->
            assert File.exists?(witness(root, subtree)),
                   "#{subtree} declares #{mode} and a plain reset took it"
        end
      end
    end
  end

  describe "the id the controller mints is the id the socket carries" do
    test "a runner under a long namespace opens the socket its own id names" do
      runner_id = NodeTestHelper.start_runner(@long_namespace)
      on_exit(fn -> NodeTestHelper.stop_runner(@long_namespace, runner_id) end)

      assert Identificator.validate(:runner, runner_id) == :ok
      assert byte_size(runner_id) <= Identificator.limit(:runner)

      assert NodeTestHelper.wait_until(
               fn -> runner_id in FleetManager.active_runners(@long_namespace) end,
               10_000
             )

      for type <- ["primary", "secondary"] do
        assert {:ok, path} = Native.stream_socket_path(Support.socket_dir(), runner_id, type)

        # The name carries the id itself, not the transport's shortening of it.
        assert Path.basename(path) == "#{type}_stream_#{runner_id}.sock"

        assert NodeTestHelper.wait_until(fn -> File.exists?(path) end, 10_000),
               "no socket at #{path} for the id the controller reports"
      end
    end
  end

  # `{absolute path, mode}` for every root the binary planned: a kept root's
  # mode is read from the reason it is reported with.
  defp planned_rows(json) do
    Enum.map(json["cleared"] || [], &{Path.expand(&1["root"]), :default}) ++
      Enum.map(json["kept"] || [], &{Path.expand(&1["root"]), kept_mode(&1["reason"])})
  end

  defp kept_mode("not reset's to clear"), do: :never
  defp kept_mode("use --include-" <> _flag), do: :flag

  defp row_dir(root, subtree), do: Path.join([root, "app", Atom.to_string(subtree)])
  defp witness(root, subtree), do: Path.join(row_dir(root, subtree), "witness.dat")

  # Point every row's knob at `<root>/app/<subtree>` so the controller's own
  # resolution and the binary's `--storage-root` name one tree.
  defp point_every_row_at(root) do
    for subtree <- Layout.subtrees(), subtree != :sockets do
      dir = row_dir(root, subtree)
      File.mkdir_p!(dir)

      case Layout.row(subtree).knob do
        {:dirname, knob} ->
          row = Keys.fetch!(knob)
          configure(row.key, Path.join(dir, Path.basename(row.default)))

        knob ->
          configure(Keys.fetch!(knob).key, dir)
      end
    end
  end

  defp configure(key, value) when is_atom(key), do: Application.put_env(:flezha, key, value)

  defp configure({parent, child}, value) do
    current = Application.get_env(:flezha, parent, [])
    Application.put_env(:flezha, parent, Keyword.put(current, child, value))
  end

  defp scratch_root() do
    root = Path.join(System.tmp_dir!(), "layout_e2e_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)
    root
  end

  defp reset_json(root, flags) do
    {out, 0} =
      System.cmd(flz_binary(), ["reset", "--storage-root", root, "--json"] ++ flags,
        stderr_to_stdout: true
      )

    Jason.decode!(out)
  end

  defp flz_binary() do
    ["_build/.target/debug/flz", "_build/.target/release/flz"]
    |> Enum.map(&Path.expand/1)
    |> Enum.find(&File.exists?/1) ||
      flunk("no `flz` binary built; run `cargo build -p flz`")
  end
end
