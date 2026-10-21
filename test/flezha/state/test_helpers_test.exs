defmodule Flezha.State.TestHelpersTest do
  @moduledoc """
  The two roots every persistence module's `setup` rests on:
  `tmp_app_data_dir/0` hands back one nothing else owns, and
  `reset_all_cells/0` empties the one the writers resolve.
  """

  # async: false: the planted band is a range of `flezha_state_test_<n>` names
  # that `tmp_app_data_dir/0` itself mints from, and this test removes every
  # one of them on exit. The sync phase is what keeps it off a live one.
  use ExUnit.Case, async: false

  alias Flezha.State.{Paths, Restore, Supervisor, TestHelpers}
  alias FlezhaTest.Helper.NodeTestHelper

  # The defect is across BEAMs — `System.unique_integer([:positive])` restarts
  # low in every run, so run N+1 re-mints run N's names — and it is reproduced
  # with a real second BEAM rather than by predicting this one's counter. That
  # stream is node-global and per-scheduler striped, so a band planted ahead of
  # a live counter bears no fixed relation to where the next draw lands, and a
  # band the stream has already passed reads as a pass.
  #
  # The child draws until it is past @floor, which costs nothing and makes the
  # band it will then mint from known here; a boot alone lands anywhere in
  # 99..1417 on this box. @floor is a value a previous run of this suite really
  # does reach, so the leftovers planted at it are the ones it really leaves.
  @floor 4096
  @band @floor..(@floor + 512)

  test "a state root a previous BEAM left behind is never handed out again" do
    planted =
      Map.new(@band, fn n ->
        dir = Path.join(System.tmp_dir!(), "flezha_state_test_#{n}")
        File.mkdir_p!(dir)
        File.write!(Path.join(dir, "left_behind"), "")
        {dir, n}
      end)

    on_exit(fn -> Enum.each(Map.keys(planted), &File.rm_rf/1) end)

    {head, stride, base} = claim_in_a_fresh_beam()
    on_exit(fn -> File.rm_rf(base) end)

    # The vacuity guard, and it discriminates: the child reads the stream head
    # immediately before the claim, so `head + stride` is the name the claim
    # asks for first. If that name is not one this test planted then nothing
    # collided, and a green would say nothing about the claim.
    assert (head + stride) in @band,
           "the claim asked for #{head + stride} first, outside the planted band #{inspect(@band)}"

    refute Map.has_key?(planted, base), "handed back a state root it did not create: #{base}"
    assert File.ls!(base) == []
  end

  # Every writer, the bind-time restore and the relay teardown resolve
  # `Paths.state_root/0`, so a reset deriving a root of its own empties a
  # directory nothing wrote to and the previous test's Cell rehydrates on the
  # next bind. The identity is the one the suite really leaked on.
  #
  # The root is claimed through the application env because that is the one the
  # runtime reads. `FLEZHA_STATE_DIR` fills `:state_root` once, in
  # `config/runtime.exs` at boot; set here it changes nothing, and deleting the
  # key beside it put this test's WAL under the checkout's `storage/app/state`.
  test "the reset empties the root the writers resolve, not one of its own" do
    root = NodeTestHelper.claim_tmp_dir("flezha_reset_root")
    restore_app = TestHelpers.override_env(:state_root, root)

    on_exit(fn ->
      restore_app.()
      File.rm_rf(root)
    end)

    assert Paths.state_root() == root, "the writers resolve #{Paths.state_root()}, not #{root}"

    cell_id = {"relay_2", "store_input_tasklet", "_"}
    TestHelpers.write_wal_of_lang(cell_id, 0)
    # The second thing a rebind restores from, and it sits beside the WAL.
    input_log = Paths.input_log_path("relay_2")
    File.write!(input_log, "queued-input")
    {:ok, writer} = Supervisor.find_or_start(cell_id)

    assert Restore.has_persisted_state?(cell_id)
    assert File.exists?(input_log)

    TestHelpers.reset_all_cells()

    refute Process.alive?(writer), "the reset returned with a writer still live"

    refute Restore.has_persisted_state?(cell_id),
           "the reset left #{Paths.wal_path(cell_id)} for the next bind to rehydrate"

    refute File.exists?(input_log), "the reset left #{input_log} for the next bind to replay"
  end

  # Two stream draws and one claim, in a BEAM whose counter genuinely starts
  # low. The draws are reported so the caller can check the claim's first
  # candidate was one it had planted.
  defp claim_in_a_fresh_beam do
    script = """
    h1 = Enum.find(Stream.repeatedly(fn -> System.unique_integer([:positive]) end), &(&1 >= #{@floor}))
    h2 = System.unique_integer([:positive])
    base = Flezha.State.TestHelpers.tmp_app_data_dir()
    IO.puts(Enum.join(["CLAIMED", h2, h2 - h1, base], " "))
    """

    elixir = System.find_executable("elixir") || flunk("no elixir on PATH")
    args = ["-pa", Application.app_dir(:flezha, "ebin"), "-e", script]
    {out, status} = System.cmd(elixir, args, stderr_to_stdout: true)
    assert status == 0, "the child BEAM exited #{status}:\n#{out}"

    case Enum.filter(String.split(out, "\n"), &String.starts_with?(&1, "CLAIMED ")) do
      [line] ->
        ["CLAIMED", head, stride, base] = String.split(line, " ", parts: 4)
        {String.to_integer(head), String.to_integer(stride), base}

      other ->
        flunk("the child BEAM reported #{length(other)} claims:\n#{out}")
    end
  end
end
