defmodule Flezha.State.TestHelpersTest do
  @moduledoc """
  The two roots every persistence module's `setup` rests on:
  `tmp_app_data_dir/0` hands back one nothing else owns, and
  `reset_all_entities/0` empties the one the writers resolve.
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

  # The state a test run reaches once a teardown has deleted `:state_root` from
  # the application env while `FLEZHA_STATE_DIR` is still set. Every writer,
  # the bind-time restore and the operative teardown resolve
  # `Paths.state_root/0`, so a reset deriving a root of its own empties a
  # directory nothing wrote to and the previous test's Entity rehydrates on the
  # next bind. The identity is the one the suite really leaked on.
  test "the reset empties the root the writers resolve, not one of its own" do
    root = NodeTestHelper.claim_tmp_dir("flezha_reset_root")
    # Taken for its restore closure, before the drift below removes the key.
    restore_app = TestHelpers.override_env(:state_root, root)
    previous_var = System.get_env("FLEZHA_STATE_DIR")
    Application.delete_env(:flezha, :state_root)
    System.put_env("FLEZHA_STATE_DIR", root)

    on_exit(fn ->
      if previous_var,
        do: System.put_env("FLEZHA_STATE_DIR", previous_var),
        else: System.delete_env("FLEZHA_STATE_DIR")

      restore_app.()
      File.rm_rf(root)
    end)

    entity_id = {"operative_2", "store_input_procedure", "_"}
    TestHelpers.write_wal_of_lang(entity_id, 0)
    # The second thing a rebind restores from, and it sits beside the WAL.
    input_log = Paths.input_log_path("operative_2")
    File.write!(input_log, "queued-input")
    {:ok, writer} = Supervisor.find_or_start(entity_id)

    assert Restore.has_persisted_state?(entity_id)
    assert File.exists?(input_log)

    TestHelpers.reset_all_entities()

    refute Process.alive?(writer), "the reset returned with a writer still live"

    refute Restore.has_persisted_state?(entity_id),
           "the reset left #{Paths.wal_path(entity_id)} for the next bind to rehydrate"

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
