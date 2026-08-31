defmodule Flezha.State.TestHelpersTest do
  @moduledoc """
  `tmp_app_data_dir/0` hands back a state root nothing else owns — the
  precondition every persistence module's `setup` rests on.
  """
  # async: false: the planted band is a range of `flezha_state_test_<n>` names
  # that `tmp_app_data_dir/0` itself mints from, and this test removes every
  # one of them on exit. The sync phase is what keeps it off a live one.
  use ExUnit.Case, async: false

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
