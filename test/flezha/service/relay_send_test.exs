defmodule Flezha.Service.Cli.SendTest do
  @moduledoc """
  `flz relay send <id> --text <payload> [--channel <ch>]` operations surface:
  the confirming verdict from `RelayManager.send_io_confirming/5` is reported
  and an optional channel is threaded into the `:io_in` metadata. The verdict
  logic itself is covered in `overwatch_confirming_ingest_test`. The error
  verdicts additionally `exit({:shutdown, 1})` for a non-zero CLI exit, so only the
  success verdicts are asserted in-process; an error exit is exercised by a live
  CLI run in the change's verification.
  """
  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Service.Cli

  setup :set_mimic_global_until_exit

  defp instance_id(), do: "snd_#{System.unique_integer([:positive])}"

  # Stub `send_io_confirming` to answer a fixed verdict and echo its args to the
  # test process, so both the output mapping and the metadata construction are
  # asserted without a live relay.
  defp run_send(inst, verdict, channel) do
    test = self()

    stub(Flezha.Service.RelayManager, :resolve_member, fn "default", ^inst, nil ->
      {:ok, inst}
    end)

    stub(Flezha.Service.RelayManager, :send_io_confirming, fn id, ns, msg, md ->
      Kernel.send(test, {:sent, id, ns, msg, md})
      verdict
    end)

    capture_io(fn ->
      Cli.call("Relay.send", Jason.encode!(["default", inst, "payload", channel, %{}]), [])
    end)
  end

  test "a delivered payload is reported and carries empty metadata when no channel is given" do
    inst = instance_id()

    out = run_send(inst, :ok, "")

    assert out =~ "Delivered to '#{inst}'"
    assert_received {:sent, ^inst, "default", "payload", %{}}
  end

  test "a channel is threaded into the io_in metadata" do
    inst = instance_id()

    run_send(inst, :ok, "in")

    assert_received {:sent, ^inst, "default", "payload", %{"channel" => "in"}}
  end

  test "an absorbed duplicate is reported" do
    inst = instance_id()

    assert run_send(inst, {:ok, :absorbed}, "") =~ "Absorbed (duplicate)"
  end

  test "a payload journaled for a suspended instance is reported" do
    inst = instance_id()

    assert run_send(inst, {:ok, :suspended}, "") =~ "delivers on wake"
  end
end
