defmodule Flezha.Cli.SocketTest do
  @moduledoc """
  A `flz` request served over the live controller's UDS control socket: the
  frames it is answered with, in order, the code it ends with, the refusals it
  runs nothing for, and a caller that hangs up ending the call.
  """
  # async: false: the live controller's socket, and global Mimic stubs reached
  # from the request's own processes.
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Service.Cli
  alias Flezha.Transport.Server
  alias FlezhaTest.Helper.CliCapture
  alias FlezhaTest.Helper.ControlFrameTestHelper, as: Frames
  alias FlezhaTest.Helper.StreamPair

  import FlezhaTest.Helper.MimicGlobal

  setup :set_mimic_global_until_exit

  defp cli(name, args, opts \\ %{}),
    do: %{protocol: 1, entry: "cli", name: name, args: args, opts: opts}

  # Send `body` as a request and read its answer.
  defp ask(body), do: body |> Frames.cli_request() |> summarise()

  defp summarise(frames) do
    {"cli_exit", exit} = List.keyfind(frames, "cli_exit", 0)

    %{
      frames: frames,
      stdout: for({"cli_stdout", bytes} <- frames, into: "", do: bytes),
      stderr: for({"cli_stderr", bytes} <- frames, into: "", do: bytes),
      code: Jason.decode!(exit)["code"]
    }
  end

  describe "an answer" do
    test "is accepted first, then printed as the verb prints it in process, then exits 0" do
      {{:ok, :ok}, text, ""} =
        CliCapture.run(fn -> Cli.call("Relay.list_namespaces", "[]", []) end)

      answer = ask(cli("Relay.list_namespaces", []))

      assert [{"cli_accepted", accepted} | _] = answer.frames
      assert Jason.decode!(accepted) == %{"protocol" => 1}
      assert {answer.stdout, answer.stderr, answer.code} == {text, "", 0}
    end

    test "under output json is the door's JSON" do
      {{:ok, :ok}, json, ""} =
        CliCapture.run(fn -> Cli.call("Relay.list_namespaces", "[]", output: "json") end)

      answer = ask(cli("Relay.list_namespaces", [], %{output: "json"}))

      assert {answer.stdout, answer.code} == {json, 0}
    end

    test "of an MCP line is the protocol's reply" do
      line = Jason.encode!(%{jsonrpc: "2.0", id: 7, method: "ping"})
      answer = ask(%{protocol: 1, entry: "mcp", line: line, opts: %{}})

      assert answer.code == 0
      assert %{"id" => 7, "result" => _} = Jason.decode!(answer.stdout)
    end
  end

  describe "a refusal" do
    test "keeps its stream and its class's code" do
      answer = ask(cli("PluginManager.disable", ["ghost", %{}]))

      assert answer.stdout == ""
      assert answer.stderr =~ "(plugin.not_found)"
      assert answer.code == 5
    end

    test "of a protocol the controller does not speak names both numbers" do
      stub(Cli, :call, fn _name, _args, _opts -> flunk("an unknown protocol ran the call") end)

      answer = ask(%{cli("Relay.list_namespaces", []) | protocol: 2})

      assert answer.stderr =~ "the controller speaks flz request protocol 1"
      assert answer.stderr =~ "this request uses 2 (cli.protocol_unsupported)"
      assert answer.code == 2
    end

    test "of an option key that names no atom leaves no atom behind" do
      key = "zz_not_an_atom_#{System.unique_integer([:positive])}"
      stub(Cli, :call, fn _name, _args, _opts -> flunk("an unknown option ran the call") end)

      answer = ask(cli("Relay.list_namespaces", [], %{key => true}))

      assert answer.stderr =~ "unknown parameter '#{key}' (control.unknown_parameter)"
      assert answer.code == 2
      assert_raise ArgumentError, fn -> String.to_existing_atom(key) end
    end
  end

  describe "what the call writes" do
    test "arrives in the order it was written, whichever device it went to" do
      stub(Cli, :call, fn _name, _args, _opts ->
        notify = Cli.Context.notifier()
        IO.write("one")
        notify.("two")
        IO.write("three")
        :ok
      end)

      answer = ask(cli("Relay.list_namespaces", []))

      assert tl(answer.frames) == [
               {"cli_stdout", "one"},
               {"cli_stderr", "two\n"},
               {"cli_stdout", "three"},
               {"cli_exit", ~s({"code":0})}
             ]
    end

    test "is split into frames when one write is larger than a frame carries" do
      big = String.duplicate("x", 200_000)
      stub(Cli, :call, fn _name, _args, _opts -> IO.write(big) end)

      answer = ask(cli("Relay.list_namespaces", []))

      assert answer.stdout == big
      assert Enum.count(answer.frames, &match?({"cli_stdout", _}, &1)) > 1
    end

    @tag :capture_log
    test "is followed by the internal code when the call crashes" do
      stub(Cli, :call, fn _name, _args, _opts -> raise "boom" end)

      answer = ask(cli("Relay.list_namespaces", []))

      assert answer.stderr =~ "(internal.uncatalogued)"
      assert answer.code == 1
      # Provoked on purpose, in a process this test cannot mark as expecting it.
      :ets.match_delete(:flezha_uncatalogued, {nil, {%RuntimeError{message: "boom"}, :_}})
    end
  end

  describe "a caller that hangs up" do
    test "ends the call and the devices it wrote to" do
      test = self()

      stub(Cli, :call, fn _name, _args, _opts ->
        send(test, {:running, self(), Process.group_leader()})
        receive do: (:never -> :ok)
      end)

      socket = Frames.dial()
      :ok = Frames.send_frame(socket, "cli_request", Jason.encode!(cli("Relay.listen", [])))
      assert_receive {:running, call, device}, 5_000
      watches = [Process.monitor(call), Process.monitor(device)]

      :gen_tcp.close(socket)

      for watch <- watches, do: assert_receive({:DOWN, ^watch, :process, _, _}, 5_000)
    end
  end

  describe "a request on the QUIC server" do
    # The tree has no QUIC client that writes a raw frame, so the server's
    # QUIC instance is handed the first frame on a stream of a pair of its own.
    test "is refused as denied and runs nothing" do
      test = self()
      stub(Cli, :call, fn _name, _args, _opts -> send(test, :ran) end)
      {stream, close} = StreamPair.open()
      on_exit(close)

      first = {:flz_data, 9, "cli_request", Jason.encode!(cli("Relay.list_namespaces", [])), %{}}
      state = %{transport: :quic, pending_streams: %{9 => stream}}
      assert {:noreply, %{pending_streams: pending}} = Server.handle_info(first, state)
      assert pending == %{}

      assert_receive {:flz_data, 0, "cli_stderr", refusal, _}, 5_000
      assert refusal =~ "not over quic (cli.local_only)"
      assert_receive {:flz_data, 0, "cli_exit", ~s({"code":4}), _}, 5_000
      refute_received :ran
    end
  end
end
