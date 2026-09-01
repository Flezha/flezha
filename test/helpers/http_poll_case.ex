defmodule FlezhaTest.HttpPollCase do
  @moduledoc """
  The fixture an `HttpPoll` test drives: a real `:gen_tcp` origin, a poller
  pointed at it, and the telemetry forward that reports what each poll did.

  The host operative is stood in for by the test process, so injection is a
  `{:ingest, body, metadata}` **call** that `take_ingest/2` answers with any
  verdict a real Overwatch could return — including the refusals that must
  leave the content undelivered and re-offered.
  """

  use ExUnit.CaseTemplate

  import ExUnit.Assertions
  import ExUnit.Callbacks

  alias Flezha.Plugin.Provider.HttpPoll
  alias Flezha.Plugin.Provider.HttpPoll.PollWorker
  alias FlezhaTest.Helper.NodeTestHelper

  @channel "config_in"

  using do
    quote do
      import FlezhaTest.HttpPollCase

      @channel FlezhaTest.HttpPollCase.channel()
    end
  end

  setup do
    ref = make_ref()
    test_pid = self()

    :telemetry.attach_many(
      ref,
      [
        [:flezha, :plugin, :http_poll, :injected],
        [:flezha, :plugin, :http_poll, :unchanged],
        [:flezha, :plugin, :http_poll, :failed]
      ],
      fn event, measurements, metadata, _ ->
        send(test_pid, {:poll_event, List.last(event), measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(ref) end)
    :ok
  end

  @doc "The entry channel every fixture poller injects into."
  def channel, do: @channel

  def base_opts(origin, extra \\ %{}) do
    Map.merge(
      %{
        url: "http://127.0.0.1:#{origin.port}/config",
        channel: @channel,
        interval_ms: 60_000,
        timeout_ms: 2_000
      },
      extra
    )
  end

  def start_poller(origin, extra \\ %{}) do
    {:ok, opts} = HttpPoll.init_provider(base_opts(origin, extra))
    {:ok, poller} = HttpPoll.activate(opts)
    kill_on_exit(poller)
    poller
  end

  def probe(poller), do: PollWorker.probe(poller)

  def kill_on_exit(pid) do
    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
  end

  # The test process IS the host, so it answers the confirming ingest call the
  # way an Overwatch would — including the refusals.
  def take_ingest(verdict \\ :ok, timeout \\ 2_000) do
    receive do
      {:"$gen_call", from, {:ingest, body, metadata}} ->
        GenServer.reply(from, verdict)
        {body, metadata}
    after
      timeout -> flunk("no ingest call within #{timeout}ms")
    end
  end

  def refute_ingest(), do: refute_received({:"$gen_call", _from, {:ingest, _, _}})

  # ---------- fake HTTP origin ----------

  # The held request the origin reported; answering it lets the poll settle.
  def release_origin() do
    assert_receive {:origin_holding, conn}, 5_000
    send(conn, :unpark)
  end

  def start_origin(config) do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true, backlog: 16])

    {:ok, port} = :inet.port(listen)

    {:ok, agent} =
      Agent.start(fn ->
        Map.merge(
          %{
            status: 200,
            body: "",
            etag: nil,
            location: nil,
            hold: nil,
            requests: 0,
            conditional: 0,
            last: ""
          },
          Map.new(config)
        )
      end)

    acceptor = spawn(fn -> accept_loop(listen, agent) end)
    origin = %{port: port, listen: listen, agent: agent, acceptor: acceptor}

    on_exit(fn -> stop_origin(origin) end)
    origin
  end

  def stop_origin(origin) do
    if Process.alive?(origin.acceptor), do: Process.exit(origin.acceptor, :kill)
    :gen_tcp.close(origin.listen)
    :ok
  end

  def set(origin, config), do: Agent.update(origin.agent, &Map.merge(&1, Map.new(config)))
  def requests(origin), do: Agent.get(origin.agent, & &1.requests)
  def conditional_requests(origin), do: Agent.get(origin.agent, & &1.conditional)
  def last_request_headers(origin), do: Agent.get(origin.agent, & &1.last)

  defp accept_loop(listen, agent) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        spawn(fn -> serve(socket, agent) end)
        accept_loop(listen, agent)

      {:error, _closed} ->
        :ok
    end
  end

  defp serve(socket, agent) do
    with {:ok, request} <- read_request(socket) do
      config =
        Agent.get_and_update(agent, fn state ->
          conditional? = String.contains?(String.downcase(request), "if-none-match:")

          state = %{
            state
            | requests: state.requests + 1,
              conditional: state.conditional + if(conditional?, do: 1, else: 0),
              last: request
          }

          {Map.put(state, :conditional?, conditional?), state}
        end)

      # A held origin keeps the request in flight until the test releases it.
      if is_pid(config.hold) do
        send(config.hold, {:origin_holding, self()})
        NodeTestHelper.park()
      end

      :gen_tcp.send(socket, response(config))
    end

    :gen_tcp.close(socket)
  end

  defp read_request(socket, acc \\ "") do
    if String.contains?(acc, "\r\n\r\n") do
      {:ok, acc}
    else
      case :gen_tcp.recv(socket, 0, 2_000) do
        {:ok, chunk} -> read_request(socket, acc <> chunk)
        {:error, _} -> :error
      end
    end
  end

  # A conditional request whose validator the origin still holds is answered
  # 304, which is what makes the ETag pass-through observable — and what a
  # failure path must never be able to trigger for itself.
  defp response(%{conditional?: true, etag: etag, status: 200}) when is_binary(etag) do
    "HTTP/1.1 304 Not Modified\r\netag: #{etag}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
  end

  defp response(config) do
    optional =
      [
        if(config.etag, do: "etag: #{config.etag}\r\n"),
        if(config.location, do: "location: #{config.location}\r\n")
      ]
      |> Enum.reject(&is_nil/1)
      |> Enum.join()

    "HTTP/1.1 #{config.status} #{reason_phrase(config.status)}\r\n" <>
      optional <>
      "Content-Type: application/octet-stream\r\n" <>
      "Content-Length: #{byte_size(config.body)}\r\n" <>
      "Connection: close\r\n\r\n" <> config.body
  end

  defp reason_phrase(200), do: "OK"
  defp reason_phrase(302), do: "Found"
  defp reason_phrase(503), do: "Service Unavailable"
  defp reason_phrase(_), do: "Status"
end
