defmodule Flezha.Plugin.Provider.HttpPoll.PollWorker do
  @moduledoc """
  The polling half of `Flezha.Plugin.Provider.HttpPoll`: one process per
  enabled plugin owning the interval timer and the injection state, so the host
  relay's dispatch path is never inside an upstream wait.

  Each request runs in a short-lived monitored child, which keeps this
  worker's mailbox answerable while a hanging endpoint is outstanding — the
  health probe reports on exactly the outage that would otherwise block it.

  Injection is content-gated, per relay it injects into. A poller its own
  relay started confirms each body, and its hash advances only once the
  relay has TAKEN the payload, so a refused ingest re-offers the same content
  rather than losing a change. A shared copy's poller serves only relays
  whose input is not durable (`Flezha.Plugin.Extension.Table.Workers.own_source?/2`)
  and casts to them, so no relay's answer holds back another's. A relay
  the worker holds no hash for (every one at start, and one newly bound) is
  offered the next body: that seed is what converges a rejoining relay
  within one interval.
  """

  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.HttpFetch
  alias Flezha.Plugin.Provider.Worker
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Relay.Ingest.Verdict

  # The facts the provider's `health/1` judges. A probe that times out, since
  # requests run off this process, means the worker itself is wedged rather
  # than merely busy.
  use Worker, facts: [:consecutive_failures, :last_error, :last_settled_at]
  use Flezha.Process.Timer, keys: [:poll]

  require Verdict
  require Logger

  # The prefix is derived from the provider's id; nothing here names a root.
  @provider "http_poll"

  defp emit(verb, measurements, metadata),
    do: ProviderBehaviour.emit(@provider, verb, measurements, metadata)

  @impl true
  def init(opts) do
    mode = if opts[:targets_from], do: :fire_and_forget, else: :verdict

    state =
      opts
      |> Map.take([
        :url,
        :entry_channel,
        :headers,
        :plugin_id,
        :interval_ms,
        :timeout_ms,
        :max_body_bytes
      ])
      |> Map.merge(Worker.fields(opts))
      |> Map.merge(%{
        targets: offered(Host.targets(opts), %{}, mode),
        mode: mode,
        fetch: nil,
        etag: nil
      })

    # The first poll is scheduled, never executed here: `activate/1` is
    # synchronous in the host, and an upstream wait there would block the bind.
    send(self(), :poll)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, %{fetch: nil} = state), do: {:noreply, start_fetch(state)}

  # A tick that lands on an outstanding request is dropped rather than queued:
  # the running fetch schedules the next one when it settles, so a slow
  # endpoint stretches the cadence instead of stacking concurrent requests.
  def handle_info(:poll, state), do: {:noreply, state}

  def handle_info({:fetch_done, pid, result}, %{fetch: {pid, ref, started}} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, settle(%{state | fetch: nil}, result, elapsed(started))}
  end

  def handle_info({:DOWN, ref, :process, _dead, reason}, %{fetch: {_pid, ref, started}} = state) do
    {:noreply,
     settle(%{state | fetch: nil}, {:error, {:fetch_crashed, reason}}, elapsed(started))}
  end

  # A relay it holds no hash for must be sent a body, which a request
  # carrying the last validator would not fetch.
  def handle_info({:targets, targets}, state) do
    held = Map.new(state.targets, fn {pid, _md, _mode, hash} -> {pid, hash} end)
    targets = offered(targets, held, state.mode)
    etag = if Enum.any?(targets, &is_nil(elem(&1, 3))), do: nil, else: state.etag
    {:noreply, %{state | targets: targets, etag: etag}}
  end

  def handle_info(_other, state), do: {:noreply, state}

  # ---------- the poll cycle ----------

  defp start_fetch(state) do
    worker = self()
    request = Map.take(state, [:url, :headers, :etag, :timeout_ms, :max_body_bytes])

    {pid, ref} =
      spawn_monitor(fn -> send(worker, {:fetch_done, self(), HttpFetch.fetch(request)}) end)

    %{state | fetch: {pid, ref, :erlang.monotonic_time(:millisecond)}}
  end

  # Every outcome funnels through here — injected, unchanged, or failed — which
  # is why the idle stamp belongs here and not on the injection path. An
  # `http_poll` is content-gated: a healthy poller against content that never
  # changes correctly injects nothing, so "last injected" would call it dead.
  defp settle(state, result, latency_ms) do
    state =
      case result do
        {:ok, :not_modified} -> unchanged(state, latency_ms)
        {:ok, body, etag} -> classify_body(state, body, etag, latency_ms)
        {:error, reason} -> failed(state, reason, latency_ms)
      end

    state = Worker.settled(state)
    arm_timer(state, :poll, state.interval_ms, :poll)
  end

  # A non-UTF-8 body is a failure rather than a frame: it cannot be carried by
  # an entry channel (the SDKs seal entry payloads to strings), so injecting it
  # would only move the error to the runner. The oversize refusal happens
  # earlier, in the fetch child, where it can still abort the transfer.
  #
  # The validator is committed only on a NON-failure outcome. Storing it here
  # would make the next request conditional on a body this poller rejected: the
  # server would answer 304, that reads as an unchanged poll, the failure run
  # resets, and the plugin sits healthy and silent forever with no hash.
  defp classify_body(state, body, etag, latency_ms) do
    if String.valid?(body) do
      hash = :crypto.hash(:sha256, body)

      if Enum.all?(state.targets, &(elem(&1, 3) == hash)),
        do: unchanged(%{state | etag: etag}, latency_ms),
        else: inject(state, body, hash, etag, latency_ms)
    else
      failed(state, {:invalid_utf8, byte_size(body)}, latency_ms)
    end
  end

  defp inject(state, body, hash, etag, latency_ms) do
    seed? = Enum.any?(state.targets, &is_nil(elem(&1, 3)))
    {targets, refusals} = Enum.map_reduce(state.targets, [], &offer(&1, &2, state, body, hash))
    state = %{state | targets: targets}

    case refusals do
      [] ->
        emit(
          :shipped,
          %{bytes: byte_size(body), latency_ms: latency_ms},
          Map.put(labels(state), :seed, seed?)
        )

        Worker.recovered(%{state | etag: etag})

      # Neither the refusing relay's hash nor the validator is advanced: it
      # did not take this content, so the next poll must re-offer it rather
      # than read it as already delivered and go quiet forever.
      [reason | _] ->
        failed(state, {:ingest, reason}, latency_ms)
    end
  end

  # Its own relay's poller confirms, not casts: a parked relay drops the
  # cast silently, and a dropped config push that has already advanced the hash
  # is lost until the endpoint happens to change again. `:absorbed` (duplicate)
  # and `:suspended` (journaled for wake) both mean the relay owns the
  # payload. A cast answers `:ok`.
  defp offer({_pid, _metadata, _mode, hash} = target, refusals, _state, _body, hash),
    do: {target, refusals}

  defp offer({pid, metadata, mode, _held} = target, refusals, state, body, hash) do
    case Host.inject(pid, body, metadata, mode, state.timeout_ms) do
      verdict when Verdict.taken?(verdict) -> {{pid, metadata, mode, hash}, refusals}
      {:error, reason} -> {target, [reason | refusals]}
    end
  end

  # Each relay the worker injects into, with the hash of the content it
  # took last, kept across a change of relays.
  defp offered(targets, held, mode) do
    for {pid, metadata, target_mode} <- Host.fan_out(targets, %{}, mode),
        do: {pid, metadata, target_mode, Map.get(held, pid)}
  end

  defp unchanged(state, latency_ms) do
    emit(:unchanged, %{latency_ms: latency_ms}, labels(state))
    Worker.recovered(state)
  end

  defp failed(state, reason, latency_ms) do
    state = Worker.failed(state, reason)
    failures = state.consecutive_failures

    Logger.warning(
      "[Plugin.Provider.HttpPoll] poll failed plugin_id=#{inspect(state.plugin_id)} " <>
        "consecutive=#{failures} reason=#{inspect(reason)}"
    )

    emit(
      :failed,
      %{consecutive_failures: failures, latency_ms: latency_ms},
      Map.put(labels(state), :reason, reason)
    )

    # The last delivered hash is retained: being unable to re-read the resource
    # does not invalidate the content the relay already holds.
    state
  end

  defp labels(state), do: %{plugin_id: state.plugin_id, channel: state.entry_channel}

  defp elapsed(started), do: :erlang.monotonic_time(:millisecond) - started
end
