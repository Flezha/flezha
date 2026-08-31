defmodule Flezha.Service.Cli.Dlq do
  @moduledoc """
  `flz dlq` — list, show, replay and drop for parked records.
  """

  alias Flezha.Dlq.Store
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.Cli.State
  alias Flezha.Service.OperativeManager

  # How long `dlq replay` waits for a re-park before declaring the replay
  # successful and removing the entry. A re-park after the grace window
  # simply creates a fresh entry (same content identity, generation 1).
  defp dlq_replay_grace_ms() do
    Application.get_env(:flezha, :dlq, [])[:replay_grace_ms] || 5_000
  end

  @doc """
  `flz dlq list [operative]` — parked entries, nearest-to-expiry (oldest)
  first.
  """
  def dlq_list(operative_class \\ "") do
    filter = if operative_class == "", do: nil, else: operative_class
    entries = Store.list(filter)

    if entries == [] do
      IO.puts("dead-letter queue is empty")
    else
      IO.puts(
        String.pad_trailing("ENTRY", 60) <>
          String.pad_trailing("ENTITY", 16) <>
          String.pad_trailing("CHANNEL", 12) <>
          String.pad_trailing("GEN", 5) <>
          String.pad_trailing("AGE", 10) <> "ERROR"
      )

      Enum.each(entries, fn e ->
        ref = "#{e["operative_class"]}/#{e["procedure_id"]}/#{e["id"]}"
        age_s = div(System.system_time(:millisecond) - (e["ts_ms"] || 0), 1000)

        IO.puts(
          String.pad_trailing(ref, 60) <>
            String.pad_trailing(e["entity_key"] || "", 16) <>
            String.pad_trailing(e["channel"] || "", 12) <>
            String.pad_trailing(Integer.to_string(e["generation"] || 1), 5) <>
            String.pad_trailing("#{age_s}s", 10) <> String.slice(e["error"] || "", 0, 60)
        )
      end)
    end
  end

  @doc """
  `flz dlq show <operative/procedure/id>` — entry metadata + payload preview.
  """
  def dlq_show(entry_ref) do
    case Store.get(entry_ref) do
      {:ok, entry, payload} ->
        Enum.each(
          ~w(id operative_class procedure_id entity_key channel origin_offset error attempts generation status ts_ms payload_bytes),
          fn key ->
            IO.puts(String.pad_trailing(key, 18) <> to_string(inspect(entry[key])))
          end
        )

        case payload do
          nil ->
            IO.puts("payload            <not byte-representable; entry is not replayable>")

          bytes ->
            preview = binary_part(bytes, 0, min(byte_size(bytes), 256))

            rendered =
              if String.printable?(preview),
                do: preview,
                else: Base.encode16(preview, case: :lower)

            IO.puts("payload            #{rendered}")
        end

      {:error, reason} ->
        render_dlq_error(reason)
    end
  end

  @doc """
  `flz dlq replay <operative/procedure/id>` — re-dispatch a parked payload
  to its operative through the ordinary input path. A successful replay
  removes the entry; a failing one re-parks it with `generation`
  incremented (the store correlates on content identity). Replay identity
  is fresh by construction: the re-drive enters as a NEW input, so a
  tier-on invocation records fresh WAL seqs — downstream sinks see new
  `flz.seq` under the current epoch.
  """
  def dlq_replay(entry_ref, opts \\ []) do
    with {:ok, entry, payload} <- Store.get(entry_ref),
         :ok <- replayable(payload),
         {:ok, overwatch} <- resolve_overwatch(entry["operative_class"]) do
      Context.with_cli_authorization(
        opts,
        :"dlq.replay",
        State.state_target_namespace(entry["operative_class"]),
        %{entry: entry_ref},
        fn ->
          generation_before = entry["generation"] || 1
          :ok = Store.mark_replaying(entry_ref)

          :telemetry.execute([:flezha, :dlq, :operator_replay], %{count: 1}, %{
            entry: entry_ref,
            channel: entry["channel"],
            generation: generation_before,
            caller_source: :cli
          })

          GenServer.cast(overwatch, {:io_in, payload, %{"channel" => entry["channel"]}})
          await_replay_outcome(entry_ref, generation_before, dlq_replay_grace_ms())
        end
      )
    else
      {:error, reason} -> render_dlq_error(reason)
    end
  end

  @doc """
  `flz dlq drop <operative/procedure/id>` — discard a parked entry and its
  payload. Audited: the drop is deliberate data loss.
  """
  def dlq_drop(entry_ref, opts \\ []) do
    case Store.get(entry_ref) do
      {:ok, entry, _payload} ->
        Context.with_cli_authorization(
          opts,
          :"dlq.drop",
          State.state_target_namespace(entry["operative_class"]),
          %{entry: entry_ref},
          fn ->
            :ok = Store.drop(entry_ref)

            :telemetry.execute([:flezha, :dlq, :operator_drop], %{count: 1}, %{
              entry: entry_ref,
              channel: entry["channel"],
              entity_key: entry["entity_key"],
              error: entry["error"],
              caller_source: :cli
            })

            IO.puts("dropped #{entry_ref}")
          end
        )

      {:error, reason} ->
        render_dlq_error(reason)
    end
  end

  defp replayable(nil), do: {:error, :payload_not_replayable}
  defp replayable(_payload), do: :ok

  defp resolve_overwatch(operative_class) do
    case ClassRegistry.overwatch_pid(operative_class) do
      {:ok, pid} -> {:ok, pid}
      :error -> resolve_overwatch_cluster(operative_class)
    end
  end

  # The class registry is node-local; a target hosted on a peer resolves
  # through the routed pid lookup and the re-injection cast rides distribution.
  defp resolve_overwatch_cluster(operative_class) do
    with {:ok, namespace} <- OperativeManager.namespace_of(operative_class),
         {:ok, pid} <- OperativeManager.get_operative_pid(namespace, operative_class) do
      {:ok, pid}
    else
      _ -> {:error, :operative_not_running}
    end
  end

  # Poll for the re-park (generation bump) until the grace window closes;
  # no re-park within it means the replay processed cleanly.
  defp await_replay_outcome(entry_ref, _generation_before, remaining_ms)
       when remaining_ms <= 0 do
    :ok = Store.resolve(entry_ref)
    IO.puts("replay succeeded; #{entry_ref} removed")
  end

  defp await_replay_outcome(entry_ref, generation_before, remaining_ms) do
    Process.sleep(100)

    case Store.get(entry_ref) do
      {:ok, %{"status" => "parked", "generation" => gen}, _payload}
      when gen > generation_before ->
        IO.puts("replay failed; #{entry_ref} re-parked with generation #{gen}")

      {:ok, _entry, _payload} ->
        await_replay_outcome(entry_ref, generation_before, remaining_ms - 100)

      {:error, :not_found} ->
        IO.puts("replay succeeded; #{entry_ref} removed")
    end
  end

  defp render_dlq_error(:not_found) do
    Context.cli_error!("error: DLQ entry not found", :stderr)
  end

  defp render_dlq_error(:bad_entry_ref) do
    Context.cli_error!("error: expected <operative>/<procedure>/<entry-id>", :stderr)
  end

  defp render_dlq_error(:payload_not_replayable) do
    Context.cli_error!(
      "error: entry carries no payload bytes (inter-procedure input); inspect or drop it",
      :stderr
    )
  end

  defp render_dlq_error(:operative_not_running) do
    Context.cli_error!(
      "error: the entry's operative is not running; start it before replaying",
      :stderr
    )
  end

  defp render_dlq_error(reason) do
    Context.cli_error!("error: #{inspect(reason)}", :stderr)
  end
end
