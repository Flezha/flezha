defmodule Flezha.Service.Cli.Schedule do
  @moduledoc """
  `flz schedule add | list | remove | run` — the text forms of the scheduled
  operator starts.
  """

  # Error reasons truncate to the column; the full reason stays in
  # log/telemetry.
  @schedule_result_width 48

  @doc "`flz schedule add`."
  def add(added, _args, _opts) do
    entry = added.entry

    if added.runner_missing do
      IO.puts(
        "warning: factory #{entry.factory_id} has no live runner in namespace " <>
          "#{entry.namespace}; firings will fail until one connects"
      )
    end

    IO.puts(
      "Schedule #{entry.name} added (#{entry.cron} #{entry.timezone}); " <>
        "next fire #{next_fire(added.next_fire)}"
    )
  end

  @doc "`flz schedule list`."
  def list([], _args, _opts), do: IO.puts("no schedules")

  def list(entries, _args, _opts) do
    IO.puts(
      row([
        "NAME",
        "CRON",
        "TZ",
        "FACTORY",
        "NAMESPACE",
        "NODE",
        "NEXT FIRE (UTC)",
        "LAST FIRE (UTC)",
        "LAST RESULT"
      ])
    )

    Enum.each(entries, fn entry ->
      IO.puts(
        row([
          entry.name,
          entry.cron,
          entry.timezone,
          entry.factory_id,
          entry.namespace,
          if(entry.node, do: to_string(entry.node), else: "-"),
          next_fire(entry.next_fire),
          last_fire_at(entry.last_fire),
          last_result(entry.last_fire)
        ])
      )
    end)
  end

  @doc "`flz schedule remove`."
  def remove(:ok, [name | _], _opts), do: IO.puts("Schedule #{name} removed")

  @doc "`flz schedule run`."
  def run(%{outcome: :spawned, operator_id: id}, _args, _opts), do: IO.puts("Spawned #{id}")

  def run(%{outcome: :already_running}, _args, _opts),
    do: IO.puts("instance already running; no-op")

  defp next_fire(nil), do: "never"
  defp next_fire(at), do: DateTime.to_iso8601(at)

  defp last_fire_at(%{at: at}), do: at |> DateTime.truncate(:second) |> DateTime.to_iso8601()
  defp last_fire_at(_), do: "-"

  defp last_result(%{outcome: :ok}), do: "ok"
  defp last_result(%{outcome: :duplicate}), do: "duplicate"

  defp last_result(%{outcome: {:retrying, reason}}),
    do: String.slice("retrying: #{reason}", 0, @schedule_result_width)

  defp last_result(_), do: "-"

  # The first eight columns padded; the last is printed raw.
  defp row(fields) do
    {padded, [last]} = Enum.split(fields, 8)

    Enum.zip_with(padded, [24, 18, 20, 24, 14, 22, 22, 22], &String.pad_trailing/2)
    |> Enum.join()
    |> Kernel.<>(last)
  end
end
