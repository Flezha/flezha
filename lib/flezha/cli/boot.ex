defmodule Flezha.Service.Cli.Boot do
  @moduledoc """
  `flz boot` — the text forms of the readiness verdict and of the bounded wait
  for it. Both read `Flezha.Boot.Readiness` through the controller, so the CLI,
  `GET /readyz` and the container layer cannot disagree.
  """

  alias Flezha.Service.Cli.Context

  @doc """
  `flz boot status`: for each controller, the verdict, its preconditions, and
  each visible namespace's recovery.
  """
  def status(answer, _args, _opts), do: Context.per_controller(answer, &block/1)

  defp block(status) do
    IO.puts("readiness: #{status.readiness}")

    Enum.each(status.preconditions, fn %{key: key, met: met} ->
      IO.puts("  #{if met, do: "met  ", else: "unmet"}  #{key}")
    end)

    IO.puts("")
    IO.puts("namespace                          recovery_state    registry_rows")
    IO.puts("----------------------------------------------------------------")

    Enum.each(status.namespaces, fn row ->
      IO.puts(
        String.pad_trailing(row.namespace, 35) <>
          String.pad_trailing(to_string(row.recovery_state), 17) <> to_string(row.registry_rows)
      )
    end)
  end

  @doc "`flz boot await`."
  def await(:ready, _args, _opts), do: IO.puts("boot await: ready")
end
