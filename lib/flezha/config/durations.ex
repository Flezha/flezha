defmodule Flezha.Config.Durations do
  @moduledoc """
  The millisecond reader.

  A duration is a row of the one table like any other knob — a name, a default
  in milliseconds, the application-env key a user overrides it with, the
  environment variable the boot config translates into that key — plus a
  vocabulary of its own: a derivation for the values that are another row's
  multiple. A silence window that is `k` x its heartbeat, a budget that sits
  inside another by a fixed headroom. The rows are declared in
  `Flezha.Config.DurationRows` and merged by `Flezha.Config.Keys`.

  This is what a site calls when the quantity it wants is a duration: `get/1`
  answers milliseconds and refuses a row that is not one, so a typo that lands
  on a byte count is a crash at the call rather than a wait of sixteen million
  milliseconds.
  """

  alias Flezha.Config
  alias Flezha.Config.Keys

  @type name :: atom()
  @type key :: atom() | {atom(), atom()} | nil
  @type derivation :: {:factor, name(), pos_integer()} | {:headroom, name(), integer()}
  @type row :: %{
          default: non_neg_integer() | nil,
          key: key(),
          env: String.t() | nil,
          derived: derivation() | nil
        }

  @doc """
  Every millisecond row of the table, one per name — the ones declared here and
  any a feature declares beside its own keys, so a duration is a duration
  wherever it is written down.
  """
  @spec rows() :: %{name() => row()}
  def rows() do
    Map.new(for {name, row} <- Keys.rows(), row.type == :ms, do: {name, duration(row)})
  end

  @doc "The row `name` declares, or `nil` when it declares no duration."
  @spec row(name()) :: row() | nil
  def row(name) do
    case Keys.row(name) do
      %{type: :ms} = row -> duration(row)
      _ -> nil
    end
  end

  defp duration(row), do: Map.take(row, [:default, :key, :env, :derived])

  @doc """
  What a site waits: the user's override for this row, else the row's
  derivation over its parent, else the declared default.
  """
  @spec get(name()) :: non_neg_integer()
  def get(name), do: Config.get(duration!(name))

  @doc "The declared value alone - no override, no environment."
  @spec default(name()) :: non_neg_integer()
  def default(name), do: Config.default(duration!(name))

  defp duration!(name) do
    row(name) || raise ArgumentError, "no duration row named #{inspect(name)}"
    name
  end
end
