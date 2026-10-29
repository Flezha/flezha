defmodule Flezha.Logs.Supervisor do
  @moduledoc """
  The durable logs: the `Flezha.Logs.Registry` every log process registers
  in, then one subtree per kind that runs on this controller.

  A kind's subtree starts, in order: its own children, the writer, its
  rotation and retention workers, and, for a bus-fed kind, its sink last, so
  it subscribes only once the writer takes records. A kind that is off starts
  nothing, so no disk I/O happens for it.

  """

  use Supervisor

  alias Flezha.Logs.{Kinds, Retention, Rotation, Writer}
  alias Flezha.Process.Cspec

  def start_link(opts \\ []), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    kinds =
      for kind <- Kinds.all(), Kinds.enabled?(kind) do
        Cspec.build(
          kind.id,
          {Supervisor, :start_link, [kind_children(kind), [strategy: :one_for_one]]},
          :supervisor_permanent
        )
      end

    Supervisor.init([{Registry, keys: :unique, name: Flezha.Logs.Registry} | kinds],
      strategy: :rest_for_one
    )
  end

  defp kind_children(kind) do
    feed =
      case kind.feed do
        {:bus, sink} -> [sink]
        :direct -> []
      end

    Enum.map(kind.children, &{&1, kind}) ++
      [{Writer, kind}, {Rotation, kind}, {Retention, kind}] ++ feed
  end
end
