defmodule StampLib.Stamp do
  @moduledoc false
  defstruct [:text]
end

defimpl String.Chars, for: StampLib.Stamp do
  def to_string(%{text: text}), do: "[#{text}]"
end

defmodule StampLib do
  @moduledoc "Renders the stamp read from `priv/stamp.txt` when the application started."

  @doc "The stamp, as a struct that renders through `String.Chars`."
  def stamp, do: %StampLib.Stamp{text: Agent.get(__MODULE__, & &1)}
end

defmodule StampLib.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    text =
      :stamp_lib |> :code.priv_dir() |> Path.join("stamp.txt") |> File.read!() |> String.trim()

    agent = %{id: StampLib, start: {Agent, :start_link, [fn -> text end, [name: StampLib]]}}
    Supervisor.start_link([agent], strategy: :one_for_one)
  end
end
