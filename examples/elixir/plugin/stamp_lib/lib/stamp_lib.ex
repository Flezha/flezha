defmodule StampLib.Stamp do
  @moduledoc false
  defstruct [:prefix, :text]
end

defimpl String.Chars, for: StampLib.Stamp do
  def to_string(%{prefix: prefix, text: text}), do: "#{prefix}[#{text}]"
end

defmodule StampLib do
  @moduledoc """
  Renders the stamp read from `priv/stamp.txt` when the application started,
  after the `prefix` the library is configured with.
  """

  @doc "The stamp, as a struct that renders through `String.Chars`."
  def stamp,
    do: %StampLib.Stamp{
      prefix: Application.fetch_env!(:stamp_lib, :prefix),
      text: Agent.get(__MODULE__, & &1)
    }
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
