defmodule Aflecha.IO.Contract do
  @moduledoc """
  Contract for IO processes.
  """

  defmacro __using__(_opts) do
    quote do
      @behaviour Aflecha.IO.Contract

      defimpl Aflecha.IO.Provider, for: unquote(__CALLER__.module) do
        @impl true
        def mfa(io_settings), do: {unquote(__CALLER__.module), :start_link, [io_settings]}
      end
    end
  end

  @callback send_payload(pid :: pid(), msg :: term()) :: :ok
end
