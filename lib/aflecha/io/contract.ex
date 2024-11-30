defmodule Aflecha.IO.Contract do
  @moduledoc """
  Contract for IO processes.
  """

  defmacro __using__(_opts) do
    quote do
      @behaviour Aflecha.IO.Contract

      defimpl Aflecha.IO.Provider, for: __MODULE__ do
        @impl true
        def mfa(io_settings), do: {__MODULE__, :start_link, [io_settings]}
      end
    end
  end

  @callback send_payload(pid :: pid(), msg :: term()) :: :ok
end
