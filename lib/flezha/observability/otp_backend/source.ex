defmodule Flezha.Observability.OtpBackend.Source do
  @moduledoc """
  Shared GenServer plumbing for the periodic observability sources
  (`:beam_vm`, `:host`, `:processhub`, `:flezha_transport`,
  `:flezha_bounded_mailbox`).

  `use Flezha.Observability.OtpBackend.Source, source: :name, default_poll_ms: N`
  injects `child_spec/1`, `start_link/1`, the `@source` / `@default_poll_ms`
  attributes, and `poll_interval_ms/0` (per-source override under
  `:flezha, :observability, :otp_backend, :poll_intervals`, else default).
  The using module supplies its own `init/1`, `handle_info/2`, and collection
  logic.
  """

  defmacro __using__(opts) do
    source = Keyword.fetch!(opts, :source)
    default_poll_ms = Keyword.fetch!(opts, :default_poll_ms)

    quote do
      use GenServer

      @source unquote(source)
      @default_poll_ms unquote(default_poll_ms)

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          restart: :permanent,
          type: :worker
        }
      end

      def start_link(opts \\ []) do
        GenServer.start_link(__MODULE__, opts, name: __MODULE__)
      end

      defp poll_interval_ms() do
        Application.get_env(:flezha, :observability, [])
        |> Keyword.get(:otp_backend, [])
        |> Keyword.get(:poll_intervals, %{})
        |> Map.get(@source, @default_poll_ms)
      end
    end
  end
end
