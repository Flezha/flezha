defmodule Flezha.Service.Cli.Context do
  @moduledoc """
  The identity a `flz` verb's trailing options carry, the one device a failure
  is written to, and the window a streaming verb receives for. Every other
  `Cli.*` module calls this one; it calls none of them. The authorization itself
  is the published function's, reached through `Flezha.Service.Admin`, and every
  failure reaches the caller through `render!/2`, in `Flezha.Error`'s words.
  """

  alias Flezha.Config.Durations
  alias Flezha.Service.Admin

  @stderr {__MODULE__, :stderr}

  def label_or(nil, default), do: default
  def label_or(value, _default), do: to_string(value)

  @doc """
  The caller context a verb's trailing options describe. Identity comes ONLY
  from a verified token (`flz --token` / `FLEZHA_TOKEN`, threaded through as
  `opts[:token]`); a caller-supplied principal, namespace or role is never
  trusted. Without a token the context stays unbound and a gated verb refuses
  naming the credential; a token that does not verify is refused through
  `render!/2` before the verb runs. An empty token is no token, as `flz` reads
  an empty `--token`.
  """
  def cli_context(opts) do
    token = with "" <- Keyword.get(opts, :token), do: nil

    case Admin.context(:cli, token) do
      {:ok, context} -> with_request_id(context, Keyword.get(opts, :request_id))
      {:error, %Flezha.Error{} = refused} -> render!(refused, opts)
    end
  end

  defp with_request_id(context, nil), do: context
  defp with_request_id(context, id) when is_binary(id), do: Map.put(context, :request_id, id)

  @doc """
  The one printer: `error` rendered in the format the call's options asked for,
  on the caller's stderr, then the verb exits with the code of the error's
  class.

  Only the request's process exits: `{:shutdown, n}` is the code its
  `cli_exit` frame carries, with no banner. Never `System.halt` here, because
  every `Flezha.Service.Cli` call runs on the controller node and would stop it.
  """
  @spec render!(Flezha.Error.t(), keyword()) :: no_return()
  def render!(%Flezha.Error{} = error, opts), do: exit({:shutdown, report(error, opts)})

  @doc "`render!/2`'s printing alone: `error` on the caller's stderr, answering its exit code."
  @spec report(Flezha.Error.t(), keyword()) :: pos_integer()
  def report(%Flezha.Error{} = error, opts) do
    puts(caller_stderr(), Flezha.Error.render(error, format(opts)))
    Flezha.Error.exit_code(error)
  end

  @doc """
  The output format the caller asked for, `"text"` or `"json"`: the `output`
  its options carry, which `flz` sets for `--json` and `--output json` on
  every verb.
  """
  @spec format(keyword()) :: String.t()
  def format(opts) do
    case Keyword.get(opts, :output) do
      "json" -> "json"
      _text -> "text"
    end
  end

  @doc """
  A way to tell the caller one line, bound now and called later from any
  process.

  `flz controller stop`'s progress lines are emitted by the shutdown
  coordinator, not by the request's process, so the device has to be resolved
  on the way in: a lookup made inside the telemetry handler would run in the
  emitting process and answer the controller.
  """
  @spec notifier() :: (String.t() -> :ok)
  def notifier() do
    device = caller_stderr()

    &puts(device, &1)
  end

  @doc """
  Bind `device` as the stderr of the request this process serves, so every
  failure and notice the verb writes reaches the caller rather than the
  controller's log.
  """
  @spec bind_stderr(pid()) :: :ok
  def bind_stderr(device) when is_pid(device) do
    Process.put(@stderr, device)
    :ok
  end

  # The device a verb writes something that is not its answer to: the one its
  # request bound, or this node's own stderr when it runs outside a request.
  @spec caller_stderr() :: :stderr | pid()
  defp caller_stderr(), do: Process.get(@stderr, :stderr)

  # The device may have died with the caller that owned it. A refusal must
  # still exit the way it says it does, so a device that will not take the line
  # loses the line rather than replacing the exit with a raise.
  @spec puts(:stderr | pid(), String.t()) :: :ok
  defp puts(device, message) do
    IO.puts(device, message)
  catch
    _, _ -> :ok
  end

  @doc """
  How long a streaming verb runs: the `duration_ms` its options carry, or the
  window `flz` uses when the caller names none.
  """
  @spec window(keyword()) :: integer()
  def window(opts),
    do: Keyword.get_lazy(opts, :duration_ms, fn -> Durations.get(:cli_stream_window_ms) end)
end
