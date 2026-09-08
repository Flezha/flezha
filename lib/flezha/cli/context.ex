defmodule Flezha.Service.Cli.Context do
  @moduledoc """
  The identity a `flz` verb's trailing options carry, the rendering of a refusal
  and an error, and the one device they are written to. Every other `Cli.*`
  module calls this one; it calls none of them. The authorization itself is
  `Flezha.AuthZ.Verb`'s, bound in `Flezha.Service.CliManager`.
  """

  alias Flezha.AuthZ.Config
  alias Flezha.AuthZ.TokenStore
  alias Flezha.Config.Durations

  # `Kernel.CLI.exec_fun/2` halts `{:shutdown, int}` with that int and prints
  # nothing, and prints an `** (exit)` banner with erl_eval frames for every
  # other reason before halting 1. Both halt 1, so this moves no `flz` exit
  # code and leaves the operator reading the sentence alone.
  @cli_exit {:shutdown, 1}

  @doc """
  A reason as text: the one `inspect/1` on this surface. A binary reason is
  already a sentence and stays one — quoting it is an artefact of inspecting,
  not something the operator asked to see.
  """
  @spec format_reason(term()) :: String.t()
  def format_reason(reason) when is_binary(reason), do: reason
  def format_reason(reason), do: inspect(reason)

  def atom_converter(orig_map) do
    string_to_atom = %{
      "on_input" => :on_input,
      "on_output" => :on_output,
      "on_interval" => :on_interval,
      "on_operative_init" => :on_operative_init,
      "on_message" => :on_message,
      "on_snapshot" => :on_snapshot,
      "on_terminate" => :on_terminate,
      "snapshot_interval" => :snapshot_interval,
      "plugins" => :plugins,
      "partitions" => :partitions,
      "group" => :group,
      "partition" => :partition,
      "key_group_from" => :key_group_from,
      "key_group_to" => :key_group_to
    }

    Enum.reduce(orig_map, %{}, fn {key, value}, acc ->
      new_key = Map.get(string_to_atom, key, key)
      Map.put(acc, new_key, value)
    end)
  end

  def put_unless_nil(map, _key, nil), do: map
  def put_unless_nil(map, key, value), do: Map.put(map, key, value)

  def label_or(nil, default), do: default
  def label_or(value, _default), do: to_string(value)

  @doc """
  The caller context a verb's trailing options describe. Identity comes ONLY
  from a verified token (`flz --token` / `FLEZHA_TOKEN`, threaded through as
  `opts[:token]`); a caller-supplied principal, namespace or role is never
  trusted, so without a resolvable token the context stays unbound and a gated
  verb refuses naming the credential.
  """
  def cli_context(opts) do
    base = %{source: :cli}

    base =
      case Keyword.get(opts, :request_id) do
        nil -> base
        id when is_binary(id) -> Map.put(base, :request_id, id)
      end

    case Keyword.get(opts, :token) do
      token when is_binary(token) and token != "" -> bind_token_identity(base, token)
      _ -> base
    end
  end

  defp bind_token_identity(base, token) do
    case TokenStore.verify(token) do
      {:ok, %{principal: principal, namespace: namespace, role: role}} ->
        base
        |> Map.put(:principal, principal)
        |> Map.put(:namespace, namespace)
        |> Map.put(:role, role)

      {:error, _reason} ->
        base
    end
  catch
    # The token store is not running while auth is disabled; an unbound
    # context is the correct degraded shape either way.
    _, _ -> base
  end

  @doc """
  Halt the verb `action` with the refusal `reason`. The one spelling of an
  authorization refusal, so a gated verb never invents its own wording.
  """
  @spec refuse!(atom() | String.t(), term()) :: no_return()
  def refuse!(action, reason) do
    cli_error!("#{action}: #{render_forbidden(reason)}")
  end

  def render_forbidden(:bootstrap_pending) do
    "refused: authorization is enforced but the bootstrap has not been claimed — " <>
      "run `flz auth bootstrap <token>` with the token in " <>
      Config.bootstrap_token_path()
  end

  def render_forbidden(:principal_not_bound) do
    "refused: no verifiable identity — supply `--token <token>` or set FLEZHA_TOKEN"
  end

  def render_forbidden(:no_role_binding) do
    "refused: the presented identity has no role binding — ask an admin for `flz role bind`"
  end

  def render_forbidden(reason), do: "refused: #{reason}"

  @doc """
  Halt the verb with `reason`, rendered in `vocabulary`'s words when it names
  that reason and by the one fallback here when it does not. The single prefix
  and the single `inspect/1` for the whole surface: a family contributes its
  vocabulary and never its printing.
  """
  @spec fail!(term(), (term() -> String.t() | nil)) :: no_return()
  def fail!(reason, vocabulary \\ fn _ -> nil end) do
    cli_error!("error: " <> (vocabulary.(reason) || format_reason(reason)))
  end

  @doc """
  Halt the verb with `message` exactly as written. `fail!/2` is what a family
  reaches for; this is the one whose whole text is a verdict an operator's
  script reads, so it carries no `error: ` prefix and still exits the same way.
  """
  @spec halt!(String.t()) :: no_return()
  def halt!(message), do: cli_error!(message)

  @doc """
  A way to tell the operator one line, bound now and called later from any
  process.

  `flz controller stop`'s progress lines are emitted by the shutdown
  coordinator, not by the rpc process, so the device has to be resolved on the
  way in: a lookup made inside the telemetry handler would run in the emitting
  process and answer the controller.
  """
  @spec notifier() :: (String.t() -> :ok)
  def notifier() do
    device = operator_stderr()

    &puts(device, &1)
  end

  # The device a verb writes something that is not its answer to: the
  # operator's stderr, on whichever node the verb is running.
  #
  # `flzvm rpc` is `elixir --rpc-eval`, which is `:erpc.call/4`, so a verb body
  # runs on the controller with the *caller's* group leader, and stdout already
  # travels back over distribution. `:stderr` does not: it is `:standard_error`,
  # a process registered per node, and the controller's is the controller's log.
  # The caller's is resolved instead, so no verb names a stream of its own.
  @spec operator_stderr() :: :stderr | pid()
  defp operator_stderr() do
    caller_node = node(Process.group_leader())

    if caller_node == node(), do: :stderr, else: remote_standard_error(caller_node)
  end

  # A node that just called us is connected, so this answers quickly or not at
  # all; either way the controller's own stderr is the honest fallback.
  defp remote_standard_error(caller_node) do
    budget = Durations.get(:cli_operator_device_ms)

    case :erpc.call(caller_node, :erlang, :whereis, [:standard_error], budget) do
      device when is_pid(device) -> device
      _ -> :stderr
    end
  catch
    _, _ -> :stderr
  end

  # The device may have died with the caller that owned it. A refusal must
  # still exit the way it says it does, so a device that will not take the line
  # loses the line rather than replacing the exit with a raise.
  @spec puts(:stderr | pid(), String.t()) :: :ok
  defp puts(device, message) do
    IO.puts(device, message)
  catch
    _, _ -> :ok
  end

  # The one printer. Private, and so are the device and the exit it uses, so a
  # `Cli.*` module contributes what it says and never how it is said: every
  # failure reaches the operator through `fail!/2` or `halt!/1`.
  #
  # Fail the surrounding `flzvm rpc` evaluation, then exit only this rpc
  # process. Never System.halt here: every CliManager call runs ON the
  # controller node, so halting would stop the controller itself.
  #
  # One stream, for every family: an operator who pipes a verb's output is
  # collecting the answer, not the failure, so a failure never joins the data.
  @spec cli_error!(String.t()) :: no_return()
  defp cli_error!(message) do
    puts(operator_stderr(), message)
    exit(@cli_exit)
  end

  @doc """
  A JSON document the operator typed. Unlike `Payloader.json_to_map/1`, whose
  moduledoc calls an undecodable document a defect, an operator's document that
  will not decode is a refusal: `what` names the flag it was written behind.
  """
  @spec json!(String.t() | nil, String.t()) :: map()
  def json!(written, what) do
    case written do
      nil -> %{}
      "" -> %{}
      text -> decode_json!(text, what)
    end
  end

  defp decode_json!(text, what) do
    case Jason.decode(text) do
      {:ok, decoded} when is_map(decoded) -> decoded
      {:ok, _other} -> cli_error!("error: #{what} must be a JSON object")
      {:error, reason} -> cli_error!("error: invalid #{what} JSON: #{Exception.message(reason)}")
    end
  end

  @doc """
  A whole number the operator typed, refused rather than guessed: a `--limit`
  nobody can read is not every record.
  """
  @spec integer!(term(), String.t()) :: integer()
  def integer!(written, what) do
    case Integer.parse(to_string(written)) do
      {value, ""} -> value
      _ -> cli_error!("error: invalid #{what} value '#{written}'")
    end
  end

  @doc """
  The value `written` names in `rows` — `{spelling, value}` in the order the
  accepted set is printed in — or a refusal naming the word and that set.

  The ONE way this surface tells an operator a word is not one it knows.
  `atom!/3` is this answered by halting; a family that threads its refusal out
  as a value reads the message instead.
  """
  @spec one_of(String.t(), [{String.t(), term()}], String.t()) ::
          {:ok, term()} | {:error, String.t()}
  def one_of(written, rows, what) do
    case List.keyfind(rows, written, 0) do
      {_written, value} ->
        {:ok, value}

      nil ->
        {:error,
         "unknown #{what} '#{written}'. Use " <>
           Enum.map_join(rows, " or ", &"'#{elem(&1, 0)}'") <> "."}
    end
  end

  @doc """
  A word the operator typed, as the atom this surface knows it by. Never
  `String.to_atom/1`: a word nobody declared would mint an atom the node then
  keeps for ever.
  """
  @spec atom!(String.t(), [atom()], String.t()) :: atom()
  def atom!(written, allowed, what) do
    case one_of(written, Enum.map(allowed, &{Atom.to_string(&1), &1}), what) do
      {:ok, known} -> known
      {:error, message} -> cli_error!("error: " <> message)
    end
  end
end
