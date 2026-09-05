defmodule Flezha.Service.Cli.Context do
  @moduledoc """
  The identity a `flz` verb's trailing options carry, and the rendering of a
  refusal and an error on the stream the verb's family prints to. Every other
  `Cli.*` module calls this one; it calls none of them. The authorization
  itself is `Flezha.AuthZ.Verb`'s, bound in `Flezha.Service.CliManager`.
  """

  alias Flezha.AuthZ.TokenStore

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
      "run `flz auth bootstrap <token>` (token path is logged at controller boot)"
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

  # The one printer. Private, so a `Cli.*` module contributes what it says and
  # never how it is said: every failure reaches the operator through `fail!`.
  #
  # Fail the surrounding `flzvm rpc` evaluation: print the message on stderr,
  # then exit only this rpc process. Never System.halt here — every CliManager
  # call runs ON the controller node via rpc, so halting would stop the
  # controller itself; :rpc turns the exit into {:badrpc, ...}, which the
  # release script converts into a non-zero local exit code for the calling
  # script.
  #
  # One stream, for every family: an operator who pipes a verb's output is
  # collecting the answer, not the failure, so a failure never joins the data.
  @spec cli_error!(String.t()) :: no_return()
  defp cli_error!(message) do
    IO.puts(:stderr, message)
    exit(:cli_error)
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
  A word the operator typed, as the atom this surface knows it by. Never
  `String.to_atom/1`: a word nobody declared would mint an atom the node then
  keeps for ever.
  """
  @spec atom!(String.t(), [atom()], String.t()) :: atom()
  def atom!(written, allowed, what) do
    case Enum.find(allowed, &(Atom.to_string(&1) == written)) do
      nil ->
        cli_error!(
          "error: unknown #{what} '#{written}'. Use " <>
            Enum.map_join(allowed, " or ", &"'#{&1}'") <> "."
        )

      known ->
        known
    end
  end
end
