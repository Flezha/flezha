defmodule Flezha.Service.Cli.Context do
  @moduledoc """
  The identity a `flz` verb's trailing options carry, and the rendering of a
  refusal and an error on the stream the verb's family prints to. Every other
  `Cli.*` module calls this one; it calls none of them. The authorization
  itself is `Flezha.AuthZ.Verb`'s, bound in `Flezha.Service.CliManager`.
  """

  alias Flezha.AuthZ.TokenStore

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
  Halt the verb `action` with the refusal `reason`, on the stream its verb
  family prints errors to. The one spelling of an authorization refusal, so a
  gated verb never invents its own wording.
  """
  @spec refuse!(atom() | String.t(), term(), :stdio | :stderr) :: no_return()
  def refuse!(action, reason, device \\ :stderr) do
    cli_error!("#{action}: #{render_forbidden(reason)}", device)
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

  # Fail the surrounding `flzvm rpc` evaluation: print the message, then exit
  # only this rpc process. Never System.halt here — every CliManager call runs
  # ON the controller node via rpc, so halting would stop the controller
  # itself; :rpc turns the exit into {:badrpc, ...}, which the release script
  # converts into a non-zero local exit code for the calling script.
  # `device` mirrors each call site's historical stream (legacy operative and
  # plugin verbs print errors to stdout; the newer state/observe/audit/dlq and
  # schedule verbs print to stderr) — the stream is observable behavior.
  @spec cli_error!(String.t(), :stdio | :stderr) :: no_return()
  def cli_error!(message, device \\ :stdio) do
    IO.puts(device, message)
    exit(:cli_error)
  end
end
