defmodule Flezha.Runner.Admission.IdentityPin do
  @moduledoc """
  Refuse a connect whose stable identity is held under another credential.

  A relay is owned by its runner's stable identity, so presenting an
  identity is the one door to another runner's relays. While the identity
  has a session in the roster (live, or kept inactive for its removal grace) or a
  handshake in flight, admitted under one credential, a connect presenting it
  under a different credential is refused rather than taken over. The pin
  lapses when the identity's last session leaves the roster. A connect that
  carries no per-runner credential (certificate-proven, or posture off) is
  neither pinned nor refused.
  """
  @behaviour Flezha.Runner.Admission.Check

  alias Flezha.Runner.FleetManager.Roster

  require Logger

  @impl true
  def check(%{stable_id: stable_id, credential: credential}, %{state: state})
      when is_binary(stable_id) and is_binary(credential) do
    if held_elsewhere?(state, stable_id, credential) do
      Logger.warning(
        "[FleetManager] runner connect refused: stable identity #{stable_id} is held " <>
          "under another credential"
      )

      {:error, {:identity_taken, stable_id}}
    else
      :ok
    end
  end

  def check(_connect, _context), do: :ok

  defp held_elsewhere?(state, stable_id, credential) do
    pinned_live? =
      case Map.get(state.stable_sessions, stable_id) do
        {session, held, _incarnation} when is_binary(held) and held != credential ->
          Roster.find_runner_namespace(state, session) != nil

        _unpinned_or_same ->
          false
      end

    pinned_live? or
      Enum.any?(state.pending_handshakes, fn {_session, %{connect: connect}} ->
        connect.stable_id == stable_id and connect.credential not in [nil, credential]
      end)
  end
end
