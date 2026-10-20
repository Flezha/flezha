defmodule Flezha.Relay.Calls.Token do
  @moduledoc """
  The token a call rides and the metadata keys that carry it: the one place
  that knows how a token is spelled, minted and read.

  An outside call's token is `w:` and random bytes (`mint/0`); a relay's call
  rides a relay token built from the controller's own delivery
  (`relay_token/1`), read back with `caller_of/1`. Readers name a key through
  a module attribute set from `reply_to_key/0` or `call_key/0`, so the key is
  a literal in their code.
  """

  @reply_to "flz.reply_to"
  @call "flz.call"
  @answer_channel "flz.call_answer_channel"
  @source "flz.call_source"
  # The controller stamps these on its own re-forwards and journal answers;
  # no sender may set them.
  @routing [@reply_to, "flz.replay", "flz.replay_end", "flz.edge", "flz.offset"]

  @type t :: binary()

  @doc "The metadata key a call's token rides."
  @spec reply_to_key() :: binary()
  def reply_to_key, do: @reply_to

  @doc "The metadata key that marks an output as a request of its relay's own call."
  @spec call_key() :: binary()
  def call_key, do: @call

  @doc "The metadata key naming the one channel a call's answer comes from."
  @spec answer_channel_key() :: binary()
  def answer_channel_key, do: @answer_channel

  @doc "The metadata key naming who made an outside call."
  @spec source_key() :: binary()
  def source_key, do: @source

  @doc "A fresh outside-call token: `w:` and 16 random bytes, unpadded base64url."
  @spec mint() :: t()
  def mint, do: "w:" <> Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)

  @doc """
  `metadata` as `strip/1` admits it, carrying `token` as its one
  `flz.reply_to`, and without the answer channel its row keeps or the source
  its call event named.
  """
  @spec entry_metadata(map(), t()) :: map()
  def entry_metadata(metadata, token),
    do: metadata |> Map.drop([@answer_channel, @source | @routing]) |> Map.put(@reply_to, token)

  @doc """
  `metadata` without the keys only the controller sets (`flz.reply_to`,
  `flz.replay`, `flz.replay_end`, `flz.edge`, `flz.offset`): what every other
  entry point admits. `flz.msg_id` passes, so a sender's id still absorbs a
  duplicate.
  """
  @spec strip(map()) :: map()
  def strip(metadata), do: Map.drop(metadata, @routing)

  @doc """
  Put back on `after_hooks` the keys only the controller sets as
  `before_hooks` arrived with them, the token included, so no `:on_input` hook
  adds, removes or rewrites one.
  """
  @spec restore(map(), map()) :: map()
  # Metadata no hook changed is answered as it is.
  def restore(unchanged, unchanged), do: unchanged

  def restore(after_hooks, before_hooks),
    do: after_hooks |> Map.drop(@routing) |> Map.merge(Map.take(before_hooks, @routing))

  @doc """
  The token a relay's call rides: built from the delivery's own fields,
  never read from metadata. It carries the answer channel, so a callee that
  restarted since it took the call still answers only from that channel.
  """
  @spec relay_token(map()) :: t()
  def relay_token(%{caller: caller, call: call_id, created: created, deadline_at: at} = call),
    do:
      "o:#{caller}:#{call_id}:#{if created, do: 1, else: 0}:#{at}:" <>
        Base.url_encode64(Map.get(call, :answer_channel) || "", padding: false)

  @doc "Whether `token` is a relay token, not an outside call's."
  @spec relay_token?(t()) :: boolean()
  def relay_token?("o:" <> _rest), do: true
  def relay_token?(_token), do: false

  @doc """
  The caller, the call, whether it started its callee, its deadline and its
  answer channel, read off a relay token.
  """
  @spec caller_of(t()) ::
          {:ok,
           %{
             caller: binary(),
             call: binary(),
             created: boolean(),
             deadline_at: integer(),
             answer_channel: binary() | nil
           }}
          | :error
  def caller_of("o:" <> rest) do
    with [answer, at, created, call_id, caller] when created in ["0", "1"] and caller != "" <-
           rest |> String.reverse() |> String.split(":", parts: 5),
         {deadline_at, ""} <- Integer.parse(String.reverse(at)),
         {:ok, answer_channel} <- Base.url_decode64(String.reverse(answer), padding: false) do
      {:ok,
       %{
         caller: String.reverse(caller),
         call: String.reverse(call_id),
         created: created == "1",
         deadline_at: deadline_at,
         answer_channel: if(answer_channel != "", do: answer_channel)
       }}
    else
      _ -> :error
    end
  end

  def caller_of(_token), do: :error

  @doc """
  The id a call's events carry: a digest of its token, from which the token
  cannot be recovered.
  """
  @spec call_id(t()) :: binary()
  def call_id(token), do: digest([token])

  @doc "A durable call's id, from its output's identity: a re-dispatch keeps it."
  @spec durable_id(map()) :: binary()
  def durable_id(metadata),
    do: digest([metadata["flz.cell"], metadata["flz.seq"], metadata["flz.epoch"]])

  defp digest(parts) do
    :sha256
    |> :crypto.hash(Enum.intersperse(Enum.map(parts, &to_string/1), <<0>>))
    |> binary_part(0, 16)
    |> Base.url_encode64(padding: false)
  end
end
