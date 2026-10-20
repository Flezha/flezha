defmodule Flezha.Relay.Ingest.Verdict do
  @moduledoc """
  What a relay's ingest answers a sender, and the one test every reader of it
  makes. It names no other module, so a reader that guards on `taken?/1`
  takes no compile-time dependency on the relay's ingest path.
  """

  @taken [:ok, {:ok, :suspended}, {:ok, :absorbed}]

  @typedoc """
  `:ok` (delivered or journaled), `{:ok, :absorbed}` (filtered, or a duplicate
  the input log holds), `{:ok, :suspended}` (journaled for an instance asleep),
  or an error the sender may retry: the relay, named, not taking input or its
  log refusing the append, or a relay that could not be reached.
  """
  @type t ::
          :ok
          | {:ok, :absorbed | :suspended}
          | {:error,
             {:not_accepting_input | :input_refused, String.t()} | {:unreachable, term()} | term()}

  @doc """
  Whether `verdict` took the input: a duplicate absorbed was taken the first
  time. A reader asks this, never `== :ok`, before it moves a position or
  ends a call.
  """
  defguard taken?(verdict) when verdict in @taken
end
