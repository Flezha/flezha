defmodule Flezha.Service.CallerContext do
  @moduledoc """
  The context every service-API call carries as its last argument, its
  validation, and the audit-of-audit signal a call emits before doing its work.

  The shape is locked here so a caller — the CLI today, a UI later — is
  validated the same way by every service module. What a module puts on the
  signal beyond `:request_id` is its own: the audit log carries the principal,
  observability does not.
  """

  alias Flezha.AuthZ
  alias Flezha.Observability.{Bus, Signal}

  @valid_sources [:cli, :ui, :sdk, :internal]

  @type t :: %{
          required(:source) => :cli | :ui | :sdk | :internal,
          optional(:principal) => term() | nil,
          optional(:request_id) => binary() | nil
        }

  @spec validate(t()) :: :ok | {:error, :invalid_caller_context}
  def validate(%{source: src}) when src in @valid_sources, do: :ok
  def validate(_), do: {:error, :invalid_caller_context}

  @doc """
  The guard a service-API call runs before its work: a context of a known
  shape, a bound principal, and `action` authorized in the caller's namespace
  scope. Whether the subsystem is enabled at all is the module's own question
  and is asked before this.
  """
  @spec authorize(t(), atom()) :: :ok | {:error, term()}
  def authorize(caller_context, action) do
    with :ok <- validate(caller_context),
         :ok <- AuthZ.require_bound_principal(caller_context) do
      AuthZ.authorize(caller_context, action, scope_for(caller_context))
    end
  end

  defp scope_for(caller_context) do
    %{namespace: Map.get(caller_context, :namespace) || "_system"}
  end

  @spec put_unless_nil(map(), atom(), term()) :: map()
  def put_unless_nil(map, _key, nil), do: map
  def put_unless_nil(map, key, value), do: Map.put(map, key, value)

  @spec emit(atom(), atom(), t(), map()) :: :ok
  def emit(source, action, caller_context, extra) do
    metadata =
      extra
      |> Map.put(:action, action)
      |> Map.put(:caller_source, Map.get(caller_context, :source))
      |> put_unless_nil(:request_id, Map.get(caller_context, :request_id))

    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: source,
        name: action,
        measurements: %{count: 1},
        metadata: metadata
      })
    )

    :ok
  end
end
