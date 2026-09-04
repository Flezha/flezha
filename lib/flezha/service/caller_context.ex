defmodule Flezha.Service.CallerContext do
  @moduledoc """
  The context every service-API call carries as its last argument, its
  validation, and the audit-of-audit signal a call emits before doing its work.

  The shape is locked here so a caller — the CLI today, a UI later — is
  validated the same way by every service module. What a module puts on the
  signal beyond `:request_id` is its own: the audit log carries the principal,
  observability does not.

  Three guards say how much a published function checks, weakest first:
  `validated/2` (the shape only, for a verb the policy names no action for),
  `scoped_read/4` (an action, in a namespace the caller resolved, unaudited, as
  the CLI's reads are) and `gated/6` (an action, then the effect, then an audit
  of what happened). Which one a function uses is the plainest statement of how
  governed it is.

  `Flezha.Service.AuditLog` and `Flezha.Service.Observability` predate the three
  and still inline their own: both authorize in the namespace the *caller*
  declares, and observability audits before its effect rather than after.
  Converting them would change both of those, so they keep their shape.
  """

  alias Flezha.AuthZ
  alias Flezha.AuthZ.Namespace
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

  @doc """
  The namespace a cluster-wide verb is judged in.

  One spelling, over `Flezha.AuthZ.Namespace`'s own name for it, so a published
  module does not carry a copy of the string.
  """
  @spec system_scope() :: String.t()
  def system_scope, do: Namespace.system_name()

  defp scope_for(caller_context) do
    %{namespace: Map.get(caller_context, :namespace) || system_scope()}
  end

  @doc """
  A published function whose verb `Flezha.AuthZ.Action` names no action for: the
  context's shape is checked, and nothing else.

  The weakest of the three guards, named for what it checks rather than for a
  read, because some of what it guards mutates — a schedule, a partition group,
  a rewound loop. Those verbs are as gated here as they are from the terminal,
  which authorizes none of them either. Naming their actions needs role grants,
  which is a decision for the repository owner and not one a service module can
  take.
  """
  @spec validated(t(), (-> result)) :: result | {:error, :invalid_caller_context}
        when result: var
  def validated(caller_context, fun) do
    with :ok <- validate(caller_context), do: fun.()
  end

  @doc """
  A published read that is authorized and not audited, as the CLI's own reads
  are: `action` is checked in `namespace`, then `fun` runs.
  """
  @spec scoped_read(t(), atom(), String.t(), (-> result)) :: result | {:error, term()}
        when result: var
  def scoped_read(caller_context, action, namespace, fun) do
    with :ok <- authorize(scoped(caller_context, namespace), action), do: fun.()
  end

  @doc """
  A published mutation: `action` is authorized in `namespace`, `fun` runs, and
  its success is audited as `audit_source` under the verb `action` names.

  `namespace` is resolved by the caller from what it is acting on, never taken
  from the context, so a consumer cannot choose the scope its own request is
  judged in. A refusal runs nothing and audits nothing, and a `fun` that
  answers `{:error, _}` is not audited either — the audit says what happened,
  which is what the CLI's `with_cli_authorization/5` has always meant by it.
  """
  @spec gated(t(), atom(), String.t(), atom(), map(), (-> result)) :: result | {:error, term()}
        when result: var
  def gated(caller_context, action, namespace, audit_source, target, fun) do
    with :ok <- authorize(scoped(caller_context, namespace), action) do
      audited(fun.(), audit_source, verb(action), caller_context, target, namespace)
    end
  end

  defp audited({:error, _reason} = error, _source, _verb, _caller_context, _target, _namespace),
    do: error

  defp audited(result, source, verb, caller_context, target, namespace) do
    emit(source, verb, caller_context, Map.put(target, :namespace, namespace))
    result
  end

  defp scoped(caller_context, namespace), do: Map.put(caller_context, :namespace, namespace)

  # `:"dlq.drop"` audits as `:drop`: the signal's source already names the
  # domain, so repeating it in the name would read `dlq_service dlq.drop`.
  defp verb(action) do
    action |> Atom.to_string() |> String.split(".") |> List.last() |> String.to_atom()
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
