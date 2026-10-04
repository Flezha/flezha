defmodule Flezha.AuthZ.Verb do
  @moduledoc """
  The one seam that applies a row of `Flezha.AuthZ.Verbs`, in one order for
  every surface: identity, scope, policy, act, audit.

  `run/5` is the seam. A published module binds its functions to rows with
  `verb`:

      use Flezha.AuthZ.Verb

      @doc "Discard a parked entry and its payload."
      verb drop(entry_ref, caller_context),
        verb: :dlq_drop,
        row: &locate/1,
        args: [entry_ref: [type: :string, doc: "The entry."]] do
        Store.drop(entry_ref)
      end

  defines `drop/2`, whose body runs only once the row has been applied to the
  caller context in its last argument. The binding also describes the
  operation for `Flezha.Service.catalogue/0`: `args:` the positional arguments
  before the context, in the vocabulary plugin options use; `stream: true` for
  a function that answers a subscription or reports as it goes; and the first
  sentence of its `@doc`, captured while it compiles, because a release
  strips the docs chunks.

  Every failure leaves `run/5` as `{:error, %Flezha.Error{}}`: a refusal, a
  fetch's reason, a body's `{:error, reason}` and a reason a body left with
  through `Flezha.Error.throw!/1` are all converted at the verb's action, once,
  before the audit record is written.

  A `{:row, field}` scope needs a `row:` fetch, called with the positional
  arguments as a list and answering `{:ok, row}` or `{:error, reason}`; the
  fetch runs only for a bound caller whose role grants the action, so a refusal
  says nothing about whether the row exists. `as:` names the fetched row inside
  a body, `call:` names the whole call. `target:` overrides the row's audit
  labels with a function of the argument list, or of the argument list and the
  act's answer, for an act whose answer names what it acted on.
  """

  alias Flezha.AuthZ
  alias Flezha.AuthZ.{Namespace, Scope, Verbs}
  alias Flezha.Observability.{Bus, Signal}
  alias Flezha.Service.CallerContext

  defmodule Call do
    @moduledoc """
    What a bound body receives: the caller's context, the namespace the verb
    was judged in, and the row a `{:row, _}` scope fetched.
    """
    defstruct [:context, :namespace, :row]

    @type t :: %__MODULE__{
            context: Flezha.Service.CallerContext.t(),
            namespace: String.t() | nil,
            row: map() | nil
          }
  end

  @type effect :: (Call.t() -> term())
  @type fetch :: ([term()] -> {:ok, map()} | {:error, term()})

  # ---------- the seam ----------

  @doc """
  Apply the row `verb` to `caller_context` for the positional `args`, then run
  `effect`. Options: `:row` (the fetch a `{:row, _}` scope reads) and `:target`
  (a function of `args`, or of `args` and the answer, answering the audit's
  target fields; the answer is `nil` for an act audited before it runs or one
  that raised).

  Answers every failure as `{:error, %Flezha.Error{}}`, converted at the row's
  action, and never exits.
  """
  @spec run(atom(), CallerContext.t(), [term()], effect(), keyword()) :: term()
  def run(verb, caller_context, args, effect, opts \\ []) when is_function(effect, 1) do
    row = Verbs.fetch!(verb)

    row
    |> apply_row(caller_context, args, converting(effect, row.action), opts)
    |> converted(row.action)
  end

  # The body's failure, returned or thrown, as the error it means here, so the
  # audit record written around it carries the code.
  defp converting(effect, action) do
    fn call ->
      result =
        try do
          effect.(call)
        catch
          :throw, {Flezha.Error, reason} -> {:error, reason}
        end

      converted(result, action)
    end
  end

  defp converted({:error, reason}, action), do: {:error, Flezha.Error.from_reason(reason, action)}
  defp converted(result, _action), do: result

  defp apply_row(%{action: :public}, caller_context, _args, effect, _opts) do
    with :ok <- CallerContext.validate(caller_context) do
      effect.(call(caller_context, nil, nil))
    end
  end

  defp apply_row(row, caller_context, args, effect, opts) do
    with :ok <- CallerContext.validate(caller_context),
         {:ok, namespace, fetched} <- resolve(row, caller_context, args, opts[:row]),
         :ok <- AuthZ.authorize(caller_context, row.action, Scope.build(namespace)) do
      audited(row, call(caller_context, namespace, fetched), args, opts, effect)
    end
  end

  defp call(caller_context, namespace, fetched),
    do: %Call{context: caller_context, namespace: namespace, row: fetched}

  # ---------- scope ----------

  defp resolve(%{scope: {:argument, n}}, _caller_context, args, _fetch) do
    {:ok, argument_namespace(Enum.at(args, n - 1)), nil}
  end

  defp resolve(%{scope: :system}, _caller_context, _args, _fetch),
    do: {:ok, Namespace.system_name(), nil}

  defp resolve(%{scope: :caller}, caller_context, _args, _fetch),
    do: {:ok, own_namespace(caller_context), nil}

  # A stored row is read only by a caller who may act: bound, and granted the
  # action at all, before the fetch. The namespace check on the row's own
  # namespace is the policy call `apply_row/5` makes next.
  defp resolve(%{scope: {:row, field}} = row, caller_context, args, fetch) do
    with :ok <- AuthZ.require_bound_principal(caller_context),
         :ok <-
           AuthZ.authorize(caller_context, row.action, Scope.build(own_namespace(caller_context))),
         {:ok, fetched} <- fetch_row(row, fetch, args) do
      {:ok, Map.fetch!(fetched, field), fetched}
    end
  end

  defp fetch_row(row, nil, _args) do
    raise ArgumentError,
          "verb #{inspect(row.verb)} has a #{inspect(row.scope)} scope and its binding supplies no row: fetch"
  end

  defp fetch_row(_row, fetch, args) when is_function(fetch, 1), do: fetch.(args)

  defp argument_namespace(nil), do: Namespace.system_name()
  defp argument_namespace(namespace), do: Namespace.or_default(namespace)

  defp own_namespace(caller_context),
    do: Map.get(caller_context, :namespace) || Namespace.system_name()

  # ---------- act and audit ----------

  defp audited(%{audit: :none}, call, _args, _opts, effect), do: effect.(call)

  defp audited(%{audit: {:before, source}} = row, call, args, opts, effect) do
    emit(row, source, call, target(row, args, opts, nil), %{})
    effect.(call)
  end

  # An authorized act that failed is still an act: the outcome rides the
  # record, and an exit or raise is re-raised once it is written.
  defp audited(%{audit: {:after, source}} = row, call, args, opts, effect) do
    {outcome, result} = observe(effect, call)
    emit(row, source, call, target(row, args, opts, answer(result)), outcome)
    settle(result)
  end

  defp observe(effect, call) do
    result = effect.(call)
    {outcome_of(result), {:value, result}}
  catch
    kind, reason -> {%{outcome: :error}, {:caught, kind, reason, __STACKTRACE__}}
  end

  defp outcome_of({:error, %Flezha.Error{code: code}}), do: %{outcome: :error, error_code: code}
  defp outcome_of(:error), do: %{outcome: :error}
  defp outcome_of(_result), do: %{outcome: :ok}

  defp answer({:value, result}), do: result
  defp answer({:caught, _kind, _reason, _stacktrace}), do: nil

  defp settle({:value, result}), do: result
  defp settle({:caught, kind, reason, stacktrace}), do: :erlang.raise(kind, reason, stacktrace)

  # One record shape for "a caller did X": the action, where it was judged,
  # who asked and from which surface, plus the row's target fields.
  defp emit(row, source, call, target, extra) do
    metadata =
      target
      |> Map.merge(extra)
      |> Map.merge(%{
        action: row.action,
        namespace: call.namespace,
        principal: Map.get(call.context, :principal),
        caller_source: Map.get(call.context, :source),
        caller_roles: caller_roles(call.context)
      })
      |> with_request_id(Map.get(call.context, :request_id))

    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: source,
        name: audit_name(row),
        measurements: %{count: 1},
        metadata: metadata
      })
    )

    :ok
  end

  # The roles the act was judged with, read from the context that was judged so
  # the record and the judgment agree.
  defp caller_roles(context) do
    case AuthZ.held_roles(context) do
      {:ok, roles, _namespace} -> roles
      :error -> nil
    end
  end

  defp with_request_id(metadata, nil), do: metadata
  defp with_request_id(metadata, request_id), do: Map.put(metadata, :request_id, request_id)

  # `:"dlq.drop"` audits as `:drop`: the source already names the domain.
  defp audit_name(%{audit_name: name}) when is_atom(name) and not is_nil(name), do: name

  defp audit_name(%{action: action}) do
    action |> Atom.to_string() |> String.split(".") |> List.last() |> String.to_atom()
  end

  defp target(row, args, opts, answer) do
    case Keyword.get(opts, :target) do
      fun when is_function(fun, 1) ->
        fun.(args)

      fun when is_function(fun, 2) ->
        fun.(args, answer)

      nil ->
        row.target
        |> Enum.zip(args)
        |> Map.new(fn {label, value} -> {label, printable(value)} end)
        |> Map.delete(:_)
    end
  end

  defp printable(%_{} = struct), do: inspect(struct)
  defp printable(value), do: value

  # ---------- the bindings ----------

  defmacro __using__(_opts) do
    quote do
      import Flezha.AuthZ.Verb, only: [verb: 2, verb: 3]
      Module.register_attribute(__MODULE__, :verb_bindings, accumulate: true)
      @before_compile Flezha.AuthZ.Verb
    end
  end

  defmacro __before_compile__(env) do
    bindings = env.module |> Module.get_attribute(:verb_bindings) |> Enum.reverse()

    quote do
      @doc false
      def __verbs__, do: unquote(Macro.escape(bindings))
    end
  end

  @doc "Bind a function to a row; see the module documentation."
  defmacro verb(head, opts) when is_list(opts) do
    {block, opts} = Keyword.pop(opts, :do)
    define(__CALLER__, head, opts, block)
  end

  defmacro verb(head, opts, do: block), do: define(__CALLER__, head, opts, block)

  defp define(caller, head, opts, block) do
    {name, args} = head |> unguarded() |> Macro.decompose_call()
    verb_name = Keyword.get(opts, :verb, name)
    row = Verbs.fetch!(verb_name)
    positional = Enum.map(args, &argument_value/1)

    if match?({:row, _}, row.scope) and not Keyword.has_key?(opts, :row) do
      raise CompileError,
        file: caller.file,
        line: caller.line,
        description:
          "#{name}/#{length(args)} binds #{inspect(verb_name)}, whose scope is a row, and supplies no row: fetch"
    end

    body = published_body(caller, name, verb_name, positional, opts, block)

    described =
      quote do
        [
          args: unquote(Keyword.get(opts, :args)),
          stream: unquote(Keyword.get(opts, :stream, false)),
          doc: unquote(__MODULE__).first_sentence(Module.get_attribute(__MODULE__, :doc))
        ]
      end

    quote do
      unquote_splicing(
        for arity <- arities(args) do
          quote do
            @verb_bindings {unquote(name), unquote(arity), unquote(verb_name), unquote(described)}
          end
        end
      )

      def unquote(head) do
        unquote(body)
      end
    end
  end

  # The description a catalogue entry carries: the first sentence of the
  # function's `@doc`, read while it compiles, because a release strips the
  # docs chunks. A full stop after an abbreviation does not end the sentence.
  def first_sentence({_line, doc}), do: first_sentence(doc)

  def first_sentence(doc) when is_binary(doc) do
    doc
    |> String.split(~r/\n\s*\n/, parts: 2)
    |> hd()
    |> String.replace(~r/\s+/, " ")
    |> String.split(~r/(?<=[.!?])(?<!\be\.g\.|\bi\.e\.|\betc\.|\bvs\.)\s/, parts: 2)
    |> hd()
    |> String.trim()
  end

  def first_sentence(_none), do: nil

  # A published function: the last parameter is the caller context, the
  # `do` block is the effect.
  defp published_body(caller, name, verb_name, positional, opts, block) do
    if is_nil(block) do
      raise CompileError,
        file: caller.file,
        line: caller.line,
        description:
          "#{name}/#{length(positional)} binds #{inspect(verb_name)} and needs a do block"
    end

    {caller_context, verb_args} = List.pop_at(positional, -1)
    run_opts = Keyword.take(opts, [:row, :target])

    quote do
      unquote(__MODULE__).run(
        unquote(verb_name),
        unquote(caller_context),
        [unquote_splicing(verb_args)],
        fn unquote(call_pattern(opts)) -> unquote(block) end,
        unquote(run_opts)
      )
    end
  end

  defp call_pattern(opts) do
    call = opts |> Keyword.get(:call) |> as_var()
    row = opts |> Keyword.get(:as) |> as_var()

    case {call, row} do
      {nil, nil} -> quote(do: _call)
      {call, nil} -> call
      {nil, row} -> quote(do: %unquote(Call){row: unquote(row)})
      {call, row} -> quote(do: %unquote(Call){row: unquote(row)} = unquote(call))
    end
  end

  defp as_var(nil), do: nil
  defp as_var({name, _meta, context} = var) when is_atom(name) and is_atom(context), do: var

  defp unguarded({:when, _meta, [call, _guard]}), do: call
  defp unguarded(call), do: call

  # The value a parameter binds, whatever it was written as: `opts \\ []`,
  # `%{} = params`, or a plain variable.
  defp argument_value({:\\, _meta, [arg, _default]}), do: argument_value(arg)
  defp argument_value({:=, _meta, [left, right]}), do: argument_value(variable(left, right))
  defp argument_value(arg), do: arg

  defp variable({name, _meta, context} = var, _other) when is_atom(name) and is_atom(context),
    do: var

  defp variable(_other, var), do: var

  defp arities(args) do
    defaults = Enum.count(args, &match?({:\\, _, _}, &1))
    (length(args) - defaults)..length(args)//1
  end
end
