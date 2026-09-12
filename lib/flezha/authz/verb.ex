defmodule Flezha.AuthZ.Verb do
  @moduledoc """
  The one seam that applies a row of `Flezha.AuthZ.Verbs`, in one order for
  every surface: identity, scope, policy, act, audit.

  `run/5` is the seam. A surface binds its functions to rows with `verb`:

      use Flezha.AuthZ.Verb

      verb drop(entry_ref, caller_context), verb: :dlq_drop, row: &locate/1 do
        Store.drop(entry_ref)
      end

  defines `drop/2`, whose body runs only once the row has been applied to the
  caller context in its last argument. A wire contract binds the other way,
  naming the module that carries each body and taking its identity from the
  trailing options list every wire verb ends with:

      use Flezha.AuthZ.Verb, identity: &Context.cli_context/1, refuse: &Context.refuse!/2, sealed: true

      verb dlq_drop(entry_ref, opts \\\\ []), to: Cli.Dlq, row: &Cli.Dlq.locate/1

  which defines `dlq_drop/1,2` calling `Cli.Dlq.dlq_drop(entry_ref, call)` with
  the bound `t:Call.t/0`, and renders a refusal through `refuse`. A `sealed`
  surface refuses to compile a public function that binds no row.

  The two forms differ in one declared way that outlives compilation: a wire
  contract binds `identity:`, because it mints a caller context from what its
  own transport carried, and a published surface binds none, because it is
  handed one. `__verb_surface__/0` answers what the module declared, so a
  caller that may only enter a surface taking a context can tell them apart
  without a list of module names.

  A `{:row, field}` scope needs a `row:` fetch, called with the positional
  arguments as a list and answering `{:ok, row}` or `{:error, reason}`; the
  fetch runs only for a bound caller whose role grants the action, so a refusal
  says nothing about whether the row exists. `as:` names the fetched row inside
  a body, `call:` names the whole call. `target:` overrides the row's audit
  labels with a function of the argument list.
  """

  alias Flezha.AuthZ
  alias Flezha.AuthZ.{Namespace, Scope, Verbs}
  alias Flezha.Observability.{Bus, Signal}
  alias Flezha.Service.CallerContext

  defmodule Call do
    @moduledoc """
    What a bound body receives: the caller's context, the namespace the verb
    was judged in, the row a `{:row, _}` scope fetched, and the trailing
    options the wire carried (without the credential).
    """
    defstruct [:context, :namespace, :row, opts: []]

    @type t :: %__MODULE__{
            context: Flezha.Service.CallerContext.t(),
            namespace: String.t() | nil,
            row: map() | nil,
            opts: keyword()
          }
  end

  @type effect :: (Call.t() -> term())
  @type fetch :: ([term()] -> {:ok, map()} | {:error, term()})

  # ---------- the seam ----------

  @doc """
  Apply the row `verb` to `caller_context` for the positional `args`, then run
  `effect`. Options: `:row` (the fetch a `{:row, _}` scope reads), `:target`
  (a function of `args` answering the audit's target fields), `:wire_opts`
  (the wire's trailing options, handed to the body on the call).

  Answers `{:error, {:forbidden, reason}}` on a refusal and never exits; a
  `{:row, _}` fetch's own `{:error, reason}` is answered as is.
  """
  @spec run(atom(), CallerContext.t(), [term()], effect(), keyword()) :: term()
  def run(verb, caller_context, args, effect, opts \\ []) when is_function(effect, 1) do
    apply_row(Verbs.fetch!(verb), caller_context, args, effect, opts)
  end

  @doc """
  The wire surface's `run/5`: bind the identity from the trailing options,
  apply the row, and render a refusal. `binding` carries `:identity`,
  `:refuse` and optionally `:row`, `:target`.
  """
  @spec invoke(atom(), [term()], keyword(), effect(), keyword()) :: term()
  def invoke(verb, args, wire_opts, effect, binding) do
    caller_context = Keyword.fetch!(binding, :identity).(wire_opts)
    run_opts = [row: binding[:row], target: binding[:target], wire_opts: wire_opts]

    case run(verb, caller_context, args, effect, run_opts) do
      {:error, {:forbidden, reason}} ->
        Keyword.fetch!(binding, :refuse).(Verbs.fetch!(verb).action, reason)

      other ->
        other
    end
  end

  @doc """
  The bindings a surface module declared: `{function, arity, verb, opts}`.
  """
  @spec bindings(module()) :: [{atom(), arity(), atom(), keyword()}]
  def bindings(module), do: module.__verbs__()

  defp apply_row(%{action: :none}, caller_context, _args, effect, opts) do
    effect.(call(caller_context, nil, nil, opts))
  end

  defp apply_row(%{action: :public}, caller_context, _args, effect, opts) do
    with :ok <- CallerContext.validate(caller_context) do
      effect.(call(caller_context, nil, nil, opts))
    end
  end

  defp apply_row(row, caller_context, args, effect, opts) do
    with :ok <- CallerContext.validate(caller_context),
         {:ok, namespace, fetched} <- resolve(row, caller_context, args, opts[:row]),
         :ok <- AuthZ.authorize(caller_context, row.action, Scope.build(namespace)) do
      audited(row, call(caller_context, namespace, fetched, opts), args, opts, effect)
    end
  end

  defp call(caller_context, namespace, fetched, opts) do
    %Call{
      context: caller_context,
      namespace: namespace,
      row: fetched,
      opts: Keyword.delete(Keyword.get(opts, :wire_opts, []), :token)
    }
  end

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
    emit(row, source, call, target(row, args, opts), %{})
    effect.(call)
  end

  # An authorized act that failed is still an act: the outcome rides the
  # record, and an exit or raise is re-raised once it is written.
  defp audited(%{audit: {:after, source}} = row, call, args, opts, effect) do
    {outcome, result} = observe(effect, call)
    emit(row, source, call, target(row, args, opts), %{outcome: outcome})
    settle(result)
  end

  defp observe(effect, call) do
    result = effect.(call)
    {outcome_of(result), {:value, result}}
  catch
    kind, reason -> {:error, {:caught, kind, reason, __STACKTRACE__}}
  end

  defp outcome_of({:error, _reason}), do: :error
  defp outcome_of(:error), do: :error
  defp outcome_of(_result), do: :ok

  defp settle({:value, result}), do: result
  defp settle({:caught, kind, reason, stacktrace}), do: :erlang.raise(kind, reason, stacktrace)

  # One record shape for "an operator did X": the action, where it was judged,
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

  defp target(row, args, opts) do
    case Keyword.get(opts, :target) do
      fun when is_function(fun, 1) ->
        fun.(args)

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

  defmacro __using__(surface) do
    quote do
      import Flezha.AuthZ.Verb, only: [verb: 2, verb: 3]
      Module.register_attribute(__MODULE__, :verb_bindings, accumulate: true)
      @verb_surface unquote(surface)
      @before_compile Flezha.AuthZ.Verb
    end
  end

  defmacro __before_compile__(env) do
    bindings = env.module |> Module.get_attribute(:verb_bindings) |> Enum.reverse()

    if Module.get_attribute(env.module, :verb_surface)[:sealed] do
      bound = MapSet.new(bindings, fn {name, arity, _verb, _opts} -> {name, arity} end)

      for {name, arity} <- Module.definitions_in(env.module, :def),
          not MapSet.member?(bound, {name, arity}) do
        raise CompileError,
          file: env.file,
          line: env.line,
          description:
            "#{inspect(env.module)}.#{name}/#{arity} is a public function on a sealed " <>
              "wire contract and binds no row of Flezha.AuthZ.Verbs; declare it with `verb`"
      end
    end

    surface = Module.get_attribute(env.module, :verb_surface)

    quote do
      @doc false
      def __verbs__, do: unquote(Macro.escape(bindings))

      @doc false
      def __verb_surface__, do: unquote(Macro.escape(surface))
    end
  end

  @doc "Bind a function to a row; see the module documentation for the two forms."
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

    body =
      case Keyword.fetch(opts, :to) do
        {:ok, target} -> wire_body(caller, name, verb_name, row, positional, args, target, opts)
        :error -> published_body(caller, name, verb_name, row, positional, opts, block)
      end

    records =
      for arity <- arities(args), do: {name, arity, verb_name, []}

    quote do
      unquote_splicing(
        for record <- records, do: quote(do: @verb_bindings(unquote(Macro.escape(record))))
      )

      def unquote(head) do
        unquote(body)
      end
    end
  end

  # A wire verb: the last parameter is its options list, whatever else it
  # takes, and the body lives in the `to:` module under the same name with the
  # bound call appended. A `:none` row is a plain delegation.
  defp wire_body(_caller, name, _verb_name, %{action: :none}, positional, _args, target, _opts) do
    quote do: unquote(target).unquote(name)(unquote_splicing(positional))
  end

  defp wire_body(caller, name, verb_name, _row, positional, args, target, opts) do
    unless List.last(args) |> then(&match?({:\\, _, [_, []]}, &1)) do
      raise CompileError,
        file: caller.file,
        line: caller.line,
        description:
          "wire verb #{name}/#{length(args)} must end with its options list (`opts \\\\ []`)"
    end

    {wire_opts, verb_args} = List.pop_at(positional, -1)
    binding = Keyword.take(opts, [:row, :target])

    quote do
      unquote(__MODULE__).invoke(
        unquote(verb_name),
        [unquote_splicing(verb_args)],
        unquote(wire_opts),
        fn call -> unquote(target).unquote(name)(unquote_splicing(verb_args), call) end,
        Keyword.merge(@verb_surface, unquote(binding))
      )
    end
  end

  # A published function: the last parameter is the caller context, the
  # `do` block is the effect.
  defp published_body(caller, name, verb_name, row, positional, opts, block) do
    if row.action == :none or is_nil(block) do
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
