defmodule Flezha.Service.Admin do
  @moduledoc """
  The one door every remote surface reaches the controller's published
  consumer surface through: the runner session an SDK speaks over, `flz`, and
  HTTP. A request names one operation and its arguments; the surface supplies
  the caller context its own transport proved, and nothing else differs.

  Nothing in a request names a principal or a role. The caller context is
  built by the surface: `Flezha.Runner.FleetManager.Session.runner_caller_context/1`
  from a runner connection, `Flezha.Service.Cli.Context.cli_context/1` from a
  verified token. Every call is then judged at the policy point every other
  caller is judged at, against the row its published function binds, and
  writes the audit record that row declares.

  What a caller may reach follows from the role its principal resolves to.
  This module keeps no list of permitted or forbidden operations: it resolves
  a name through `Flezha.Service.lookup/1` and hands the decision to
  `Flezha.AuthZ`.

  ## Placement

  `{:proven, namespace}` is a runner connection: the namespace slot the
  operation's row declares is filled from the connection, so the caller has no
  slot to write one into. `:given` is a caller whose namespace is an ordinary
  argument, judged by the row's scope. An operation whose row is not `remote`,
  or one that streams, is not reachable by a caller over the network
  (`Flezha.Service.CallerContext.networked?/1`).

  ## The wire form

  `name` is `"<Module>.<function>"` for a module of `Flezha.Service.modules/0`,
  for example `"Operator.start"` or `"PluginManager.enable"`.

  `args` is a JSON array of the arguments before the caller context. Each is
  converted by the argument row its operation declares (`Flezha.Service`); an
  argument left off the end takes its row's default. A word that names no atom
  this node knows, as a value or as the key of named fields, is refused with
  `control.unknown_parameter`, never minted.

  `call/4` answers the published function's value as JSON, and a failure as
  its `%Flezha.Error{}`; `run/4` answers the value itself, for a surface that
  renders terms. A request the door cannot place is refused with a `control.*`
  code.
  """

  alias Flezha.AuthZ
  alias Flezha.AuthZ.Policy
  alias Flezha.Observability.Signal
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias Flezha.Service.CallerContext

  @type placement :: {:proven, String.t()} | :given

  @doc """
  Run the operation `name` for `caller_context`, answering `{:ok, value}` or
  `{:error, %Flezha.Error{}}`. `args` is a JSON array, or the arguments already
  decoded.
  """
  @spec run(String.t(), String.t() | [term()], CallerContext.t(), placement()) ::
          {:ok, term()} | {:error, Flezha.Error.t()}
  def run(name, args, caller_context, placement) when is_binary(name) do
    with {:ok, entry} <- resolve(name, caller_context),
         {:ok, written} <- decode_args(args),
         {:ok, positional} <- positional(entry, written, placement) do
      apply_entry(entry, positional ++ [caller_context])
    end
  end

  @doc "`run/4`, answering the value as the JSON every remote surface carries."
  @spec call(String.t(), String.t() | [term()], CallerContext.t(), placement()) ::
          {:ok, String.t()} | {:error, Flezha.Error.t()}
  def call(name, args, caller_context, placement) do
    with {:ok, value} <- run(name, args, caller_context, placement), do: {:ok, json(value)}
  end

  @doc "A value as the JSON `call/4` answers it with."
  @spec json(term()) :: String.t()
  def json(value), do: value |> Signal.metadata_json() |> Jason.encode!()

  # A published function raises on input it will not take, and `Flezha.AuthZ.Verb`
  # re-raises after writing the act's audit record. Here that failure has to
  # become the requester's answer: the caller is blocked on a reply, so a body
  # that died silently would cost it the whole call deadline and tell it
  # nothing. The record is already written by the time this catches.
  defp apply_entry(entry, args) do
    answer(apply(entry.module, entry.function, args))
  catch
    kind, reason ->
      {:error, Flezha.Error.new("internal.uncatalogued", reason: raised(kind, reason))}
  end

  defp raised(:error, %{__struct__: _} = exception), do: Exception.message(exception)
  defp raised(kind, reason), do: "#{kind}: #{inspect(reason)}"

  defp answer({:error, %Flezha.Error{}} = refused), do: refused
  defp answer({:ok, value}), do: {:ok, value}
  defp answer({:ok, value, more}), do: {:ok, [value, more]}
  defp answer(other), do: {:ok, other}

  @doc """
  The operations `caller_context` reaches through `call/4` and whose action
  its role grants, for a surface that lists them: every one it reaches while
  authorization is open, none for a caller with no role while it is not. The
  namespace is still judged at each call.
  """
  @spec catalogue(CallerContext.t()) :: [Flezha.Service.entry()]
  def catalogue(caller_context),
    do: Enum.filter(reachable(caller_context), &granted?(&1, caller_context))

  @doc "The operations `caller_context` reaches through `call/4`, whatever its role."
  @spec reachable(CallerContext.t()) :: [Flezha.Service.entry()]
  def reachable(caller_context),
    do: Enum.filter(Flezha.Service.catalogue(), &reachable?(&1, caller_context))

  defp granted?(entry, %{role: role}) when role != nil,
    do: entry.action == :public or Policy.grants?(role, entry.action)

  defp granted?(_entry, _caller_context), do: AuthZ.enforcement_posture() == :open

  # ---------- resolving a name ----------

  defp resolve(name, caller_context) do
    case Flezha.Service.lookup(name) do
      {:ok, entry} ->
        if reachable?(entry, caller_context),
          do: {:ok, entry},
          else: refused("control.unknown_function", function: name)

      :error ->
        refused("control.unknown_function", function: name)
    end
  end

  # An operation that is not `remote` stays off the network. Nor does a
  # stream reach it: a networked caller gets one reply per request, and a
  # stream would hold its task, and whatever it attached, for as long as it
  # runs.
  defp reachable?(%{remote: true, stream: stream}, _caller_context) when stream != true,
    do: true

  defp reachable?(_entry, caller_context), do: not CallerContext.networked?(caller_context)

  # ---------- arguments ----------

  defp decode_args(args) when is_list(args), do: {:ok, args}

  defp decode_args(args_json) when is_binary(args_json) do
    case Jason.decode(args_json) do
      {:ok, args} when is_list(args) -> {:ok, args}
      {:ok, _other} -> refused("control.args_not_array", [])
      {:error, reason} -> refused("control.invalid_args_json", reason: Exception.message(reason))
    end
  end

  defp positional(entry, written, placement) do
    written = place_namespace(entry, written, placement)

    with {:ok, filled} <- filled(entry, written),
         {:ok, converted} <- converted(entry, filled),
         do: {:ok, stamp_namespace(entry, converted, placement)}
  end

  # `{:argument, n}` names a position, so the proven namespace is inserted
  # there and the caller has no slot to write one into.
  defp place_namespace(entry, written, {:proven, namespace}) do
    case Flezha.AuthZ.Verbs.fetch!(entry.verb).scope do
      {:argument, n} -> List.insert_at(written, n - 1, namespace)
      _other -> written
    end
  end

  defp place_namespace(_entry, written, :given), do: written

  # An argument left off the end takes its declared default; one the caller
  # had to write, or one too many, is the wrong argument list.
  defp filled(entry, written) do
    missing = Enum.drop(entry.args, length(written))

    if length(written) > length(entry.args) or Enum.any?(missing, &required?/1),
      do: refused("control.wrong_arity", function: entry.name),
      else: {:ok, written ++ Enum.map(missing, fn {_name, row} -> row[:default] end)}
  end

  defp required?({_name, row}), do: row[:required]

  defp converted(entry, values) do
    entry.args
    |> Enum.zip(values)
    |> Enum.reduce_while({:ok, []}, fn {{name, row}, value}, {:ok, done} ->
      case convert(name, row, value, entry) do
        {:ok, converted} -> {:cont, {:ok, [converted | done]}}
        refused -> {:halt, refused}
      end
    end)
    |> case do
      {:ok, done} -> {:ok, Enum.reverse(done)}
      refused -> refused
    end
  end

  defp convert(name, row, value, entry) do
    case {convert_row(name, row, value), row[:type]} do
      {{:ok, converted}, _type} ->
        {:ok, converted}

      {{:error, {:invalid_option_value, _field, word}}, type}
      when type in [:atom, :keyword] and is_binary(word) ->
        refused("control.unknown_parameter", parameter: word)

      {{:error, {:invalid_option_value, _field, word}}, :enum} ->
        refused("usage.unknown_word", noun: name, value: word, allowed: row[:values])

      {{:error, reason}, _type} ->
        {:error, Flezha.Error.from_reason(reason, entry.action)}
    end
  end

  # An optional argument written as nothing takes its default.
  defp convert_row(name, row, nil),
    do:
      if(row[:required],
        do: {:error, {:invalid_option_value, name, nil}},
        else: {:ok, row[:default]}
      )

  defp convert_row(name, row, value), do: Options.convert_row(name, value, [{name, row}])

  # A `{:row, field}` scope reads the namespace out of a fetched row, which
  # for an enable is built from the caller's own named fields: stamping the
  # field there is the proven-namespace rule for a named slot rather than a
  # positional one.
  defp stamp_namespace(entry, args, {:proven, namespace}) do
    case Flezha.AuthZ.Verbs.fetch!(entry.verb).scope do
      {:row, field} -> Enum.map(args, &stamp(&1, field, namespace))
      _other -> args
    end
  end

  defp stamp_namespace(_entry, args, :given), do: args

  defp stamp(arg, field, namespace) when is_list(arg) do
    if Keyword.keyword?(arg), do: Keyword.put(arg, field, namespace), else: arg
  end

  defp stamp(arg, _field, _namespace), do: arg

  defp refused(code, details), do: {:error, Flezha.Error.new(code, details)}
end
