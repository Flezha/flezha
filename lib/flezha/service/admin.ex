defmodule Flezha.Service.Admin do
  @moduledoc """
  The door an application's own code reaches the controller's published
  consumer surface through: one verb-keyed request per act, judged as the
  identity its connection proved.

  Nothing in a request names a namespace, a principal or a role. The caller
  context is built by `Flezha.Runner.FleetManager.Session.runner_caller_context/1`
  from the proven connection, and the namespace slot the verb's own row
  declares is filled from it. An SDK-issued act is therefore judged at the
  policy point every other caller is judged at, against the same row, and
  writes the audit record that row declares.

  What a caller may reach follows from the role its principal resolves to.
  This module keeps no list of permitted or forbidden verbs: it resolves a
  name to a declared binding and hands the decision to `Flezha.AuthZ`.

  ## The wire form

  `verb` is `"<Module>.<function>"` for a `Flezha.Service.*` module that
  declares verb bindings, for example `"Operator.start"` or
  `"PluginManager.enable"`.

  `args` is a JSON array of the call's arguments other than the namespace and
  the caller context. A JSON object in it is the call's named parameters and
  becomes a keyword list whose keys are atoms this node already knows; what is
  nested inside one is data and stays as `Jason` decodes it.

  The answer is the published function's own, rendered as JSON:
  `{:ok, value}` answers `value`, an error answers its reason as text.
  """

  alias Flezha.AuthZ.Verb
  alias Flezha.AuthZ.Verbs
  alias Flezha.Observability.Signal
  alias Flezha.Runner.FleetManager.Session

  @surface Flezha.Service

  @doc """
  Run `verb` for the namespace this connection proved, answering
  `{:ok, json}` or `{:error, text}`.
  """
  @spec call(binary(), binary(), binary()) :: {:ok, binary()} | {:error, binary()}
  def call(namespace, verb, args_json)
      when is_binary(namespace) and is_binary(verb) and is_binary(args_json) do
    with {:ok, binding} <- resolve(verb),
         {:ok, args} <- decode_args(args_json),
         positional = place_namespace(binding.row, args, namespace),
         :ok <- accepts?(binding, length(positional) + 1, verb) do
      run(binding, positional ++ [Session.runner_caller_context(namespace)])
    end
  end

  # A published function raises on input it will not take, and `Flezha.AuthZ.Verb`
  # re-raises after writing the act's audit record. Here that failure has to
  # become the requester's answer: the caller is blocked on a reply, so a body
  # that died silently would cost it the whole call deadline and tell it
  # nothing. The record is already written by the time this catches.
  defp run(binding, args) do
    answer(apply(binding.module, binding.function, args))
  catch
    kind, reason -> {:error, refusal_text(kind, reason)}
  end

  defp refusal_text(:error, %{__struct__: _} = exception), do: Exception.message(exception)
  defp refusal_text(kind, reason), do: "#{kind}: #{inspect(reason)}"

  # ---------- resolving a name to a declared binding ----------

  # One walk of the module's declared bindings answers both questions a name
  # asks: which row judges it, and which argument counts it takes.
  defp resolve(verb) do
    with {:ok, module_name, function_name} <- split(verb),
         {:ok, module} <- surface_module(module_name),
         {:ok, function} <- existing_atom(function_name),
         {:ok, row_name, arities} <- bound(module, function) do
      {:ok, %{module: module, function: function, row: Verbs.fetch!(row_name), arities: arities}}
    else
      :error -> {:error, "no published function is named `#{verb}`"}
    end
  end

  defp split(verb) do
    case String.split(verb, ".", parts: 2) do
      [module_name, function_name] when module_name != "" and function_name != "" ->
        {:ok, module_name, function_name}

      _ ->
        :error
    end
  end

  # Only the published surface is addressable, and only a module of it that is
  # loaded and declares bindings. `Module.concat/1` would mint an atom for any
  # name a caller invented; `to_existing_atom` cannot.
  #
  # A wire contract lives under this prefix too and must never be entered from
  # here: its bodies render a terminal and end the process on a refusal. It is
  # told apart by what it declared, not by name: a contract binds `identity:`
  # because it mints its own caller context, and a published surface binds none
  # because it is handed one.
  defp surface_module(module_name) do
    with {:ok, module} <- existing_atom(Atom.to_string(@surface) <> "." <> module_name),
         true <- Code.ensure_loaded?(module) and function_exported?(module, :__verbs__, 0),
         false <- Keyword.has_key?(module.__verb_surface__(), :identity) do
      {:ok, module}
    else
      _ -> :error
    end
  end

  defp bound(module, function) do
    module
    |> Verb.bindings()
    |> Enum.filter(&match?({^function, _arity, _row, _opts}, &1))
    |> case do
      [] -> :error
      [{_name, _arity, row_name, _opts} | _] = bindings -> {:ok, row_name, arities(bindings)}
    end
  end

  defp arities(bindings), do: MapSet.new(bindings, fn {_name, arity, _row, _opts} -> arity end)

  defp accepts?(binding, arity, verb) do
    if MapSet.member?(binding.arities, arity),
      do: :ok,
      else: {:error, "`#{verb}` takes no argument list of this length"}
  end

  defp existing_atom(text) do
    {:ok, String.to_existing_atom(text)}
  rescue
    ArgumentError -> :error
  end

  # ---------- the namespace slot the row declares ----------

  # `{:argument, n}` names a position, so the proven namespace is inserted
  # there and the caller has no slot to write one into. A `{:row, field}` scope
  # reads the namespace out of a fetched row, which for an enable is built from
  # the caller's own named parameters: stamping the field there is the same
  # rule expressed for a named slot rather than a positional one. `:caller` and
  # `:system` read the context, which the caller never supplies.
  defp place_namespace(%{scope: {:argument, n}}, args, namespace),
    do: List.insert_at(args, n - 1, namespace)

  defp place_namespace(%{scope: {:row, field}}, args, namespace),
    do: Enum.map(args, &stamp(&1, field, namespace))

  defp place_namespace(_row, args, _namespace), do: args

  defp stamp(arg, field, namespace) when is_list(arg) do
    if Keyword.keyword?(arg), do: Keyword.put(arg, field, namespace), else: arg
  end

  defp stamp(arg, _field, _namespace), do: arg

  # ---------- arguments ----------

  defp decode_args(args_json) do
    case Jason.decode(args_json) do
      {:ok, args} when is_list(args) -> parameters(args, [])
      {:ok, _other} -> {:error, "arguments must be a JSON array"}
      {:error, reason} -> {:error, "invalid argument JSON: " <> Exception.message(reason)}
    end
  end

  defp parameters([], done), do: {:ok, Enum.reverse(done)}

  defp parameters([%{} = object | rest], done) do
    case named(Map.to_list(object), []) do
      {:ok, keyword} -> parameters(rest, [keyword | done])
      {:error, _reason} = refusal -> refusal
    end
  end

  defp parameters([value | rest], done), do: parameters(rest, [value | done])

  defp named([], done), do: {:ok, Enum.reverse(done)}

  defp named([{key, value} | rest], done) do
    case existing_atom(key) do
      {:ok, atom} -> named(rest, [{atom, value} | done])
      :error -> {:error, "unknown parameter `#{key}`"}
    end
  end

  # ---------- the answer ----------

  defp answer({:error, {:forbidden, reason}}),
    do: {:error, "forbidden: #{reason}"}

  defp answer({:error, reason}), do: {:error, text(reason)}
  defp answer({:ok, value}), do: {:ok, json(value)}
  defp answer(other), do: {:ok, json(other)}

  defp json(value), do: value |> Signal.metadata_json() |> Jason.encode!()

  defp text(reason) when is_binary(reason), do: reason
  defp text(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp text(reason), do: inspect(reason)
end
