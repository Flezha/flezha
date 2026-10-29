defmodule Flezha.Service.Logs do
  @moduledoc """
  Service-API surface for every durable log the controller keeps, read across
  the whole cluster. Each function names its `kind` (`kinds/1` lists them,
  with the filters and columns a reader shows them by), takes a
  `Flezha.Service.CallerContext` as its last argument and binds a row of
  `Flezha.AuthZ.Verbs`.

  Reads ask every controller through `Flezha.Cluster.Reads` and name, in
  `unreachable`, each one that did not answer within the read bound; `read/2`
  pages newest first with the shape of `Flezha.Cluster.Page`. Only records of
  the caller's namespace are answered, except to `admin@_system`.

  A call on the `audit` kind is itself audited, under the audit log's own rows,
  before the work; a call on any other kind writes no audit record. A kind
  that is off answers `logs.kind_disabled` (the audit kind its own
  `audit.disabled`, or `observability.disabled` when the bus is off).
  """

  use Flezha.AuthZ.Verb

  alias Flezha.AuthZ.{Namespace, Verb}
  alias Flezha.Cluster.{Page, Reads}
  alias Flezha.Logs.{Codec, Kind, Kinds, Local, Reader, Writer}
  alias Flezha.Service.CallerContext

  @type caller_context :: CallerContext.t()

  @read_doc "`kind`, `filters`, `since_us`, `until_us`, `limit`, `before`, `node` and `file`."

  @doc """
  Every log kind, with the filters a read takes and the columns a reader shows.
  Each names its id, title and description, whether it runs and why not, its
  filters (`field`, `label`, `type`, the `values` of an enum) and its columns.
  """
  @spec kinds(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb kinds(caller_context), verb: :logs_kinds do
    {:ok, Enum.map(Kinds.all(), &describe/1)}
  end

  @doc """
  A kind's records from every controller, newest first, each naming its `node`.
  At most `limit` (default 100, at most 1000), older than the `before` cursor a
  previous read answered as `next`. With `node` and `file`, the records of that
  one file on that controller, oldest first.
  """
  @spec read(map() | keyword(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb read(params, caller_context),
    verb: :logs_read,
    args: [params: [type: :keyword, doc: @read_doc]] do
    params = Map.new(params)

    on_kind(params, :audit_read, caller_context, fn kind ->
      with {:ok, query} <- query(kind, params, caller_context) do
        if params[:file],
          do: read_file(kind, params, query),
          else: read_pages(kind, params, query)
      end
    end)
  end

  @doc """
  Stream a kind's new records from every controller as they are written.
  Each record of the caller's namespace, naming its `node`, goes to the
  context's `:emit` (a function, or a pid that receives
  `{:flezha_emit, record}`) until the watched process exits, `:emit` answers
  `:done` or the context's `window_ms` runs out:
  `{:ok, :caller_down | :done | :window_closed}`. `filters` narrows it as it
  narrows a read.
  """
  @spec tail(map() | keyword(), caller_context()) :: {:ok, atom()} | {:error, term()}
  verb tail(params, caller_context),
    verb: :logs_tail,
    stream: true,
    args: [params: [type: :keyword, doc: "`kind` and `filters`."]] do
    params = Map.new(params)

    on_kind(params, :audit_tail, caller_context, fn kind ->
      with {:ok, query} <- query(kind, params, caller_context),
           do: forward(kind, query, caller_context)
    end)
  end

  @doc "A kind's rotated files on every controller, each with its record times and digest."
  @spec files(map() | keyword(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb files(params, caller_context),
    verb: :logs_files,
    args: [params: [type: :keyword, doc: "`kind`."]] do
    on_kind(Map.new(params), :audit_list, caller_context, fn kind ->
      %{rows: files, unreachable: unreachable} =
        Reads.rows(Reads.controllers(), Local, :files, [kind.id])

      {:ok, %{files: files, unreachable: unreachable}}
    end)
  end

  @doc "A kind's operational snapshot, one row per controller."
  @spec stats(map() | keyword(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb stats(params, caller_context),
    verb: :logs_stats,
    args: [params: [type: :keyword, doc: "`kind`."]] do
    on_kind(Map.new(params), :audit_stats, caller_context, fn kind ->
      %{rows: rows, unreachable: unreachable} =
        Reads.rows(Reads.controllers(), Local, :stats, [kind.id])

      {:ok, %{controllers: rows, unreachable: unreachable}}
    end)
  end

  @doc "Close a kind's current file and start a new one, on `node` or on every controller."
  @spec rotate(map() | keyword(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb rotate(params, caller_context),
    verb: :logs_rotate,
    args: [params: [type: :keyword, doc: "`kind` and `node`."]] do
    params = Map.new(params)

    on_kind(params, :audit_rotate, caller_context, fn kind ->
      with {:ok, nodes} <- nodes(params[:node]) do
        %{rows: rotated, unreachable: unreachable} = Reads.rows(nodes, Local, :rotate, [kind.id])
        {:ok, %{rotated: rotated, unreachable: unreachable}}
      end
    end)
  end

  # The kind the call names, then, for the audit kind, the audit log's own row
  # around the work, so an audit read is audited as it always was.
  defp on_kind(params, audit_verb, caller_context, work) do
    with {:ok, kind} <- kind(params[:kind]) do
      checked = fn _call -> with :ok <- Kinds.check(kind), do: work.(kind) end

      if kind.id == :audit,
        do: Verb.run(audit_verb, caller_context, [params], checked, target: &audit_target/1),
        else: checked.(nil)
    end
  end

  defp audit_target([params | _]), do: Map.take(params, [:file])

  defp kind(id) do
    case Kinds.fetch(to_string(id)) do
      {:ok, kind} -> {:ok, kind}
      :error -> {:error, {:logs_unknown_kind, inspect(id), known()}}
    end
  end

  defp known(), do: Enum.map_join(Kinds.all(), ", ", &Atom.to_string(&1.id))

  defp describe(kind) do
    disabled =
      case Kinds.check(kind) do
        :ok -> nil
        {:error, reason} -> Flezha.Error.from_reason(reason, :"logs.read").message
      end

    %{
      id: Atom.to_string(kind.id),
      title: kind.title,
      description: kind.description,
      enabled: disabled == nil,
      disabled_reason: disabled,
      filters: kind.filters,
      columns: kind.columns
    }
  end

  defp query(kind, params, caller_context) do
    filters = Map.new(params[:filters] || %{}, fn {field, value} -> {to_string(field), value} end)

    case Enum.find(Map.keys(filters), &(&1 not in Kind.filter_fields(kind))) do
      nil ->
        {:ok,
         %{
           filters: filters,
           since_us: params[:since_us],
           until_us: params[:until_us],
           namespace: Namespace.read_scope(caller_context)
         }}

      field ->
        {:error, {:logs_unknown_filter, field, Enum.join(Kind.filter_fields(kind), ", ")}}
    end
  end

  defp read_pages(kind, params, query) do
    paging = %{limit: Map.get(params, :limit, 100), before: params[:before]}

    with {:ok, page} <-
           Page.parse(paging, :logs_read,
             sorts: [newest: :desc],
             keys: [:limit, :before],
             cursor: :before,
             limit: {100, 1_000}
           ) do
      %{rows: rows, next: next, unreachable: unreachable} =
        Reads.controllers()
        |> Reads.rows(Local, :read, [kind.id, query, page])
        |> Page.merge(page, &Local.key/1)

      {:ok, %{records: Enum.map(rows, &elem(&1, 1)), next: next, unreachable: unreachable}}
    end
  end

  defp read_file(kind, params, query) do
    with {:ok, [node]} <- nodes(params[:node] || node()) do
      case Reads.ask([node], Local, :read_file, [
             kind.id,
             params.file,
             query,
             Map.get(params, :limit, 1_000)
           ]) do
        %{answers: [{_node, {:error, _} = missing}]} -> missing
        %{answers: [{_node, records}]} -> {:ok, %{records: records, next: nil, unreachable: []}}
        %{unreachable: unreachable} -> {:ok, %{records: [], next: nil, unreachable: unreachable}}
      end
    end
  end

  # Only a controller the cluster directory lists is asked.
  defp nodes(nil), do: {:ok, Reads.controllers()}

  defp nodes(name) do
    case Enum.find(Reads.controllers(), &(to_string(&1) == to_string(name))) do
      nil -> {:error, {:unknown_node, to_string(name)}}
      node -> {:ok, [node]}
    end
  end

  # The records every controller's writer broadcasts, narrowed as a read is.
  defp forward(kind, query, caller_context) do
    {emit, watch} = CallerContext.emitter(caller_context)
    match = Reader.matcher(scoped(query), nil, nil)
    :ok = Phoenix.PubSub.subscribe(Flezha.Observability.PubSub, Writer.topic(kind))

    try do
      outcome =
        CallerContext.receive_for(
          Map.get(caller_context, :window_ms, :infinity),
          fn
            {:log_lines, _kind, lines} ->
              lines |> decoded() |> Enum.filter(match) |> CallerContext.hand_on(emit)

            _other ->
              :continue
          end,
          watch
        )

      {:ok, outcome}
    after
      Phoenix.PubSub.unsubscribe(Flezha.Observability.PubSub, Writer.topic(kind))
    end
  end

  defp scoped(%{namespace: :all, filters: filters}), do: filters

  defp scoped(%{namespace: namespace, filters: filters}),
    do: Map.put(filters, "namespace", namespace)

  defp decoded(lines) do
    for line <- lines, {:ok, record} <- [Codec.decode(line)], do: record
  end
end
