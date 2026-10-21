defmodule Flezha.Plugin.Provider.PostgresPollTest do
  @moduledoc """
  The Postgres poll source with no database: its declaration, the refusals at
  enable, the queries it builds from a catalog answer, the row encoding and the
  password handling it shares with the sink. The reads themselves are
  `postgres_poll_integration_test.exs`.
  """

  use ExUnit.Case, async: true

  alias Flezha.Plugin.Provider.Postgres.Connection
  alias Flezha.Plugin.Provider.PostgresPoll
  alias Flezha.Plugin.Provider.PostgresPoll.Query
  alias Flezha.Plugin.Provider.PostgresPoll.Reader
  alias Flezha.Plugin.Provider.WorkerSup
  alias FlezhaTest.Helper.ProviderPrepare

  @secret "pw-#{System.unique_integer([:positive])}"

  defp opts(extra \\ %{}) do
    Map.merge(
      %{
        database: "shop",
        username: "reader",
        password: @secret,
        channel: "rows",
        table: "orders",
        id_column: "id",
        plugin_id: "poll-#{System.unique_integer([:positive])}"
      },
      extra
    )
  end

  defp code(reason), do: Flezha.Error.from_reason(reason, nil).code

  describe "the declaration" do
    test "a feeding source that is not shared and declares caught_up as its own event" do
      decl = PostgresPoll.__provider__()

      assert decl.feeds
      refute decl.shareable
      assert [caught_up: {:event, words}] = decl.telemetry
      assert words[:sentence] == "{plugin_id} caught up with {table}"
      assert Flezha.Plugin.Providers.fetch("relay_postgres_poll") == {:ok, PostgresPoll}
    end
  end

  describe "refusals at enable" do
    test "a table that is not an identifier is refused naming it, and nothing is started" do
      hostile = "orders; drop table x"

      assert {:error, {:invalid_option_value, :table, ^hostile} = reason} =
               ProviderPrepare.prepare_and_activate(PostgresPoll, opts(%{table: hostile}))

      assert code(reason) == "plugin.option_invalid"
      assert WorkerSup.keys(WorkerSup.family(PostgresPoll)) == []
    end

    test "every name option is checked, and a schema prefix is the one dot allowed" do
      refusals = [
        {%{table: "a.b.c"}, :table},
        {%{table: "1orders"}, :table},
        {%{id_column: "id\""}, :id_column},
        {%{timestamp_column: "updated at"}, :timestamp_column},
        {%{columns: "id,total;"}, :columns},
        {%{table: String.duplicate("t", 64)}, :table}
      ]

      for {extra, field} <- refusals do
        assert {:error, {:invalid_option_value, ^field, _}} =
                 ProviderPrepare.prepare(PostgresPoll, opts(extra)),
               "#{inspect(extra)} was not refused"
      end

      assert {:ok, _} =
               ProviderPrepare.prepare(
                 PostgresPoll,
                 opts(%{table: "Sales.Order_$1", columns: "id,Total", timestamp_column: "at"})
               )
    end

    test "the starting point and the batch are refused by their rows" do
      assert {:error, {:invalid_option_value, :start_at, "now"}} =
               ProviderPrepare.prepare(PostgresPoll, opts(%{start_at: "now"}))

      assert {:error, {:option_below_minimum, :batch_size, 0, 1}} =
               ProviderPrepare.prepare(PostgresPoll, opts(%{batch_size: 0}))
    end
  end

  describe "the cursor" do
    test "a cursor read under the same table and columns is kept, another one is dropped" do
      cursor = %{key: ["orders", "id", nil], at: ["41"]}

      assert {:ok, %{cursor: ^cursor}} =
               ProviderPrepare.prepare(PostgresPoll, opts(%{cursor: cursor}))

      for extra <- [%{table: "orders_v2"}, %{id_column: "no"}, %{timestamp_column: "at"}] do
        assert {:ok, %{cursor: nil}} =
                 ProviderPrepare.prepare(PostgresPoll, opts(Map.put(extra, :cursor, cursor)))
      end
    end
  end

  describe "the password" do
    test "the prepared password is the placeholder, and the connection authenticates with the literal one" do
      assert {:ok, prepared} = ProviderPrepare.prepare(PostgresPoll, opts())
      assert prepared.password == "**redacted**"

      [{_key, worker}] = PostgresPoll.worker_specs(prepared)
      for printed <- [inspect(prepared), inspect(worker)], do: refute(printed =~ @secret)
      refute Map.has_key?(Flezha.Plugin.strip_transient(plugin(prepared)).opts, :password_secret)

      configure = Keyword.fetch!(worker.conn_opts, :configure)
      assert configure.([]) == [password: @secret]

      # Prepared again from what it holds, as a plugin restart does.
      assert {:ok, again} = ProviderPrepare.prepare(PostgresPoll, prepared)
      assert Keyword.fetch!(Reader.args(again).conn_opts, :configure).([]) == [password: @secret]
    end

    test "password_env is read at connect, and nothing connects with the placeholder" do
      env = "FLZ_POLL_TEST_PW_#{System.unique_integer([:positive])}"
      System.put_env(env, "from-env")
      on_exit(fn -> System.delete_env(env) end)

      prepared = Connection.prepare(%{password: nil, password_env: env})
      assert Connection.prepare(%{password: "**redacted**"}) == %{password: "**redacted**"}

      conn = Connection.conn_opts(Map.merge(prepared, endpoint()))
      assert Keyword.fetch!(conn, :configure).([]) == [password: "from-env"]
    end
  end

  describe "the queries" do
    # What the catalog read answers: schema, table, column, type, typtype,
    # typname, whether an array, and the array's element type.
    @catalog [
      ["sales", "Orders", "id", "bigint", "b", "int8", false, nil],
      ["sales", "Orders", "Total", "numeric", "b", "numeric", false, nil],
      [
        "sales",
        "Orders",
        "updated_at",
        "timestamp with time zone",
        "b",
        "timestamptz",
        false,
        nil
      ]
    ]

    test "incrementing: names quoted, the cursor a parameter cast from its text" do
      {:ok, plan} = Query.plan(spec(%{table: "sales.Orders"}), @catalog)

      assert plan.relation == "sales.Orders"
      assert plan.columns == [{"id", :value}, {"Total", :text}, {"updated_at", :temporal}]

      assert plan.first ==
               ~s|SELECT "id", "Total"::text, "updated_at", r."id"::text FROM "sales"."Orders" r | <>
                 ~s|WHERE r."id" IS NOT NULL ORDER BY r."id" LIMIT $1|

      assert plan.next ==
               ~s|SELECT "id", "Total"::text, "updated_at", r."id"::text FROM "sales"."Orders" r | <>
                 ~s|WHERE (r."id") > (($1::text)::bigint) ORDER BY r."id" LIMIT $2|

      assert plan.last ==
               ~s|SELECT r."id"::text FROM "sales"."Orders" r WHERE r."id" IS NOT NULL | <>
                 ~s|ORDER BY r."id" DESC LIMIT 1|
    end

    test "timestamp and id: the pair compared as a row, the delay an integer parameter" do
      spec = spec(%{table: "sales.Orders", timestamp_column: "updated_at", columns: ["Total"]})
      {:ok, plan} = Query.plan(spec, @catalog)

      cursor = ~s|r."updated_at"::text, r."id"::text|
      delay = ~s|r."updated_at" < now() - $|

      assert plan.first ==
               ~s|SELECT "Total"::text, #{cursor} FROM "sales"."Orders" r | <>
                 ~s|WHERE r."updated_at" IS NOT NULL AND r."id" IS NOT NULL AND | <>
                 ~s|#{delay}1::bigint * interval '1 millisecond' | <>
                 ~s|ORDER BY r."updated_at", r."id" LIMIT $2|

      assert plan.next ==
               ~s|SELECT "Total"::text, #{cursor} FROM "sales"."Orders" r | <>
                 ~s|WHERE (r."updated_at", r."id") > | <>
                 ~s|(($1::text)::timestamp with time zone, ($2::text)::bigint) AND | <>
                 ~s|#{delay}3::bigint * interval '1 millisecond' | <>
                 ~s|ORDER BY r."updated_at", r."id" LIMIT $4|
    end

    test "a name the catalog answers is quoted whole: a quote doubled, a dot kept" do
      evil = ~s|x", pg_read_file('/etc/passwd') AS "y|

      rows = [
        ["sales", "or.ders", "id", "bigint", "b", "int8", false, nil],
        ["sales", "or.ders", evil, "uuid", "b", "uuid", false, nil]
      ]

      {:ok, plan} = Query.plan(spec(%{table: "sales.orders"}), rows)

      assert plan.first ==
               ~s|SELECT "id", "x"", pg_read_file('/etc/passwd') AS ""y"::text, r."id"::text | <>
                 ~s|FROM "sales"."or.ders" r WHERE r."id" IS NOT NULL ORDER BY r."id" LIMIT $1|

      assert Connection.relation(nil, ~s|a"b|) == ~s|"a""b"|
      assert Connection.split_table("sales.orders") == {"sales", "orders"}
      assert Connection.split_table("orders") == {nil, "orders"}
    end

    test "a missing table and a missing column are named" do
      assert Query.plan(spec(%{table: "nope"}), []) == {:error, {:table_missing, "nope"}}

      assert Query.plan(spec(%{timestamp_column: "changed"}), @catalog) ==
               {:error, {:column_missing, "changed"}}

      assert Query.plan(spec(%{columns: ["id", "total"]}), @catalog) ==
               {:error, {:column_missing, "total"}}
    end

    test "a type the encoding does not map is read as its text, an array of one as text[]" do
      catalog = [
        ["public", "t", "id", "integer", "b", "int4", false, nil],
        ["public", "t", "addr", "inet", "b", "inet", false, nil],
        ["public", "t", "mood", "mood", "e", "mood", false, nil],
        ["public", "t", "tags", "text[]", "b", "_text", true, "text"],
        ["public", "t", "nets", "inet[]", "b", "_inet", true, "inet"],
        ["public", "t", "moods", "mood[]", "b", "_mood", true, nil]
      ]

      {:ok, plan} = Query.plan(spec(%{table: "t"}), catalog)

      assert plan.columns == [
               {"id", :value},
               {"addr", :text},
               {"mood", :text},
               {"tags", {:array, :value}},
               {"nets", {:array, :text}},
               {"moods", {:array, :text}}
             ]

      assert plan.first =~
               ~s|SELECT "id", "addr"::text, "mood"::text, "tags", | <>
                 ~s|"nets"::text[], "moods"::text[], r."id"::text FROM|
    end
  end

  describe "the row encoding" do
    test "every type of the encoding becomes its JSON value, in column order" do
      columns = [
        {"i", :value},
        {"f", :value},
        {"nan", :value},
        {"inf", :value},
        {"b", :value},
        {"n", :text},
        {"at", :temporal},
        {"day", :temporal},
        {"t", :temporal},
        {"local", :temporal},
        {"u", :text},
        {"j", :value},
        {"bin", :bytea},
        {"none", :value},
        {"arr", {:array, :value}},
        {"grid", {:array, :temporal}},
        {"bins", {:array, :bytea}}
      ]

      values = [
        42,
        1.5,
        :NaN,
        :"-inf",
        true,
        "12.3400",
        ~U[2026-10-02 12:30:00.123456Z],
        ~D[2026-10-02],
        ~T[12:30:00.000000],
        ~N[2026-10-02 12:30:00],
        "6f1c3a52-6f0a-4f43-9a41-0d8f8c2e7b11",
        %{"a" => [1, 2]},
        <<1, 2, 255>>,
        nil,
        [1, nil, 3],
        [[~D[2026-01-01]], [nil]],
        [<<0>>]
      ]

      assert Connection.encode(columns, values) ==
               ~s|{"i":42,"f":1.5,"nan":"NaN","inf":"-Infinity","b":true,"n":"12.3400",| <>
                 ~s|"at":"2026-10-02T12:30:00.123456Z","day":"2026-10-02","t":"12:30:00.000000",| <>
                 ~s|"local":"2026-10-02T12:30:00","u":"6f1c3a52-6f0a-4f43-9a41-0d8f8c2e7b11",| <>
                 ~s|"j":{"a":[1,2]},"bin":"AQL/","none":null,"arr":[1,null,3],| <>
                 ~s|"grid":[["2026-01-01"],[null]],"bins":["AA=="]}|
    end
  end

  defp spec(extra) do
    Query.spec(
      Map.merge(%{table: "orders", id_column: "id", timestamp_column: nil, columns: nil}, extra)
    )
  end

  defp endpoint, do: %{hostname: "db", port: 5432, database: "d", username: "u", ssl: false}

  defp plugin(opts),
    do: %Flezha.Plugin{id: "p", provider: PostgresPoll, opts: opts, hooks: [on_terminate: []]}
end
