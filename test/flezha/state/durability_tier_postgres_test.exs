defmodule Flezha.State.DurabilityTier.Backend.PostgresTest do
  @moduledoc """
  The Postgres durability backend against a real Postgres in
  `docker/dev/docker-compose.yml`. Tagged `:postgres_integration` so the
  default `mix test` skips it — run with:

      POSTGRES_INTEGRATION=1 mix test --include postgres_integration \\
        test/flezha/state/durability_tier_postgres_test.exs

  Pre-req: `docker compose -f docker/dev/docker-compose.yml up -d postgres-sink`
  (reachable at localhost:5433, user/db/password `flezha`/`flezha_test`).

  What matters here is the blob contract and, above all, the conditional write:
  the tier's fencing is only as strong as `put_if/4`'s refusal, and that
  refusal is one SQL statement whose `WHERE` clause is easy to get subtly
  backwards.
  """

  # async: false: registered singletons.

  use ExUnit.Case, async: false

  alias Flezha.State.DurabilityTier.Backend.Postgres, as: Backend
  alias FlezhaTest.Helper.PostgresSink

  @moduletag :postgres_integration

  use FlezhaTest.DurabilityBackendContract, backend: Backend

  @table "flezha_durability_test_artifacts"

  setup_all do
    if System.get_env("POSTGRES_INTEGRATION") != "1" do
      raise "Refusing to run postgres_integration tests without POSTGRES_INTEGRATION=1"
    end

    opts =
      PostgresSink.conn_opts()
      |> Keyword.merge(table: @table, auto_create: true)

    start_supervised!(Backend.child_spec(opts))
    %{opts: opts}
  end

  setup %{opts: opts} do
    {:ok, entries} = Backend.list("", opts)
    Enum.each(entries, fn {key, _age} -> Backend.delete(key, opts) end)
    %{opts: opts}
  end

  test "list treats underscores literally", %{opts: opts} do
    # `_` is a LIKE wildcard and appears in every reserved and base64-encoded
    # identity, so a LIKE-based prefix match would silently over-match here.
    assert :ok = Backend.put("manifest/e/cls/task/_key", "a", opts)
    assert :ok = Backend.put("manifest/e/cls/task/xkey", "b", opts)
    assert :ok = Backend.put("manifest/e/other/task/k", "c", opts)

    assert {:ok, entries} = Backend.list("manifest/e/cls/", opts)

    assert entries |> Enum.map(&elem(&1, 0)) |> Enum.sort() ==
             ["manifest/e/cls/task/_key", "manifest/e/cls/task/xkey"]

    assert {:ok, [{"manifest/e/cls/task/_key", _}]} =
             Backend.list("manifest/e/cls/task/_key", opts)
  end

  test "a table name that is not a bare identifier is refused, not interpolated" do
    assert_raise ArgumentError, fn -> Backend.table(table: "artifacts; DROP TABLE users") end
  end
end
