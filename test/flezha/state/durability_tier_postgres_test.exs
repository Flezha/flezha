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

  use ExUnit.Case, async: false

  alias Flezha.State.DurabilityTier.Backend.Postgres, as: Backend

  @moduletag :postgres_integration

  @table "flezha_durability_test_artifacts"

  setup_all do
    if System.get_env("POSTGRES_INTEGRATION") != "1" do
      raise "Refusing to run postgres_integration tests without POSTGRES_INTEGRATION=1"
    end

    opts =
      FlezhaTest.Helper.PostgresSink.conn_opts()
      |> Keyword.merge(table: @table, auto_create: true)

    start_supervised!(Backend.child_spec(opts))
    %{opts: opts}
  end

  setup %{opts: opts} do
    {:ok, keys} = Backend.list("", opts)
    Enum.each(keys, &Backend.delete(&1, opts))
    %{opts: opts}
  end

  test "put, get and delete round-trip opaque bytes", %{opts: opts} do
    blob = :crypto.strong_rand_bytes(4096)

    assert :ok = Backend.put("blob/e/a/b/c/r-0-0-9", blob, opts)
    assert {:ok, ^blob} = Backend.get("blob/e/a/b/c/r-0-0-9", opts)

    assert :ok = Backend.delete("blob/e/a/b/c/r-0-0-9", opts)
    assert {:error, :not_found} = Backend.get("blob/e/a/b/c/r-0-0-9", opts)
    assert :ok = Backend.delete("blob/e/a/b/c/r-0-0-9", opts), "delete is idempotent"
  end

  test "list is prefix-scoped and treats underscores literally", %{opts: opts} do
    # `_` is a LIKE wildcard and appears in every reserved and base64-encoded
    # identity, so a LIKE-based prefix match would silently over-match here.
    assert :ok = Backend.put("manifest/e/cls/proc/_key", "a", opts)
    assert :ok = Backend.put("manifest/e/cls/proc/xkey", "b", opts)
    assert :ok = Backend.put("manifest/e/other/proc/k", "c", opts)

    assert {:ok, keys} = Backend.list("manifest/e/cls/", opts)
    assert Enum.sort(keys) == ["manifest/e/cls/proc/_key", "manifest/e/cls/proc/xkey"]

    assert {:ok, exact} = Backend.list("manifest/e/cls/proc/_key", opts)
    assert exact == ["manifest/e/cls/proc/_key"]
  end

  test "put_if writes at an equal or higher epoch and refuses a stale one", %{opts: opts} do
    key = "manifest/e/cls/proc/k"

    assert :ok = Backend.put_if(key, "epoch-0", 0, opts)
    assert {:ok, "epoch-0"} = Backend.get(key, opts)

    # Equal epochs overwrite: one controller updates its own manifest many
    # times inside a single epoch.
    assert :ok = Backend.put_if(key, "epoch-0-again", 0, opts)
    assert {:ok, "epoch-0-again"} = Backend.get(key, opts)

    assert :ok = Backend.put_if(key, "epoch-2", 2, opts)
    assert {:ok, "epoch-2"} = Backend.get(key, opts)

    # The fence: a claimant at a lower epoch is refused and told who holds it.
    assert {:error, {:stale_epoch, 2}} = Backend.put_if(key, "epoch-1", 1, opts)
    assert {:ok, "epoch-2"} = Backend.get(key, opts)
  end

  test "a table name that is not a bare identifier is refused, not interpolated" do
    assert_raise ArgumentError, fn -> Backend.table(table: "artifacts; DROP TABLE users") end
  end
end
