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

    assert {:ok, entries} = Backend.list("manifest/e/cls/", opts)

    assert entries |> Enum.map(&elem(&1, 0)) |> Enum.sort() ==
             ["manifest/e/cls/proc/_key", "manifest/e/cls/proc/xkey"]

    # The age is computed by the database, so it is a real elapsed value rather
    # than something this process timed.
    assert Enum.all?(entries, fn {_key, age} -> is_integer(age) and age >= 0 end)

    assert {:ok, [{"manifest/e/cls/proc/_key", _}]} =
             Backend.list("manifest/e/cls/proc/_key", opts)
  end

  test "put_if extends an owned lineage and refuses a bid that is not ahead", %{opts: opts} do
    key = "manifest/e/cls/proc/k"

    assert :ok = Backend.put_if(key, "claim-1", {:claim, 1}, opts)
    assert {:ok, "claim-1"} = Backend.get(key, opts)

    # An owner extends at the claim it holds, many times over.
    assert :ok = Backend.put_if(key, "claim-1-again", {:extend, 1}, opts)
    assert {:ok, "claim-1-again"} = Backend.get(key, opts)

    # A bid at the counter already stored is refused. This strictness is the
    # compare-and-set: without it two claimants reading the same counter would
    # both write, which is exactly the last-writer-wins this replaced.
    assert {:error, {:claim_held, 1}} = Backend.put_if(key, "collision", {:claim, 1}, opts)
    assert {:ok, "claim-1-again"} = Backend.get(key, opts)

    # A genuine takeover bids above it.
    assert :ok = Backend.put_if(key, "claim-2", {:claim, 2}, opts)
    assert {:ok, "claim-2"} = Backend.get(key, opts)

    # And the writer it replaced can no longer extend.
    assert {:error, {:claim_held, 2}} = Backend.put_if(key, "stale", {:extend, 1}, opts)
    assert {:ok, "claim-2"} = Backend.get(key, opts)
  end

  test "a table name that is not a bare identifier is refused, not interpolated" do
    assert_raise ArgumentError, fn -> Backend.table(table: "artifacts; DROP TABLE users") end
  end
end
