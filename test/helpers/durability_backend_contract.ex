defmodule FlezhaTest.DurabilityBackendContract do
  @moduledoc """
  The blob contract every durability backend keeps, written once and run by
  each backend's own test file:

      use FlezhaTest.DurabilityBackendContract, backend: LocalPath

  The file's setup puts the backend's options in the context as `:opts`, and
  the `use` follows the alias that names the backend and any `@moduletag`, as
  both apply only to what comes after them. What only one backend does stays
  in that backend's file.
  """

  defmacro __using__(opts) do
    backend = Keyword.fetch!(opts, :backend)

    quote do
      test "put, get and delete round-trip opaque bytes", %{opts: opts} do
        backend = unquote(backend)
        blob = :crypto.strong_rand_bytes(64 * 1024)

        assert :ok = backend.put("blob/e/cls/task/k/r-0-0-9", blob, opts)
        assert {:ok, ^blob} = backend.get("blob/e/cls/task/k/r-0-0-9", opts)

        assert :ok = backend.delete("blob/e/cls/task/k/r-0-0-9", opts)
        assert {:error, :not_found} = backend.get("blob/e/cls/task/k/r-0-0-9", opts)
        assert :ok = backend.delete("blob/e/cls/task/k/r-0-0-9", opts), "delete is idempotent"
      end

      test "list is prefix-scoped and answers each key as it was put, with its age",
           %{opts: opts} do
        backend = unquote(backend)
        assert :ok = backend.put("manifest/e/cls/task/_key", "a", opts)
        assert :ok = backend.put("manifest/e/other/task/k", "b", opts)

        assert {:ok, [{"manifest/e/cls/task/_key", age}]} = backend.list("manifest/e/cls/", opts)
        assert is_integer(age) and age >= 0
      end

      test "put_if extends an owned lineage and refuses a bid that is not ahead", %{opts: opts} do
        backend = unquote(backend)
        key = "manifest/e/cls/task/k"

        assert :ok = backend.put_if(key, "claim-1", {:claim, 1}, opts)
        assert {:ok, "claim-1"} = backend.get(key, opts)

        # An owner extends at the claim it holds, many times over.
        assert :ok = backend.put_if(key, "claim-1-again", {:extend, 1}, opts)
        assert {:ok, "claim-1-again"} = backend.get(key, opts)

        # A bid at the counter already stored is refused. This strictness is the
        # compare-and-set: without it two claimants reading the same counter
        # would both write, and the last one would win.
        assert {:error, {:claim_held, 1}} = backend.put_if(key, "collision", {:claim, 1}, opts)
        assert {:ok, "claim-1-again"} = backend.get(key, opts)

        # A genuine takeover bids above it, and the writer it superseded
        # cannot extend.
        assert :ok = backend.put_if(key, "claim-2", {:claim, 2}, opts)
        assert {:ok, "claim-2"} = backend.get(key, opts)
        assert {:error, {:claim_held, 2}} = backend.put_if(key, "stale", {:extend, 1}, opts)
        assert {:ok, "claim-2"} = backend.get(key, opts)
      end
    end
  end
end
