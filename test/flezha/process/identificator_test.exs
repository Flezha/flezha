defmodule FlezhaTest.Process.IdentificatorTest do
  @moduledoc """
  The id-kind table at its limits, and the one number two runtimes share.

  Eight limit declarations in five files and two runtimes answered "may this be
  an identifier" in six different shapes, and the mint answered none of them:
  it appended four decimal draws and produced a runner id that exceeded the
  transport's socket bound on 92 % of draws, so the socket a controller opened
  carried a name the controller never reported.
  """
  use ExUnit.Case, async: true

  alias Flezha.Identificator
  alias Flezha.Transport.Native

  describe "every kind, at limit and limit + 1" do
    test "a value of exactly max is accepted and one byte more is refused" do
      for kind <- Identificator.kinds() do
        %{max: max} = Identificator.rule(kind)

        assert Identificator.validate(kind, sample(kind, max)) == :ok,
               "#{kind} refused a value of exactly #{max} bytes"

        assert {:error, {:too_long, ^kind, ^max}} =
                 Identificator.validate(kind, sample(kind, max + 1))
      end
    end

    test "a value below min is refused, and the token row's min is a floor not a cap" do
      for kind <- Identificator.kinds() do
        %{min: min} = Identificator.rule(kind)

        assert Identificator.validate(kind, sample(kind, min)) == :ok

        if min > 1 do
          assert {:error, {:too_short, ^kind, ^min}} =
                   Identificator.validate(kind, sample(kind, min - 1))
        end
      end

      # `>= 8` on a token id is a minimum: a longer digest prefix stays valid.
      assert Identificator.rule(:token).min == 8
      assert Identificator.validate(:token, String.duplicate("a", 8)) == :ok
      assert Identificator.validate(:token, String.duplicate("a", 64)) == :ok
    end

    test "a value the kind's regex refuses is malformed, and a non-binary is too" do
      assert {:error, {:malformed, :namespace}} = Identificator.validate(:namespace, "Upper")
      assert {:error, {:malformed, :namespace}} = Identificator.validate(:namespace, "9lead")
      assert {:error, {:malformed, :plugin}} = Identificator.validate(:plugin, "Has-Dash")

      assert {:error, {:malformed, :schedule_name}} =
               Identificator.validate(:schedule_name, "a b")

      assert {:error, {:malformed, :token}} = Identificator.validate(:token, "zzzzzzzz")
      assert {:error, {:malformed, :operative}} = Identificator.validate(:operative, :not_binary)
    end

    test "a kind no row declares is refused rather than defaulted" do
      assert_raise KeyError, fn -> Identificator.rule(String.to_atom("seventh_kind")) end
    end
  end

  describe "mint/2" do
    test "what it mints always passes the same table that validates it" do
      for kind <- [:runner, :operative] do
        for namespace <- ["a", "default", "colocation_ns", String.duplicate("z", 80)] do
          id = Identificator.mint(kind, namespace)

          assert Identificator.validate(kind, id) == :ok,
                 "mint(#{kind}, #{byte_size(namespace)} bytes) produced #{byte_size(id)} bytes"
        end
      end
    end

    test "a runner id fits the socket name, so the transport never renames it" do
      # The colocation failure: `colocation_ns_` plus four decimal draws
      # exceeded 24 bytes on 92 % of draws, and the transport replaced the id
      # with `colocation_~<12 hex>`.
      for _ <- 1..500 do
        id = Identificator.mint(:runner, "colocation_ns")
        assert byte_size(id) <= Identificator.limit(:runner)
      end
    end

    test "distinct draws, and the registry retry still holds" do
      ids = for _ <- 1..2_000, do: Identificator.mint(:runner, "default")
      assert length(Enum.uniq(ids)) == length(ids)
    end
  end

  describe "the two runtimes compare notes" do
    test "the runner ceiling the controller mints against is the one the transport applies" do
      assert Identificator.limit(:runner) == Native.stream_socket_id_max()
    end

    test "an id at the ceiling is composed unshortened, and one byte more is not" do
      dir = Path.join(System.tmp_dir!(), "id_pin_#{System.unique_integer([:positive])}")
      max = Identificator.limit(:runner)

      at_limit = String.duplicate("a", max)
      assert {:ok, path} = Native.stream_socket_path(dir, at_limit, "primary")
      assert Path.basename(path) == "primary_stream_#{at_limit}.sock"

      over = String.duplicate("a", max + 1)
      assert {:ok, shortened} = Native.stream_socket_path(dir, over, "primary")
      refute Path.basename(shortened) == "primary_stream_#{over}.sock"
      assert Path.basename(shortened) =~ ~r/~[0-9a-f]{12}\.sock$/
    end
  end

  # A value of exactly `bytes` that satisfies the kind's regex, so the length
  # rule is what each case is measuring.
  defp sample(:namespace, bytes), do: String.duplicate("a", bytes)
  defp sample(:token, bytes), do: String.duplicate("f", bytes)
  defp sample(:plugin, bytes), do: String.duplicate("a", bytes)
  defp sample(:schedule_name, bytes), do: String.duplicate("a", bytes)
  defp sample(:legacy_state_dir, bytes), do: String.duplicate("a", bytes)
  defp sample(_kind, bytes), do: String.duplicate("a", bytes)
end
