defmodule FlezhaTest.Case do
  @moduledoc """
  The case template that owns the fixtures a test declares:

      use FlezhaTest.Case, fixtures: [:tmp_app_data, :env], async: false

  A row that takes options is named with them:

      use FlezhaTest.Case, fixtures: [{:auth_stores, prefix: "flz_verbs", claim: true}]

  A fixture is one row in the table - `{name, setup, cleanup}` - and a module
  that needs one names it. Before this table each module built its own: a
  temporary directory was thirty-one spellings across thirty-five files, an
  application-environment key was a `put_env` in `setup` and a `restore_env`
  written four times over, and every one of those pairs was a chance to forget
  the second half. `scripts/check-test-fixture.sh` fails a module that builds
  one of these by hand and declares none.

  `async:` stays at the call site, because the fixture a module declares does
  not decide whether it names a global; the module does.

  ## The rows

  A row's `setup` returns the map merged into the test context. Its `cleanup`
  is the state that has to be put back, and every declared row's cleanup runs
  through ONE `on_exit`, innermost fixture first.

  A row whose setup ALREADY unwinds itself declares no cleanup, and that is not
  an omission: `Flezha.State.TestHelpers.tmp_app_data_dir/0` is also driven from
  a bare BEAM with no ExUnit running, so the unwind has to live with the claim
  rather than with the caller, and `setup_auth_stores/1` and
  `FlezhaTest.Helper.DlqFixtures.isolated_dir/1` are reached directly by modules that
  do not use this template at all. Wrapping those in a second cleanup here would
  put the same removal in two places.

  ## What this table deliberately does NOT own

  `start_supervised/1` is not a row. ExUnit already owns a supervised child and
  unwinds it with the test, so it IS the seam; a row for it would replace one
  seam with another instead of removing a bypass.
  """

  use ExUnit.CaseTemplate

  alias Flezha.State.TestHelpers, as: StateHelpers
  alias FlezhaTest.Helper.DlqFixtures
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  # A function and not a module attribute: an attribute is injected as an
  # escaped term, and an anonymous function cannot be escaped.
  #
  # {name, setup/1 -> context map, cleanup/1 | nil}. `setup` takes the options a
  # module named beside the row, so a row whose helper is already configurable
  # stays one row instead of forking into a second for every option set.
  # `cleanup` takes the map the setup returned, so a row that has something to
  # put back reads back what it claimed rather than looking it up again.
  defp table do
    [
      # A state root of this test's own, registered as `:state_root`.
      {:tmp_app_data, fn _opts -> %{base: StateHelpers.tmp_app_data_dir()} end, nil},
      # A bare temporary directory for a module with no state tree under it.
      {:tmp_dir,
       fn opts ->
         %{tmp: NodeTestHelper.claim_tmp_dir(Keyword.get(opts, :prefix, "flz_case"))}
       end, fn %{tmp: tmp} -> File.rm_rf!(tmp) end},
      # The five auth stores over a directory of their own.
      {:auth_stores, fn opts -> AuthTestHelper.setup_auth_stores(opts) end, nil},
      # The DLQ store pointed at a directory of this test's own.
      {:isolated_dlq_dir, fn opts -> %{dlq_dir: DlqFixtures.isolated_dir(opts)} end, nil},
      # The writer registry, `Flezha.State.Supervisor` and the `FsyncBatcher`.
      {:state_tree,
       fn opts ->
         :ok = StateHelpers.start_state_tree(opts)
         %{}
       end, nil},
      # Every `:flezha` application-environment key this test changes, put back
      # as it was. The snapshot is the whole environment and the restore is the
      # DIFF against it, so a key the test added is deleted, a key it changed is
      # returned to its old value, and a key it never touched is left alone -
      # including one another fixture's own cleanup has already removed.
      {:env, fn _opts -> %{__env_before__: Application.get_all_env(:flezha)} end,
       fn %{__env_before__: before} -> restore_env(before) end}
    ]
  end

  @doc "The declared fixture names, in table order."
  @spec fixtures() :: [atom()]
  def fixtures, do: Enum.map(table(), fn {name, _setup, _cleanup} -> name end)

  @doc """
  Run `declarations` in table order, register one `on_exit` for the cleanups
  that declare one, and return the merged context.

  A declaration is a row name, or `{name, opts}` when the row takes options.
  """
  @spec install([atom() | {atom(), keyword()}]) :: map()
  def install(declarations) do
    named = Enum.map(declarations, &elem(normalise(&1), 0))
    unknown = named -- fixtures()

    if unknown != [] do
      raise ArgumentError,
            "unknown fixture(s) #{inspect(unknown)}; the table holds #{inspect(fixtures())}"
    end

    opts_for = Map.new(declarations, &normalise/1)

    claimed =
      for {name, setup, cleanup} <- table(), name in named do
        {cleanup, setup.(opts_for[name])}
      end

    # One `on_exit`, innermost fixture first: a row claimed later may depend on
    # one claimed earlier, so it is put back first.
    ExUnit.Callbacks.on_exit(fn ->
      for {cleanup, context} <- Enum.reverse(claimed), cleanup != nil do
        cleanup.(context)
      end

      :ok
    end)

    # A row may carry state its cleanup needs and the test never asks for. Those
    # keys are spelled `__name__` and are kept out of the context, so a fixture
    # that has to remember something does not put it in front of the reader.
    claimed
    |> Enum.reduce(%{}, fn {_cleanup, context}, acc -> Map.merge(acc, context) end)
    |> Map.reject(fn {key, _value} -> String.starts_with?(Atom.to_string(key), "__") end)
  end

  defp normalise({name, opts}) when is_atom(name) and is_list(opts), do: {name, opts}
  defp normalise(name) when is_atom(name), do: {name, []}

  defp restore_env(before) do
    for {key, value} <- Application.get_all_env(:flezha) do
      case Keyword.fetch(before, key) do
        {:ok, ^value} -> :ok
        {:ok, original} -> Application.put_env(:flezha, key, original)
        :error -> Application.delete_env(:flezha, key)
      end
    end

    for {key, value} <- before, not Keyword.has_key?(Application.get_all_env(:flezha), key) do
      Application.put_env(:flezha, key, value)
    end

    :ok
  end

  using opts do
    names = Keyword.get(opts, :fixtures, [])

    quote do
      setup do
        FlezhaTest.Case.install(unquote(names))
      end
    end
  end
end
