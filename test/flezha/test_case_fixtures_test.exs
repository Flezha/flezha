defmodule FlezhaTest.CaseFixturesTest do
  use FlezhaTest.Case, fixtures: [:tmp_app_data, :tmp_dir, :env, :system_env], async: false

  # What the run started with. `setup_all`'s `on_exit` is the only callback that
  # runs after a test's own, so it is the one place a restore can be judged at
  # all: a teardown that deletes rather than puts back is invisible from inside
  # the test that wrote it.
  @kept "FLZ_CASE_KEPT"
  @changed "FLZ_CASE_CHANGED"
  @added "FLZ_CASE_ADDED"

  setup_all do
    System.put_env(@kept, "the run started with this")
    System.put_env(@changed, "before")
    state_root_before = Application.get_env(:flezha, :state_root)

    on_exit(fn ->
      kept = System.get_env(@kept)
      changed = System.get_env(@changed)
      added = System.get_env(@added)
      Enum.each([@kept, @changed, @added], &System.delete_env/1)

      assert kept == "the run started with this",
             "a variable the run started with was lost by a module's teardown"

      assert changed == "before", "a variable the run started with was left changed"
      refute added, "a variable the test added was left behind"

      # The `:tmp_app_data` row claims `:state_root` per test, and the run's own
      # root has to be back once the last claim has unwound. A teardown that
      # deletes the key instead drops every module after this one onto the
      # declared default, which is the checkout's `app_data/state`.
      assert Application.get_env(:flezha, :state_root) == state_root_before,
             "the run's state root was not put back: the rest of the run writes to " <>
               Flezha.State.Paths.state_root()
    end)

    :ok
  end

  test "the declared rows arrive in the context", context do
    assert File.dir?(context.base)
    assert File.dir?(context.tmp)
    assert Application.get_env(:flezha, :state_root) == context.base
    Application.put_env(:flezha, :probe_only_key, 1)
  end

  test "the env row put the probe key back", _context do
    refute Application.get_env(:flezha, :probe_only_key)
  end

  # The three ways a test moves an OS variable, in the shape KB-045 filed: an
  # unconditional `System.delete_env` in a teardown loses the first of them.
  test "the system_env row puts back what a test deleted, changed and added" do
    System.delete_env(@kept)
    System.put_env(@changed, "after")
    System.put_env(@added, "1")
  end

  test "an undeclared fixture is refused" do
    assert_raise ArgumentError, ~r/unknown fixture/, fn -> FlezhaTest.Case.install([:nope]) end
  end

  # Expanding the template's `use` runs its guard without compiling a module,
  # which under `async: true` would join the running suite.
  test "a module declaring :env or :system_env cannot be async" do
    for row <- [:env, :system_env] do
      declared = quote do: FlezhaTest.Case.__using__(fixtures: [unquote(row)], async: true)

      assert_raise ArgumentError, ~r/#{row}.*async: true/, fn ->
        Macro.expand_once(declared, __ENV__)
      end
    end

    # A directory of its own names no global, so `:tmp_dir` stays allowed.
    Macro.expand_once(
      quote(do: FlezhaTest.Case.__using__(fixtures: [:tmp_dir], async: true)),
      __ENV__
    )
  end
end
