defmodule FlezhaTest.StateCase do
  @moduledoc """
  The case template the persistence modules share:

      use FlezhaTest.StateCase, async: false

  Two rows of the `FlezhaTest.Case` fixture table: `:tmp_app_data` gives each
  test a state root of its own, registered as `:state_root` and removed with the
  test, and `:state_tree` runs the writer registry, `Flezha.State.Supervisor`
  and the `FsyncBatcher` above it. The root arrives in the context as `:base`.

  `async:` stays at the call site: every user of this template names a global,
  and the option beside the `use` is what says which.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      import Flezha.State.TestHelpers, only: [uniq: 0]
    end
  end

  setup do
    FlezhaTest.Case.install([:tmp_app_data, :state_tree])
  end
end
