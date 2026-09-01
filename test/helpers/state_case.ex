defmodule FlezhaTest.StateCase do
  @moduledoc """
  The case template the persistence modules share:

      use FlezhaTest.StateCase, async: false

  Each test gets a state root of its own — registered as `:state_root` and
  removed with the test — and the writer registry, `Flezha.State.Supervisor`
  and the `FsyncBatcher` running above it. The root arrives in the context as
  `:base`.

  `async:` stays at the call site: every user of this template names a global,
  and the option beside the `use` is what says which.
  """

  use ExUnit.CaseTemplate

  alias Flezha.State.TestHelpers

  using do
    quote do
      import Flezha.State.TestHelpers, only: [uniq: 0]
    end
  end

  setup do
    base = TestHelpers.tmp_app_data_dir()
    TestHelpers.start_state_tree()

    %{base: base}
  end
end
