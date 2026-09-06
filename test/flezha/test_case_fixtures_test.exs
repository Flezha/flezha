defmodule FlezhaTest.CaseFixturesTest do
  use FlezhaTest.Case, fixtures: [:tmp_app_data, :tmp_dir, :env], async: false

  test "the declared rows arrive in the context", context do
    assert File.dir?(context.base)
    assert File.dir?(context.tmp)
    assert Application.get_env(:flezha, :state_root) == context.base
    Application.put_env(:flezha, :probe_only_key, 1)
  end

  test "the env row put the probe key back", _context do
    refute Application.get_env(:flezha, :probe_only_key)
  end

  test "an undeclared fixture is refused" do
    assert_raise ArgumentError, ~r/unknown fixture/, fn -> FlezhaTest.Case.install([:nope]) end
  end
end
