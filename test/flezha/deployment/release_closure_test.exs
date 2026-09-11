defmodule FlezhaTest.Deployment.ReleaseClosureTest do
  @moduledoc """
  `mix test` puts every compiled dependency on the code path, while a release
  ships only the applications its `.app` files reach. A library that declares a
  dependency `runtime: false` leaves it out of its `.app`, so every release built
  on that library lacks it while every suite still loads it.
  """
  use ExUnit.Case, async: true

  test "every release ships each dependency of an application it ships" do
    config = Mix.Project.config()
    releases = Keyword.keys(Keyword.get(config, :releases, []))
    assert releases != [], "mix.exs defines no release to check"

    for name <- releases do
      # The closure `mix release` itself assembles, its overrides included.
      shipped = Mix.Release.from_config!(name, config, []).applications

      edges =
        for {app, deps} <- Mix.Project.deps_tree(),
            Map.has_key?(shipped, app),
            dep <- deps,
            do: {app, dep}

      # process_hub declares elector `runtime: false`: an edge the dependency
      # tree carries and no `.app` file does, which is what this test must see.
      assert {:process_hub, :elector} in edges

      missing = for {app, dep} <- edges, not Map.has_key?(shipped, dep), do: {dep, app}

      assert missing == [],
             "release #{name} leaves out #{inspect(missing)}, each keyed to the " <>
               "shipped application that needs it; declare each in mix.exs"
    end
  end
end
