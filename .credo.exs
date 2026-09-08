# Credo in measuring mode: `mix credo --strict` counts and nothing ratchets the
# count. The ceilings a change is held to are docs/code_standards.md.
%{
  configs: [
    %{
      name: "default",
      files: %{
        included: ["lib/", "test/", "config/", "mix.exs"],
        # Every pattern here is matched against an ABSOLUTE path, so each must be
        # unanchored: credo passes a %Regex{} through `convert_globs_to_local_paths/2`
        # untouched and `exclude/2` is a bare `String.match?`.
        excluded: [~r"/_build/", ~r"/deps/", ~r"/node_modules/", ~r"/test/apps/"]
      },
      strict: true,
      checks: %{
        extra: [
          {Credo.Check.Refactor.CyclomaticComplexity, [max_complexity: 10]}
        ],
        disabled: [
          {Credo.Check.Readability.ParenthesesOnZeroArityDefs, []}
        ]
      }
    }
  ]
}
