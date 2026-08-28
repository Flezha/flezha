# Credo in measuring mode: `mix credo --strict` counts, scripts/refactor_metrics.py
# ratchets the count. The ceilings are docs/refactor/charter.md §7.
%{
  configs: [
    %{
      name: "default",
      files: %{
        included: ["lib/", "test/", "config/", "mix.exs"],
        excluded: [~r"/_build/", ~r"/deps/", ~r"/node_modules/", ~r"^test/apps/"]
      },
      strict: true,
      checks: %{
        extra: [
          {Credo.Check.Refactor.CyclomaticComplexity, [max_complexity: 10]}
        ]
      }
    }
  ]
}
