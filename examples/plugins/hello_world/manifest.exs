%{
  id: "hello_world",
  version: "1.0.0",
  description: "Minimal installable-plugin example for Flezha.",
  author: "Flezha Examples",
  modules: [Flezha.Examples.HelloWorld],
  provides: [
    %{
      id: "default",
      provider: Flezha.Examples.HelloWorld,
      hooks: [{:on_input, []}],
      default_opts: %{greeting: "hello"},
      scope: :relay
    }
  ],
  dependencies: []
}
