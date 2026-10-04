# Plugins from your own code

Enabling, binding, unbinding and disabling are on the SDK's admin handle, so an
application can attach the plugins its own namespace needs. The calls carry what the
command line carries: the plugin's name, its provider and the provider's own options under
`opts` at enable, then the operator and the hooks that `--hook=on_output` spells at bind.
Here the echo runner enables a file sink named `writer` and binds `my_echo` to it, once
the runner has started:

<!-- tabs-open -->

### Java

```java
// Main.java: the echo runner's plugin calls
Admin admin = runner.admin();
ObjectNode opts = JsonNodeFactory.instance.objectNode().put("path", "echo-out.txt");
admin.enablePlugin("writer", PluginSpec.of("operator_file_sink").opts(opts));
ArrayNode hooks = JsonNodeFactory.instance.arrayNode();
hooks.addArray().add("on_output").addObject();
admin.bindPlugin("writer", "my_echo", hooks, null);
```

[Full file](https://github.com/flezha/flezha_java_sdk/blob/main/examples/echo/src/main/java/com/flezha/examples/echo/Main.java)

### Rust

```rust
// main.rs: the echo runner's plugin calls
let admin = runner.admin();
admin.enable_plugin(
    "writer",
    &flezha_runner::PluginSpec::new("operator_file_sink")
        .opts(serde_json::json!({ "path": "echo-out.txt" })),
)?;
admin.bind_plugin("writer", "my_echo", serde_json::json!([["on_output", {}]]), "")?;
```

[Full file](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/echo/src/main.rs)

### Node

```javascript
// runner.mjs: the echo runner's plugin calls
const admin = runner.admin();
await admin.enablePlugin('writer', 'operator_file_sink', { opts: { path: 'echo-out.txt' } });
await admin.bindPlugin('writer', 'my_echo', { hooks: [['on_output', {}]] });
```

[Full file](https://github.com/flezha/flezha_node_sdk/blob/main/examples/echo/runner.mjs)

### Python

```python
# runner.py: the echo runner's plugin calls
admin = runner.admin()
admin.enable_plugin("writer", "operator_file_sink", opts={"path": "echo-out.txt"})
admin.bind_plugin("writer", "my_echo", hooks=[["on_output", {}]])
```

[Full file](https://github.com/flezha/flezha_python_sdk/blob/main/examples/echo/runner.py)

### Ruby

```ruby
# runner.rb: the echo runner's plugin calls
admin = runner.admin
admin.enable_plugin("writer", "operator_file_sink", opts: { "path" => "echo-out.txt" })
admin.bind_plugin("writer", "my_echo", hooks: [["on_output", {}]])
```

[Full file](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/echo/runner.rb)

### PHP

```php
// runner.php: the echo runner's plugin calls
$admin = $runner->admin();
$admin->enablePlugin('writer', 'operator_file_sink', ['path' => 'echo-out.txt']);
$admin->bindPlugin('writer', 'my_echo', hooks: [['on_output', new stdClass()]]);
```

[Full file](https://github.com/flezha/flezha_php_sdk/blob/main/examples/echo/runner.php)

### Elixir

```elixir
# application.ex: the echo application's plugin calls
alias Flezha.SDK.Admin
runner = Flezha.SDK.RunnerServer
{:ok, _} =
  Admin.enable_plugin(runner, "writer", "operator_file_sink",
    opts: %{"path" => "echo-out.txt"}
  )

{:ok, _} = Admin.bind_plugin(runner, "writer", "my_echo", hooks: [["on_output", %{}]])
```

[Full file](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/echo/lib/echo/application.ex)

<!-- tabs-close -->

The admin handle also has `unbindPlugin("writer", "my_echo")` and
`disablePlugin("writer")` (`unbind_plugin` and `disable_plugin` in Rust, Python, Ruby and
Elixir). [`namespaces.md`](../basics/namespaces.md) covers the whole admin handle. What it cannot do
is install, upgrade or uninstall a plugin package: those are node-wide acts, so they stay
with the user. An application may attach and detach the plugins its own namespace already
has.
