# Example plugin

A plugin project, `flezha_example_plugin`, and a library it bundles, `stamp_lib`. Its one
provider, `example_stamp`, appends every output payload of a relay to the file its
`path` option names, prefixed with a stamp `stamp_lib` renders.

The one package exercises what a controller does with any package at boot: it bundles a
library the release does not ship, starts that library's application, reads a file from
its `priv/`, re-consolidates `String.Chars` for the library's struct, and supervises the
provider's declared worker.

Package it on the controller's OTP and Elixir versions:

```
MIX_ENV=prod mix deps.get
MIX_ENV=prod mix flezha.plugin.package
```

The archive is written to `_build/flezha_example_plugin-0.1.0.fpz`. The guide
`docs/topics/plugins/writing_a_plugin.md` installs a package by rebuilding a controller's
image, and `docs/topics/plugins/installing_plugins.md` covers the other ways.
