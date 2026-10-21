# Add your own plugin

On this page you build the HTTP plugin's package and install it by rebuilding
the controller's image. A plugin package (`.fpz`) is loaded when the controller
boots. [Installing plugins](../installing_plugins.html) covers packages,
and [Writing a plugin](../writing_a_plugin.html) how to write your own.

> From the root of your clone, build the plugin builder image, then package the
> HTTP plugin in it.
> {: .step}

```sh
$ docker build --target plugin-builder -t flezha/plugin-builder:0.1.0 -f deploy/docker/prod/Dockerfile.controller .
$ docker run --rm -v "$PWD:/src" -w /src/plugins/flezha_http_plugin flezha/plugin-builder:0.1.0 sh -c 'mix deps.get && mix flezha.plugin.package'
```

```output
...
Generated flezha_http app
Wrote /src/plugins/flezha_http_plugin/_build/flezha_http-0.1.0.fpz
```

> Copy the package into the agentdesk's `controller/plugins/` folder, then
> rebuild the controller's image and replace its container, from the agentdesk
> folder.
> {: .step}

```sh
$ cp ../../../../plugins/flezha_http_plugin/_build/flezha_http-0.1.0.fpz controller/plugins/
$ docker compose up -d --build --wait controller
$ flz plugin list --installed
```

```output
flezha_http 0.1.0  loaded  providers: http_server
```

The sessions survived the new controller. They are asleep, and wake on the
next message:

```sh
$ flz relay call anna --namespace agents --text 'Did you notice anything?'
```

```output
{"text":"Message 5: you said \"Did you notice anything?\". I am a demo model.","tokens":13}
```

## Next

[Serve HTTP](serve_http.md) uses the plugin.
