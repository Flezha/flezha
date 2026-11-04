# Add your own plugin

On this page you download the HTTP plugin's package and install it by
rebuilding the controller's image. A plugin package (`.fpz`) is loaded when the controller
boots. [Installing plugins](../installing_plugins.html) covers packages,
and [Writing a plugin](../writing_a_plugin.html) how to write your own.

> Download the HTTP plugin's package into the agentdesk's `controller/plugins/`
> folder, then rebuild the controller's image and replace its container, from
> the agentdesk folder.
> {: .step}

```sh
$ curl -fsSL -o controller/plugins/flezha_http-0.1.1.fpz https://github.com/flezha/flezha/releases/download/v0.1.1/flezha_http-0.1.1.fpz
$ docker compose up -d --build --wait controller
$ flz plugin list --installed
```

```output
flezha_http 0.1.1  loaded  providers: http_server
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
