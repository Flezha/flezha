# Echo: the Go hello world

The Go hello world, as a standalone project. `go.mod` names one released version of
`github.com/flezha/flezha_go_sdk`, the way your own project would.

Three files, one for each of the [building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md):

| File | Part | What it does |
|---|---|---|
| `tasklet.go` | the tasklet | the processing stage: prefixes each message |
| `relay.go` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `main.go` | the runner | this process: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([Up and running](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md)).

```sh
eval "$(go run github.com/flezha/flezha_go_sdk/cmd/flz-native@v0.1.1)"
go run .
```

The `eval` line fetches the SDK's prebuilt core library and sets `CGO_LDFLAGS`, as
the SDK's [Install](../../README.md#install) describes; on a platform it has no
library for, build one from a checkout ([Building from a checkout](#building-from-a-checkout)).

Then, from another terminal:

```sh
flz relay start echo --id my_echo
flz relay send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

It blocks until shutdown, which is a SIGTERM or Ctrl-C. The namespace is the
first argument and defaults to `default`: `go run . my_app`.

## Building from a checkout

To run the example against a Flezha checkout instead of the published module, or on a
platform with no prebuilt core library, a C toolchain and the checkout are needed.
From the root of the checkout, build the core once:

```sh
scripts/build-go-sdk.sh --release
```

Then, from this folder, point Go at the checkout, with its absolute path:

```sh
go work init .
go work edit -replace github.com/flezha/flezha_go_sdk=/path/to/flezha/sdk/go
export CGO_LDFLAGS="-L/path/to/flezha/_build/.target/release/go"
```

Git ignores the `go.work` file.

## Where to go next

[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what each of the three parts is and why the split is the way it
is. The SDK's guide, `docs/internal/sdk/go_sdk.md` in the Flezha repository,
covers state, keys, timers, steps relays and the rest.
