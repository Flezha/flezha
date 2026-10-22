# Starter: a relay that counts visits per page

A small, complete Flezha project in Go, to grow into your own: one relay, one
tasklet that keeps state, and a test. `go.mod` names the SDK by its released
version, the way your own project does.

| File | What it is |
|---|---|
| `count.go` | the tasklet `count`: keeps a page's total in state, changes it only through the `Counted` change, and answers the new count |
| `relay.go` | the relay `starter`: the entry channel `hit`, described and keyed by page, into `count`, out on `counts` |
| `main.go` | the runner: serves the relay until shutdown |
| `count_test.go` | the test: four hits with no controller, and the replay check |

## Run it

You need a controller running and reachable, as
[Start the controller](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md#start-the-controller)
starts one. From this folder, run the test and start the runner:

```sh
go test ./...
go run .
```

Then, from another terminal, start the relay and call it:

```sh
flz relay start starter --id starter
flz relay call starter --channel hit --arg page=home
```

```
{"page":"home","total":1}
```

Each call for the same page answers the next total. `flz` is the release's
`bin/flz`. The namespace is the runner's first argument and defaults to
`default`: `go run . my_app`.

## Make it yours

Nothing inside the files is renamed for you. Change the module path in
`go.mod`. `starter` is the name `flz relay start` takes. `count` is the name
the tasklet's state is saved under, so choose it before the first run.

## Before the SDK is published

`github.com/flezha/flezha_go_sdk` is not published yet, and neither is the
core library it links through cgo, so a C toolchain and a Flezha checkout are
needed. From the root of the checkout, build the core once:

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
