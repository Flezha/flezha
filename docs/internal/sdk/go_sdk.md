# Go SDK

A poll host over `flz_core`, linked in through its C ABI with cgo. The SDK contains no kernel
code: transport, framing, ordering, per-cell exclusivity, durability, retries, the retry→park
ladder, timers, credit, routing, steps runs and reconnect all live in the core. This package
brings the authoring surface, the codec for Go types, the goroutine host and idiomatic errors.

**Support tier:** [tier 2](../../public/publishing/maturity.md#sdk-go), passes the full conformance
run exactly as tier 1 does, with every capability present, and is permanently supported; it is
not yet run by the maintainer in a live deployment, so a defect specific to it is likely to be
found by its users first.

Everything here is reached through the C ABI (`flz_core_ffi`), the one PHP calls through
`ext-ffi`, so a capability the Go SDK cannot reach is a defect of the ABI rather than of this
package.

- The boundary in mechanism detail → [`poll_core.md`](poll_core.md)
- The language-agnostic authoring contract → [`authoring_contract.md`](authoring_contract.md)
- What proves this SDK still works → [`conformance.md`](conformance.md), and the manifest at
  `test/apps/go_runner/conformance.json`

**Contents**

1. [Building](#building)
2. [Authoring](#authoring)
3. [State and changes](#state-and-changes)
4. [Concurrency](#concurrency)
5. [Signals and shutdown](#signals-and-shutdown)
6. [Testing a tasklet](#testing-a-tasklet)
7. [Size](#size)
8. [Fixtures](#fixtures)
9. [Diagnostics](#diagnostics)

## Building

The module is `github.com/flezha/flezha_go_sdk`, package `flezha`, and it requires nothing beyond
the Go standard library; its `go.mod` names the oldest Go release the Go project supports. The
core is linked statically from the `staticlib` of `flz_core_ffi`, so a built runner is one
executable that loads no core library at run time. That makes cgo, and therefore a C toolchain,
a requirement: `CGO_ENABLED=0` builds are not supported.

The SDK's cgo directives name the core library and the system libraries it needs. The directory
holding `libflz_core_ffi.a` comes from the build, through `CGO_LDFLAGS`:

```sh
scripts/build-go-sdk.sh            # debug core; --release for a release one
export CGO_LDFLAGS="-L$PWD/_build/.target/debug/go"
cd sdk/go && go test ./...
```

The script builds the crate's `staticlib`, copies it into `_build/.target/<profile>/go/`, a
directory holding only the `.a` (the target directory also holds the shared library, which a
linker would prefer), builds the test app to `_build/.target/<profile>/go_runner`, and prints the
`CGO_LDFLAGS` line it used. A link that cannot find the library fails naming `flz_core_ffi`.
The SDK's own suite also drives `_build/.target/debug/flz-fake-controller`, which the script
builds.

How a `go get` user obtains the prebuilt library is deferred with every SDK's per-platform
distribution, until the project is published.

## Authoring

A tasklet is a type with a `Spec` and a `Run`. `Spec` carries the required `Name`, where every
cell's state is saved, and the scalar settings; it is read once, when the relay is registered.
`Run` takes the input and a typed context and returns an `error`:

```go
type Order struct {
	Pair string `json:"pair"`
	Qty  int    `json:"qty"`
}

type Book struct {
	Sum int `json:"sum"`
}

type Added struct {
	Qty int `json:"qty"`
}

func (a Added) ApplyTo(b *Book) { b.Sum += a.Qty }

type Totals struct{}

func (Totals) Spec() flezha.Spec { return flezha.Spec{Name: "totals", IdleTTLSeconds: 3600} }

func (Totals) Changes() flezha.Changes[Book] { return flezha.Changes[Book]{"added": Added{}} }

func (Totals) Run(in Order, ctx *flezha.Ctx[Book, int]) error {
	if err := ctx.Change(Added{Qty: in.Qty}); err != nil {
		return err
	}
	return ctx.Emit(ctx.State().Sum)
}
```

`Run` returning `nil` completes the item. Any other error is retried up the channel's retry
ladder; an error wrapped by `flezha.NonRetryable` parks at once; a panic is recovered and retried
as an error carrying its text, and the runner keeps serving. A value leaves only through
`ctx.Emit` (`flezha.WithKey`, `flezha.WithChannel`, `flezha.WithMetadata` qualify it); a failed
emit returns its error, and returning it fails the item.

A hook is declared by implementing it, with the tasklet's own types: `InitState(cellKey *string) S`
(the zero value of `S` otherwise), `Changes`, `OnTimer`, `EventTime` (which makes the tasklet's
clock event time), `CellsFor` (a static key set's router), `Compensate`, `MigrateState` and
`MigrateChange(fromVersion, name, value)`. A tasklet that does not implement a hook declares none.

A relay is a type with a `RelaySpec` and its channels. Every step that changes the element type is
a package function over typed streams (`Input`, `Map`, `FlatMap`, `Into`, `NewMerge` / `From` /
`MergeInto`, `RouteLatest`, `Call`), so a stream wired into a tasklet that cannot take its values
fails `go build`:

```go
type Orders struct{}

func (Orders) Spec() flezha.RelaySpec { return flezha.RelaySpec{FactoryID: "orders"} }

func (Orders) Channels(c *flezha.ChannelBuilder) {
	orders := flezha.Input[Order](c, "orders").KeyBy(func(o Order) string { return o.Pair })
	flezha.Into(orders, Totals{}).Output("totals")
}
```

The compiler names both sides of a wrong edge: the stream's element type and the tasklet that
cannot take it (`sdk/go/testdata/wrong_edge` pins the message). What static types cannot see is
judged by the core at registration, under the same codes every SDK reports, and
`RegisterFactory` returns it as a `*flezha.Error` a caller reads with `errors.As`.

`RelaySpec` carries the settings tree as data: `Run: "steps"`,
`Run: map[string]any{"steps": map[string]any{"each": "per_input"}}`, `Durable: false`. A channel
reads JSON into its element type unless `InputOptions` names `flezha.Text` or `flezha.Bytes`; an
output sends a string or a `[]byte` as it stands and any other value as JSON, unless it names a
codec or `OutputEncoded` gives its own encoder.

The short form is a function returning `(O, bool)`: it emits the value when the boolean is true and
nothing when it is false. Its tasklet is named after the factory id. `ShortTasklet` is the same for a
tasklet type, whose hooks it keeps:

```go
upcase := flezha.ShortRelay("upcase", func(in string, _ *flezha.Ctx[flezha.NoState, string]) (string, bool) {
	return strings.ToUpper(in), in != ""
}, flezha.ShortOptions{Codec: flezha.Text, Output: "out"})
```

A program serves its relay types with one runner:

```go
runner, err := flezha.CreateRunner("trading")
if err != nil {
	return err
}
if err := runner.RegisterFactory(Orders{}); err != nil {
	return err
}
return runner.Run()
```

`CreateRunner` checks the linked core first: a core of another activation schema is refused,
naming both versions, before anything connects. `Start` and `AwaitTermination` split `Run`,
`Close` shuts the runner down, `Send` and `Call` reach another relay through the controller, and
`Control` administers the namespace the connection proved. `flezha.StartLocal` runs one relay
type in this process with no controller.

## State and changes

State and changes are encoded with `encoding/json` and stamped with the Go SDK's language byte, so
no other SDK's records are ever handed to this codec. A tasklet lists its change types under
explicit names in `Changes`, and a change record is `{"c": <name>, "v": <value>}`: the record
carries the listed name, never the Go type's, so renaming a change type keeps every saved record
readable. Recording a change whose type is not listed is refused at the call with
`runtime.change_not_registered`, naming the type and the tasklet.

**Enforcement level.** `ctx.State()` returns a copy of the cell's state, so assigning a field of it
changes nothing durable. Maps, slices and pointers inside the state are shared with the live state
and are not guarded: a tasklet that writes through them without a change loses that write on the
next restore. The detection offered is the test harness's replay check (`ReplayEquals`), which
folds the recorded changes into a fresh state and compares it with the live one.

## Concurrency

A runner is a pool of poll goroutines, each looping poll, dispatch, complete, and one supervisor
answering the core's pool governor. The base is `FLEZHA_WORKER_POOL_SIZE` (default: the host's core
count). When every poller is held inside a tasklet with work queued, the governor answers a spawn
and the pool grows, up to `FLEZHA_WORKER_POOL_MAX` (default ten times the base); a grown poller that
finds no work for the governor's retire time leaves. The supervisor cannot be configured away: a
completion blocked on a full `DropNothing` consumer is released only by a new poller draining it,
which a pool of one recovers from too.

The arithmetic: one poller serves about `1 / body latency` activations a second, and in-flight
concurrency is the poller count. Per-cell ordering is the core's: a cell never has two activations
in flight, and different cells run on different pollers at once. No goroutine is locked to its
thread; a blocking core call holds its OS thread, and the Go scheduler starts another for the rest.

Every buffer the core hands out is copied into Go memory and freed before dispatch, so no Go value
points into core memory; the SDK's tests run its pool under the race detector with
`GODEBUG=cgocheck=1`.

## Signals and shutdown

The Go runtime owns signal handling, and a foreign handler installed without `SA_ONSTACK` crashes
it, so the runner calls `flz_core_host_owns_signals` before it starts: the core installs no handler,
the runner receives SIGTERM and SIGINT through `os/signal`, and the first one is handed to the core
through `flz_core_terminate`, which drains within the core's grace and exits the process as it does
for every SDK. When standard input is not a terminal, the core also watches it, so a harness that
closes the runner's standard input stops it the same way. A pool still inside a tasklet after the
drain bound is left behind, and the context is not freed under it.

## Testing a tasklet

`flezhatest.New` drives one cell of a tasklet through the SDK's own dispatch path, with no
controller: `Feed` an input (with `replay` to replay the next live input, which is served the
effects it kept), read `State`, `FireTimer` on demand, `Compensate`, `Restore` from a snapshot and a
change log, and `ReplayEquals`:

```go
h, err := flezhatest.New[Order, Book, int](Totals{})
if err != nil {
	return err
}
out, err := h.Feed(Order{Pair: "EUR/USD", Qty: 4}, 0, false)
// out.Outputs is [4], h.State().Sum is 4, and h.ReplayEquals() is nil.
```

The code on this page is compiled and run as the examples in `sdk/go/example_test.go`.

## Size

Most of a runner is the core. Measured on 2026-10-22, x86_64 Linux, the release test apps of this
tree, stripped: the Go runner is 11.9 MB (11 933 056 bytes) and the Rust runner 12.5 MB (12 462 744
bytes), both one executable that loads no core library. Shrinking the core is its
own change for every SDK.

## Fixtures

The manifest at `test/apps/go_runner/conformance.json` defers nothing: every capability is carried
with the reference's fixtures, `local_send` included. The app is `sdk/go/test_app`: the flow
fixtures in `fixtures_core.go`, `fixtures_probe.go` and `fixtures_parts.go`, persistence, schema
evolution and effectively-once in `fixtures_state.go`, the steps relays in `fixtures_steps.go` and
`fixtures_steps_loop.go`, calls in `fixtures_call.go`, the reference topologies in `conformance.go`
and the local-send drill in `local_send.go`.

## Diagnostics

The SDK logs through `log/slog` at `FLEZHA_LOG_LEVEL` (`debug` | `info` | `warn` | `error`, default
`warn`) to standard error, unless the application hands it a logger with `flezha.WithLogger`. The
core's notices are drained into the same logger at their own level, on every supervisor answer and
at start and close, so nothing reaches standard error past the application's logger.
