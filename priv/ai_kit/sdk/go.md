# The go SDK's echo example

Every file of `sdk/go/examples/echo`, the runner Up and running builds. Its dependency file names the SDK package and the version this release pins.

## examples/echo/README.md

````markdown
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
````

## examples/echo/go.mod

````
module echo

go 1.26

require github.com/flezha/flezha_go_sdk v0.1.1
````

## examples/echo/main.go

````go
// The runner: a process that connects to the controller and serves your
// relays. Register every relay you want served before Start; one runner can
// serve several, and the same call takes a steps relay.
package main

import (
	"log"
	"os"

	flezha "github.com/flezha/flezha_go_sdk"
)

func main() {
	namespace := "default"
	if len(os.Args) > 1 {
		namespace = os.Args[1]
	}

	runner, err := flezha.CreateRunner(namespace)
	if err != nil {
		log.Fatal(err)
	}
	if err := runner.RegisterFactory(EchoRelay{}); err != nil { // the relays this runner serves
		log.Fatal(err)
	}
	// Run is Start followed by AwaitTermination. Splitting them leaves room for
	// the calls below, and still blocks until shutdown.
	if err := runner.Start(); err != nil { // connects to the controller
		log.Fatal(err)
	}

	// Everything `flz relay` and `flz plugin` do is also a method of Control,
	// and Send hands a message to a relay. Uncomment to drive this namespace
	// from the application instead of from a terminal:
	//
	// control := runner.Control()
	// if _, err := control.StartRelay("echo", "my_echo", nil, nil); err != nil {
	// 	log.Fatal(err)
	// }
	// _, err = control.EnablePlugin("writer", "relay_file_output",
	// 	map[string]any{"path": "echo-out.txt"}, false, "", nil)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// if _, err := control.BindPlugin("writer", "my_echo", []any{[]any{"on_output", map[string]any{}}}, ""); err != nil {
	// 	log.Fatal(err)
	// }
	// if _, err := runner.Send("my_echo", "in", []byte("hello world"), flezha.MsgID("hello-1")); err != nil {
	// 	log.Fatal(err)
	// }
	//
	// docs/public/topics/core/sending_and_calling.md covers sending. An
	// enforced controller refuses all of it until
	// `flz role bind runner@default app default`.

	if err := runner.AwaitTermination(); err != nil {
		log.Fatal(err)
	}
}
````

## examples/echo/relay.go

````go
// The relay: the deployable unit, the entry channel a message arrives on, the
// tasklet it passes through and the channel its result leaves on.
package main

import flezha "github.com/flezha/flezha_go_sdk"

// EchoRelay routes the channel "in" through Echo and out on "out".
type EchoRelay struct{}

func (EchoRelay) Spec() flezha.RelaySpec {
	return flezha.RelaySpec{FactoryID: "echo"} // the name `flz relay start` takes
}

func (EchoRelay) Channels(c *flezha.ChannelBuilder) {
	// messages arrive on the entry channel "in"
	in := flezha.Input[string](c, "in", flezha.InputOptions{Codec: flezha.Text})
	// each one runs through the echo tasklet; the result leaves on "out", for a
	// plugin to pick up
	flezha.Into(in, Echo{}).Output("out")
}
````

## examples/echo/tasklet.go

````go
// The tasklet: a processing stage of the relay's graph, run once per message.
// It keeps no state and emits through its context, the one way a value leaves
// a tasklet.
package main

import flezha "github.com/flezha/flezha_go_sdk"

// Echo emits "echo: " plus what it got.
type Echo struct{}

// state is saved under this name, so choose it once
func (Echo) Spec() flezha.Spec { return flezha.Spec{Name: "echo"} }

func (Echo) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit("echo: " + in) // where it goes is the relay's business
}
````
