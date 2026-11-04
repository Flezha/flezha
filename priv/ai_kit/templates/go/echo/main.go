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
