// Starts the desk's runner: the process that connects to the controller and
// serves tradedesk's relays.
//
// The namespace is the first argument and defaults to "trading". A runner
// serves one namespace, and at most one runner may run per process. Under an
// enforced controller it proves its namespace with that namespace's key, read
// from FLEZHA_NAMESPACE_SECRET.
//
// The guides write the desk's relays one at a time, and each is registered
// here before Run. Registering a factory starts nothing: it offers the
// controller relays of that type, which are started from it later.
// RegisterFactory is the same call for both kinds, the stream that tracks
// prices and the steps relay that places one order.
package main

import (
	"log"
	"os"

	flezha "github.com/flezha/flezha_go_sdk"
)

func main() {
	namespace := "trading"
	if len(os.Args) > 1 {
		namespace = os.Args[1]
	}

	runner, err := flezha.CreateRunner(namespace)
	if err != nil {
		log.Fatal(err)
	}
	if err := runner.RegisterFactory(TradeWatcherRelay{}); err != nil {
		log.Fatal(err)
	}
	if err := runner.RegisterFactory(OrderExecutorRelay{}); err != nil {
		log.Fatal(err)
	}

	if err := runner.Run(); err != nil {
		log.Fatal(err)
	}
}
