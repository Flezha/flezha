// The runner: connects to the controller and serves the starter relay until
// shutdown.
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
	if err := runner.RegisterFactory(StarterRelay{}); err != nil {
		log.Fatal(err)
	}
	if err := runner.Run(); err != nil {
		log.Fatal(err)
	}
}
