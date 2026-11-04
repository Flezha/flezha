// Starts the desk's runner: the process that connects to the controller and
// serves agentdesk's relays, in the agents namespace.
package main

import (
	"log"

	flezha "github.com/flezha/flezha_go_sdk"
)

func main() {
	runner, err := flezha.CreateRunner("agents")
	if err != nil {
		log.Fatal(err)
	}
	for _, relay := range []flezha.Relay{AgentRelay{}, GatewayRelay{}} {
		if err := runner.RegisterFactory(relay); err != nil {
			log.Fatal(err)
		}
	}
	if err := runner.Run(); err != nil {
		log.Fatal(err)
	}
}
