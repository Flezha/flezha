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
