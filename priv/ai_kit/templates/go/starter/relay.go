package main

import flezha "github.com/flezha/flezha_go_sdk"

// StarterRelay is the relay: a visit arrives on hit, goes to the cell of its
// page, and the count leaves on the output counts, which also answers a
// caller.
type StarterRelay struct{}

func (StarterRelay) Spec() flezha.RelaySpec {
	return flezha.RelaySpec{FactoryID: "starter"} // the name `flz relay start` takes
}

func (StarterRelay) Channels(c *flezha.ChannelBuilder) {
	hits := flezha.Input[Hit](c, "hit", flezha.InputOptions{
		Description: "Counts visits per page", // shown by `flz relay info` and to AI agents
	}).KeyBy(func(hit Hit) string { return hit.Page }) // one cell, and one count, per page
	flezha.Into(hits, CountHits{}).Output("counts")
}
