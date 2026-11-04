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
