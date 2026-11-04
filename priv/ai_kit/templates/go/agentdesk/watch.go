package main

import flezha "github.com/flezha/flezha_go_sdk"

// Watch answers a request for a session's events with its stream, and any
// other request with 404.
type Watch struct{}

func (Watch) Spec() flezha.Spec { return flezha.Spec{Name: "watch"} }

func (Watch) Run(request Request, ctx *flezha.Ctx[flezha.NoState, HTTP]) error {
	answer := HTTP{Status: 404, Body: "no such route"}
	if request.Listens() {
		answer = HTTP{Stream: request.User()}
	}
	// keyed by the user: an output of an effectively-once relay needs a key
	return ctx.Emit(answer, flezha.WithKey(request.User()))
}
