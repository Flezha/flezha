package main

import flezha "github.com/flezha/flezha_go_sdk"

const deadlineMs = 60_000

// GatewayRelay is the HTTP side of the desk: a flow relay that carries the
// HTTP plugin, answers every request, and calls the session a message is for.
type GatewayRelay struct{}

func (GatewayRelay) Spec() flezha.RelaySpec {
	return flezha.RelaySpec{
		FactoryID: "gateway",
		// each request and answer on disk before it runs
		Run: map[string]any{"flow": map[string]any{"exactly": "effectively_once"}},
	}
}

func (GatewayRelay) Channels(c *flezha.ChannelBuilder) {
	byUser := func(event Event) string { return event.User }

	// every request arrives on "http"; kept as a handle to split it
	http := flezha.Input[Request](c, "http")
	// route 1: GET .../events gets the session's stream
	listens := http.Filter(func(request Request) bool { return !request.Sends() })
	flezha.Into(listens, Watch{}).
		Keyed().
		Output("") // a stream, or 404

	// route 2: POST .../messages is checked against the user's budget
	sends := flezha.Map(http.Filter(Request.Sends), Sent).
		KeyBy(byUser) // one cell per user
	verdicts := flezha.Into(sends, Budget{})

	// a verdict: the message plus its user's tokens today; used twice
	// use 1: answer the POST
	flezha.Into(verdicts, Admit{}).
		Keyed().
		Output("") // 202, or 429 over the budget

	// use 2: pass an admitted message on to the user's session
	admitted := flezha.Map(verdicts.Filter(Verdict.Admitted), func(v Verdict) Message { return v.Message }).
		KeyBy(func(message Message) string { return message.User }) // the user's session
	// call the "agent" relay; its answer comes back here
	answers := flezha.Call[Message, Answer](admitted, flezha.CallSpec{
		Factory: "agent", Channel: "message", DeadlineMs: deadlineMs,
	})
	// the session's answer, as tokens used
	used := flezha.Map(answers, Used).KeyBy(byUser)
	flezha.Into(used, Budget{}) // back into the same budget
}
