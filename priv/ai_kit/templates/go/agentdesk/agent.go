package main

import flezha "github.com/flezha/flezha_go_sdk"

// Message is a message for a user's session, as the gateway sends it: the
// session is named after the user.
type Message struct {
	User string `json:"user"`
	Text string `json:"text"`
}

// AgentRelay is one conversation: a steps relay that runs one turn per
// message it is sent, for as long as the session lasts. Every session is an
// instance of its own, with its own state.
type AgentRelay struct{}

func (AgentRelay) Spec() flezha.RelaySpec {
	return flezha.RelaySpec{
		FactoryID: "agent",
		Run: map[string]any{"steps": map[string]any{
			"each":       "per_input",     // one turn per message
			"on_failure": "keep_progress", // a failed turn fails alone
		}},
		Durable: map[string]any{"local": map[string]any{
			"sleep_after": 10_000, // an idle session sleeps after 10 s
		}},
	}
}

func (AgentRelay) Channels(c *flezha.ChannelBuilder) {
	// entry 1: `flz relay call`
	turn := flezha.Input[string](c, "turn", flezha.InputOptions{Codec: flezha.Text})
	flezha.Into(turn, Reply{}).
		Output("answer") // to whoever called, and whoever listens

	// entry 2: the gateway, into the same Reply
	message := flezha.Input[Message](c, "message")
	flezha.Into(flezha.Map(message, func(m Message) string { return m.Text }), Reply{})
}
