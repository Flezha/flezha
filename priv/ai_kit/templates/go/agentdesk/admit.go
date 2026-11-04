package main

import (
	"fmt"

	flezha "github.com/flezha/flezha_go_sdk"
)

// Admit answers a message 202 when it goes on to the session, and 429 when its
// user is over the budget.
type Admit struct{}

func (Admit) Spec() flezha.Spec { return flezha.Spec{Name: "admit"} }

func (Admit) Run(verdict Verdict, ctx *flezha.Ctx[flezha.NoState, HTTP]) error {
	user := verdict.Message.User
	answer := HTTP{Status: 202, Body: "sent to " + user}
	if !verdict.Admitted() {
		answer = HTTP{Status: 429, Body: fmt.Sprintf("%s has used %d of today's %d tokens", user, verdict.Spent, dailyTokens)}
	}
	return ctx.Emit(answer, flezha.WithKey(user))
}
