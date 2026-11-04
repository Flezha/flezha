package main

import (
	"time"

	flezha "github.com/flezha/flezha_go_sdk"
)

const dailyTokens = 40

// Event is what reaches a user's cell: a message to admit, or the tokens an
// answer used.
type Event struct {
	User    string   `json:"user"`
	Message *Message `json:"message,omitempty"`
	Tokens  int      `json:"tokens,omitempty"`
}

// Sent is a message to admit.
func Sent(request Request) Event {
	message := request.Message()
	return Event{User: message.User, Message: &message}
}

// Used is the tokens a session's answer used; a failed turn used none.
func Used(response flezha.Response[Message, Answer]) Event {
	event := Event{User: response.Request.User}
	if response.Ok() {
		event.Tokens = response.Value.Tokens
	}
	return event
}

// Verdict is a message, and the tokens its user had used today when it came.
type Verdict struct {
	Message Message `json:"message"`
	Spent   int     `json:"spent"`
}

func (v Verdict) Admitted() bool { return v.Spent < dailyTokens }

// Spent is what each user's cell remembers: the day, in UTC, and its tokens.
type Spent struct {
	Day    string `json:"day"`
	Tokens int    `json:"tokens"`
}

func (s Spent) on(today string) int {
	if today == s.Day {
		return s.Tokens
	}
	return 0
}

// Counted is the tokens an answer used, counted on a day.
type Counted struct {
	Day    string `json:"day"`
	Tokens int    `json:"tokens"`
}

func (c Counted) ApplyTo(s *Spent) {
	s.Tokens = s.on(c.Day) + c.Tokens
	s.Day = c.Day
}

// Budget keeps the tokens a user's answers used today, and admits a message
// while they are under the day's budget. One cell per user.
type Budget struct{}

func (Budget) Spec() flezha.Spec { return flezha.Spec{Name: "budget"} }

func (Budget) Changes() flezha.Changes[Spent] {
	return flezha.Changes[Spent]{"counted": Counted{}}
}

func (Budget) Run(event Event, ctx *flezha.Ctx[Spent, Verdict]) error {
	today := time.UnixMilli(int64(ctx.Now())).UTC().Format(time.DateOnly)
	if event.Message != nil {
		// A message to admit: hand on its verdict.
		return ctx.Emit(Verdict{Message: *event.Message, Spent: ctx.State().on(today)})
	}
	// The tokens an answer used: count them.
	return ctx.Change(Counted{Day: today, Tokens: event.Tokens})
}
