package main

import (
	"fmt"

	flezha "github.com/flezha/flezha_go_sdk"
)

// Answer is the demo model's answer, or one piece of it.
type Answer struct {
	Text   string `json:"text"`
	Tokens int    `json:"tokens"`
}

// Conversation is what the session remembers.
type Conversation struct {
	Messages []string `json:"messages"`
}

// Said is a message said.
type Said struct {
	Message string `json:"message"`
}

func (s Said) ApplyTo(c *Conversation) { c.Messages = append(c.Messages, s.Message) }

// Unsaid is the last message taken back.
type Unsaid struct{}

func (Unsaid) ApplyTo(c *Conversation) {
	if n := len(c.Messages); n > 0 {
		c.Messages = c.Messages[:n-1]
	}
}

// Reply answers a message with the demo model, sending the answer as it
// grows, and keeps the conversation so the next answer knows what came before.
type Reply struct{}

func (Reply) Spec() flezha.Spec { return flezha.Spec{Name: "reply"} }

func (Reply) Changes() flezha.Changes[Conversation] {
	return flezha.Changes[Conversation]{"said": Said{}, "unsaid": Unsaid{}}
}

func (Reply) Run(message string, ctx *flezha.Ctx[Conversation, Answer]) error {
	// Each piece streams out as it is made; then the whole answer.
	answer, err := askDemoModel(ctx.State().Messages, message, ctx.Part)
	if err != nil {
		return err
	}
	if err := ctx.Emit(answer); err != nil {
		return err
	}
	return ctx.Change(Said{Message: message})
}

// Compensate is the undo, when a turn is taken back: the conversation forgets
// its message.
func (Reply) Compensate(_ []Answer, ctx *flezha.Ctx[Conversation, Answer]) error {
	if messages := ctx.State().Messages; len(messages) > 0 {
		fmt.Printf("[undo] forgot \"%s\"\n", messages[len(messages)-1])
	}
	return ctx.Change(Unsaid{})
}
