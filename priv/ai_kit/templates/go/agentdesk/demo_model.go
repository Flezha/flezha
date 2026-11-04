package main

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	flezha "github.com/flezha/flezha_go_sdk"
)

const (
	maxMessage = 200
	perToken   = 50 * time.Millisecond
)

// askDemoModel is the demo model: a canned answer, one token at a time, and
// the number of tokens it used. It stands in for a real model so the example
// runs anywhere; a real model's client would be called here instead, handing
// each token it streams to part the same way.
func askDemoModel(earlier []string, message string, part func(Answer) error) (Answer, error) {
	if length := utf8.RuneCountInString(message); length > maxMessage {
		return Answer{}, flezha.NonRetryable(fmt.Errorf(
			"the demo model reads at most %d characters, and this message has %d", maxMessage, length))
	}
	text := fmt.Sprintf("Message %d: you said \"%s\". I am a demo model.", len(earlier)+1, message)
	tokens := strings.SplitAfter(text, " ")
	for i, token := range tokens {
		if err := part(Answer{Text: token, Tokens: i + 1}); err != nil {
			return Answer{}, err
		}
		time.Sleep(perToken)
	}
	return Answer{Text: text, Tokens: len(tokens)}, nil
}
