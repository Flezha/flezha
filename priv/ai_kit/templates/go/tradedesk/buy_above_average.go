package main

import (
	"fmt"
	"math"

	flezha "github.com/flezha/flezha_go_sdk"
)

const (
	buyAbovePct     = 2.0
	unitsPerPercent = 10.0
)

// BuyAboveAverage buys when a price is far enough above its symbol's average.
//
// It remembers nothing: the move it is handed already carries the average, so
// the decision is a function of one value, and a restart costs this stage
// nothing because there is nothing to bring back. That is why its state is
// flezha.NoState, beside TrackPrices, which keeps the window.
//
// It does two things per input, both through the context: it starts a steps
// relay and it emits.
type BuyAboveAverage struct{}

func (BuyAboveAverage) Spec() flezha.Spec { return flezha.Spec{Name: "buy_above_average"} }

func (BuyAboveAverage) Run(m Move, ctx *flezha.Ctx[flezha.NoState, string]) error {
	percent := (m.Price - m.Average) / m.Average * 100.0
	if percent < buyAbovePct {
		return nil
	}
	quantity := int(math.Round(percent * unitsPerPercent)) // a stronger move buys more

	ticket := OrderTicket{Symbol: m.Symbol, Quantity: quantity}
	if _, err := ctx.SpawnSteps("order_executor", ticket); err != nil { // one steps relay per order
		return err
	}

	return ctx.Emit(fmt.Sprintf("%s %.4f is %.2f%% above its average of %.4f, buying %d",
		m.Symbol, m.Price, percent, m.Average, quantity))
}
