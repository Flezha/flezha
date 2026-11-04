package main

import (
	"fmt"
	"time"

	flezha "github.com/flezha/flezha_go_sdk"
)

const exchangeMax = 100

// ConfirmFill asks the exchange whether the order filled, on one arm of the
// fan-out. The paper exchange rejects anything above its own size, which is
// what makes this the step that fails, and takes ten seconds to answer, which
// leaves time to stop the runner in the middle of an order.
//
// It remembers nothing and has nothing to undo, so it writes only Run. It ends
// the instance with a result rather than emitting one: the order's outcome is
// the instance's own answer, which `flz relay info` reads back, so the desk
// needs no output here.
//
// A failure here is retried first, and once the retries are spent the
// instance ends abnormally. Everything that finished is then undone in
// reverse, across both arms: the order is cancelled and the money released,
// while the desk's book is skipped because it writes no Compensate.
type ConfirmFill struct{}

func (ConfirmFill) Spec() flezha.Spec { return flezha.Spec{Name: "confirm_fill"} }

func (ConfirmFill) Run(placed PlacedOrder, ctx *flezha.Ctx[flezha.NoState, flezha.NoOutput]) error {
	time.Sleep(10 * time.Second) // the paper exchange takes ten seconds to answer
	quantity := placed.Order.Quantity
	if quantity > exchangeMax {
		return fmt.Errorf("the exchange fills at most %d a time, and this is %d", exchangeMax, quantity) // tried again, then the order is undone
	}
	return ctx.Complete(fmt.Sprintf("filled %s %d as %s", placed.Order.Symbol, quantity, placed.OrderID))
}
