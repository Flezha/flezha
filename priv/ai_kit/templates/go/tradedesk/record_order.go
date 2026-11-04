package main

import (
	"fmt"

	flezha "github.com/flezha/flezha_go_sdk"
)

// RecordOrder writes the placed order into the desk's book, on its own arm of
// the fan-out.
//
// Confirming the fill and booking the order are two independent consequences
// of the same event, and neither waits for the other, which is what a branch
// is for.
//
// The plainest shape a tasklet has: a value in, nothing kept, nothing out.
// With flezha.NoOutput as its output it declares no output channel at all.
//
// It writes no Compensate on purpose. A rollback undoes what the desk did to
// the world, not what it wrote down about it: the attempt happened, and the
// book says so even when the order was cancelled again.
type RecordOrder struct{}

func (RecordOrder) Spec() flezha.Spec { return flezha.Spec{Name: "record_order"} }

func (RecordOrder) Run(placed PlacedOrder, _ *flezha.Ctx[flezha.NoState, flezha.NoOutput]) error {
	fmt.Printf("[book] %s %d as %s\n", placed.Order.Symbol, placed.Order.Quantity, placed.OrderID)
	return nil
}
