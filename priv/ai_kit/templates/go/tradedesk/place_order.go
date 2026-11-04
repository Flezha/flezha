package main

import (
	"fmt"

	flezha "github.com/flezha/flezha_go_sdk"
)

// PlacedOrder is what it hands on.
type PlacedOrder struct {
	Order   OrderTicket `json:"order"`
	OrderID string      `json:"order_id"`
}

// Placement is what this step remembers.
type Placement struct {
	OrderID string `json:"order_id"`
}

// Placing is how the id is written down.
type Placing struct {
	OrderID string `json:"order_id"`
}

func (p Placing) ApplyTo(placement *Placement) { placement.OrderID = p.OrderID }

// PlaceOrder is step two: place the order with the paper exchange, keeping the
// id it answers with, and cancel the order again if it is undone. It has the
// same shape as step one.
type PlaceOrder struct{}

func (PlaceOrder) Spec() flezha.Spec { return flezha.Spec{Name: "place_order"} }

func (PlaceOrder) Changes() flezha.Changes[Placement] {
	return flezha.Changes[Placement]{"placing": Placing{}}
}

// Run is the step: place the order under an id, remember the id, and hand the
// order on.
func (PlaceOrder) Run(reserved Reserved, ctx *flezha.Ctx[Placement, PlacedOrder]) error {
	orderID := "ord-" + ctx.StableID()
	if err := ctx.Change(Placing{OrderID: orderID}); err != nil {
		return err
	}
	return ctx.Emit(PlacedOrder{Order: reserved.Order, OrderID: orderID})
}

// Compensate is the undo: cancel the order, by the id the state kept.
func (PlaceOrder) Compensate(_ []PlacedOrder, ctx *flezha.Ctx[Placement, PlacedOrder]) error {
	fmt.Println("[undo] cancelled", ctx.State().OrderID)
	return nil
}
