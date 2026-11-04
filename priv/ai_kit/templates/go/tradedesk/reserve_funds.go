package main

import (
	"fmt"

	flezha "github.com/flezha/flezha_go_sdk"
)

// Reserved is what it hands on.
type Reserved struct {
	Order         OrderTicket `json:"order"`
	ReservationID string      `json:"reservation_id"`
}

// Reservation is what this step remembers.
type Reservation struct {
	ID string `json:"id"`
}

// Reserving is how the id is written down.
type Reserving struct {
	ID string `json:"id"`
}

func (r Reserving) ApplyTo(reservation *Reservation) { reservation.ID = r.ID }

// ReserveFunds is step one: put the money for the order aside, and release it
// again if the order is undone.
//
// The reservation id comes from ctx.StableID(), and the step both remembers it
// and hands it on.
type ReserveFunds struct{}

func (ReserveFunds) Spec() flezha.Spec { return flezha.Spec{Name: "reserve_funds"} }

func (ReserveFunds) Changes() flezha.Changes[Reservation] {
	return flezha.Changes[Reservation]{"reserving": Reserving{}}
}

// Run is the step: reserve the money under an id, remember the id, and hand
// the order on.
func (ReserveFunds) Run(order OrderTicket, ctx *flezha.Ctx[Reservation, Reserved]) error {
	id := "res-" + ctx.StableID() // the same id when the step runs again
	if err := ctx.Change(Reserving{ID: id}); err != nil {
		return err
	}
	return ctx.Emit(Reserved{Order: order, ReservationID: id})
}

// Compensate is the undo: release the money, by the id the state kept.
func (ReserveFunds) Compensate(_ []Reserved, ctx *flezha.Ctx[Reservation, Reserved]) error {
	fmt.Println("[undo] released", ctx.State().ID)
	return nil
}
