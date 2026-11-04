package main

import flezha "github.com/flezha/flezha_go_sdk"

// OrderTicket is the order an instance is started with. The entry channel
// declares this type, so the first message is decoded into it:
// --text '{"symbol": "EUR/USD", "quantity": 10}' --channel order.
type OrderTicket struct {
	Symbol   string `json:"symbol"`
	Quantity int    `json:"quantity"`
}

// OrderExecutorRelay is the desk's steps relay: one instance per order, which
// runs its steps once and then ends.
//
// A steps relay declares its steps the way a flow relay declares its graph, on
// the same builder, and the controller schedules, restarts and relocates it in
// the same way. What differs is the lifecycle: each step's output is written
// down as it commits, so an instance that is interrupted resumes at the step
// it reached rather than from the beginning, and the instance ends by itself
// once the chain has run.
//
// When a step fails and its retries are spent, the instance ends abnormally
// instead, and the default rollback undoes every step that finished, in
// reverse, through their Compensate hooks: the order is cancelled, then the
// money is released. A step with no Compensate is skipped, which is why the
// desk's book is left alone.
type OrderExecutorRelay struct{}

// Spec names the relay's stable identity, which
// `flz relay start order_executor` resolves. It is durable, so changing it
// declares a new relay rather than renaming this one.
func (OrderExecutorRelay) Spec() flezha.RelaySpec {
	return flezha.RelaySpec{
		FactoryID: "order_executor",
		Run: map[string]any{"steps": map[string]any{
			"each":       "once",     // one instance per order; each step runs once
			"on_failure": "undo_all", // a failed step undoes the finished ones
		}},
	}
}

// Channels declares two steps in order, then a fan-out. Confirming the fill
// and booking the order are two independent consequences of the placement,
// and neither waits for the other. Each arm of Branch receives the same
// stream.
//
// The driver runs the chain level by level: every step of a level commits
// before any of its children run, so the desk's order of events is fixed
// rather than a race. A step that fails poisons its whole level, which is why
// a refusal on either arm unwinds what the other arm did.
//
// Nothing ends on an output: the last step ends the instance with its result,
// which `flz relay info` shows.
func (OrderExecutorRelay) Channels(c *flezha.ChannelBuilder) {
	order := flezha.Input[OrderTicket](c, "order") // the order to carry out
	reserved := flezha.Into(order, ReserveFunds{}) // step one
	placed := flezha.Into(reserved, PlaceOrder{})  // step two, once step one is done
	placed.Branch(
		func(fill flezha.Stream[PlacedOrder]) { flezha.Into(fill, ConfirmFill{}) }, // two arms, side by side
		func(book flezha.Stream[PlacedOrder]) { flezha.Into(book, RecordOrder{}) },
	)
}
