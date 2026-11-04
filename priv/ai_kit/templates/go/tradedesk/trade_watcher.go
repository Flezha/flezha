package main

import flezha "github.com/flezha/flezha_go_sdk"

// TradeMessage is one message from the exchange: Channel says what it is, and
// a trade message carries one or more trades in Data. The exchange's other
// messages, such as a subscription's confirmation, have neither field, so both
// stay empty rather than failing to decode. Fields the struct does not name are
// ignored.
type TradeMessage struct {
	Channel string  `json:"channel"`
	Data    []Trade `json:"data"`
}

// TradeWatcherRelay is the desk's stream: trades in, each price compared with
// its symbol's recent average, and an order started when a price jumps far
// enough above it.
//
// A flow relay runs for as long as the desk does. The exchange sends other
// messages beside its trades and delivers several trades in one message, so
// the wiring keeps the trade messages, splits each into its trades before
// anything else sees them, drops what the desk does not follow, and routes the
// rest to a cell per symbol.
type TradeWatcherRelay struct{}

func (TradeWatcherRelay) Spec() flezha.RelaySpec {
	return flezha.RelaySpec{FactoryID: "trade_watcher"}
}

// Channels wires four transforms, then two tasklets, and one place output
// leaves the desk.
//
// The transforms run at the routing seam rather than as tasklets of their
// own: no cell, no mailbox, no dispatch hop, which is why they must stay cheap
// and free of side effects. KeyBy names the cell each trade belongs to, and has
// to be the last link before Into, because it is evaluated on whatever the
// transforms above it produced. Only the first stage is keyed: a plain Into
// leaves its consumer single-cell, which is right for a stage that keeps
// nothing per symbol.
func (TradeWatcherRelay) Channels(c *flezha.ChannelBuilder) {
	messages := flezha.Input[TradeMessage](c, "trades")                                   // JSON from the exchange
	messages = messages.Filter(func(m TradeMessage) bool { return m.Channel == "trade" }) // trade messages only
	trades := flezha.FlatMap(messages, func(m TradeMessage) []Trade { return m.Data })    // a message into its trades
	trades = trades.Filter(func(t Trade) bool { return t.OrdType == "market" })           // market orders only
	trades = trades.KeyBy(func(t Trade) string { return t.Symbol })                       // one cell per symbol
	moves := flezha.Into(trades, TrackPrices{})
	signals := flezha.Into(moves, BuyAboveAverage{}) // fed by the tasklet above
	signals.Output("signals")                        // out to the file plugin
}
