package main

import flezha "github.com/flezha/flezha_go_sdk"

// Trade is one trade. Fields the exchange sends beside these are ignored.
type Trade struct {
	Symbol  string  `json:"symbol"`
	Price   float64 `json:"price"`
	OrdType string  `json:"ord_type"`
}

type Move struct {
	Symbol  string  `json:"symbol"`
	Price   float64 `json:"price"`
	Average float64 `json:"average"`
}

const window = 10

// PriceWindow is what each symbol's cell remembers.
type PriceWindow struct {
	Prices []float64 `json:"prices"`
}

// PriceSeen is how a price joins the window.
type PriceSeen struct {
	Price float64 `json:"price"`
}

func (s PriceSeen) ApplyTo(w *PriceWindow) {
	w.Prices = append(w.Prices, s.Price)
	if len(w.Prices) > window {
		w.Prices = w.Prices[1:]
	}
}

// TrackPrices keeps a symbol's last ten prices, and compares each new trade
// with their average.
//
// One cell per symbol, because the relay keys the channel by symbol, and the
// window is that cell's state. Every change to it is recorded as it happens,
// so a runner that is killed and started again finds the window it had rather
// than an empty one.
type TrackPrices struct{}

func (TrackPrices) Spec() flezha.Spec { return flezha.Spec{Name: "track_prices"} }

func (TrackPrices) Changes() flezha.Changes[PriceWindow] {
	return flezha.Changes[PriceWindow]{"price_seen": PriceSeen{}}
}

func (TrackPrices) Run(trade Trade, ctx *flezha.Ctx[PriceWindow, Move]) error {
	if prices := ctx.State().Prices; len(prices) > 0 { // a symbol's first trade has nothing to compare with
		if err := ctx.Emit(Move{Symbol: trade.Symbol, Price: trade.Price, Average: average(prices)}); err != nil {
			return err
		}
	}
	return ctx.Change(PriceSeen{Price: trade.Price})
}

func average(prices []float64) float64 {
	var sum float64
	for _, p := range prices {
		sum += p
	}
	return sum / float64(len(prices))
}
