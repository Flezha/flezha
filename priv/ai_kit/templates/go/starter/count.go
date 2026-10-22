package main

import flezha "github.com/flezha/flezha_go_sdk"

// Hit is one visit of a page.
type Hit struct {
	Page string `json:"page"`
}

// Count is a page's total after a visit.
type Count struct {
	Page  string `json:"page"`
	Total uint64 `json:"total"`
}

// Seen is what each page's cell remembers.
type Seen struct {
	Total uint64 `json:"total"`
}

// Counted is how a visit changes it.
type Counted struct{}

func (Counted) ApplyTo(seen *Seen) { seen.Total++ }

// CountHits counts the visits of one page and answers the new count.
//
// The relay keys the channel by page, so each page has a cell of its own, and
// Seen is that cell's state. The state changes only through the Counted
// change, which is recorded as it happens, so a runner that restarts finds the
// count it had.
type CountHits struct{}

func (CountHits) Spec() flezha.Spec {
	return flezha.Spec{Name: "count"} // state is saved under this name, so choose it once
}

// Changes lists the change under the name its records carry.
func (CountHits) Changes() flezha.Changes[Seen] {
	return flezha.Changes[Seen]{"counted": Counted{}}
}

func (CountHits) Run(hit Hit, ctx *flezha.Ctx[Seen, Count]) error {
	if err := ctx.Change(Counted{}); err != nil {
		return err
	}
	return ctx.Emit(Count{Page: hit.Page, Total: ctx.State().Total})
}
