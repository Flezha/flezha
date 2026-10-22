package main

import (
	"slices"
	"testing"

	"github.com/flezha/flezha_go_sdk/flezhatest"
)

// Runs the tasklet with no controller: one harness per page stands in for
// that page's cell.
func TestCountsEachPageOnItsOwnAndSurvivesARestart(t *testing.T) {
	cells := map[string]*flezhatest.TaskletTestHarness[Hit, Seen, Count]{}
	var counts []Count
	for _, page := range []string{"home", "home", "about", "home"} {
		cell, ok := cells[page]
		if !ok {
			var err error
			cell, err = flezhatest.New[Hit, Seen, Count](CountHits{}, flezhatest.Options{CellKey: &page})
			if err != nil {
				t.Fatal(err)
			}
			cells[page] = cell
		}
		out, err := cell.Feed(Hit{Page: page}, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		counts = append(counts, out.Outputs...)
	}

	want := []Count{{"home", 1}, {"home", 2}, {"about", 1}, {"home", 3}}
	if !slices.Equal(counts, want) {
		t.Errorf("counts = %v, want %v", counts, want)
	}
	// Replaying the recorded changes rebuilds the state each cell had, as a
	// restart does.
	for page, cell := range cells {
		if err := cell.ReplayEquals(); err != nil {
			t.Errorf("%s: %v", page, err)
		}
	}
}
