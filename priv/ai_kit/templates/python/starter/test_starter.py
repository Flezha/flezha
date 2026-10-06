"""Runs the tasklet with no controller: one harness per page stands in for that
page's cell."""

import unittest

from count import CountHits

from flezha.testing import TaskletTestHarness


class CountHitsTest(unittest.TestCase):
    def test_counts_each_page_on_its_own_and_survives_a_restart(self) -> None:
        cells: dict[str, TaskletTestHarness] = {}
        totals = []
        for page in ["home", "home", "about", "home"]:
            cell = cells.setdefault(page, TaskletTestHarness(CountHits, cell_key=page))
            totals.append(cell.feed({"page": page}).outputs[0]["total"])

        self.assertEqual(totals, [1, 2, 1, 3])
        # Replaying the recorded changes rebuilds the state each cell had, as a
        # restart does.
        for cell in cells.values():
            self.assertTrue(cell.replay_equals())


if __name__ == "__main__":
    unittest.main()
