<?php

declare(strict_types=1);

namespace Flezha\Examples\Starter\Tests;

use Flezha\Examples\Starter\CountHits;
use Flezha\Testing\TaskletTestHarness;
use PHPUnit\Framework\TestCase;

/** Runs the tasklet with no controller: one harness per page stands in for that page's cell. */
final class CountHitsTest extends TestCase
{
    public function testCountsEachPageOnItsOwnAndSurvivesARestart(): void
    {
        $cells = [];
        $totals = [];
        foreach (['home', 'home', 'about', 'home'] as $page) {
            $cells[$page] ??= new TaskletTestHarness(CountHits::class, cellKey: $page);
            $totals[] = $cells[$page]->feed(['page' => $page])->outputs[0]['total'];
        }

        self::assertSame([1, 2, 1, 3], $totals);
        // Replaying the recorded changes rebuilds the state each cell had, as a
        // restart does.
        foreach ($cells as $cell) {
            $cell->replayEquals();
        }
    }
}
