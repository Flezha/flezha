<?php

declare(strict_types=1);

namespace Flezha\Examples\Starter;

use Flezha\Attribute\Relay;
use Flezha\Graph\Builder;

/**
 * A visit arrives on `hit`, goes to the cell of its page, and the count leaves
 * on the output `counts`, which also answers a caller. The description is shown
 * by `flz relay info` and to AI agents.
 */
#[Relay(factoryId: 'starter')] // the name `flz relay start` takes
final class StarterRelay extends \Flezha\Relay
{
    private const HIT = [
        'type' => 'object',
        'properties' => ['page' => ['type' => 'string']],
        'required' => ['page'],
    ];

    public static function channels(Builder $c): void
    {
        $c->input('hit', description: 'Counts visits per page', schema: self::HIT)
            ->keyBy(fn (array $hit) => $hit['page']) // one cell, and one count, per page
            ->into(CountHits::class)
            ->output('counts');
    }
}
