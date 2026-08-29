<?php

declare(strict_types=1);

namespace Flezha;

use Flezha\Attribute\Operative as OperativeAttribute;
use Flezha\Graph\ClassSpec;

/** A stream operative: fed by the controller through its entry channels. */
abstract class StreamOperative extends Operative
{
    protected static function buildSpec(OperativeAttribute $declaration): ClassSpec
    {
        $builder = static::builder($declaration);

        return static::assemble($declaration, $builder, 'stream', $builder->syncDurability);
    }
}
