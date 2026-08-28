<?php

declare(strict_types=1);

// PHP CS Fixer is the PHP SDK's formatter; pinned in sdk/php/composer.json.
$finder = PhpCsFixer\Finder::create()->in([
    __DIR__ . '/sdk/php/src',
    __DIR__ . '/sdk/php/tests',
    __DIR__ . '/sdk/php/examples',
    __DIR__ . '/test/apps/php_runner',
]);

return (new PhpCsFixer\Config())
    ->setRules(['@PSR12' => true])
    ->setUsingCache(false)
    ->setFinder($finder);
