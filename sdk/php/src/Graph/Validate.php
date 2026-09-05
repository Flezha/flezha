<?php

declare(strict_types=1);

namespace Flezha\Graph;

use Flezha\Clock;
use Flezha\Codec\Json;
use Flezha\Exception\DeclarationException;
use Flezha\Exception\FlezhaException;
use Flezha\Ffi\Core;

/**
 * The verdict on one declaration, and the two defects a record cannot carry.
 *
 * The topology is judged in ONE place, `flz_core::host::register::check`: this
 * class builds the record, submits it through `flz_core_check_class`, and
 * renders the core's verdict as this package's own refusal. What is refused
 * here instead is what the record cannot state — whether a PHP class defines
 * the method its declaration promises, and whether it supplies the migration a
 * schema floor needs. The core cannot see either.
 */
final class Validate
{
    private function __construct()
    {
    }

    /**
     * Judge one declaration and answer what the core derived from it: the
     * channel names, kinds, consumers, sink names and acknowledgement
     * granularity it read off the record.
     *
     * The defects this package can see are refused first, so an author reads
     * "no eventTime method" rather than a topology verdict about a clock the
     * class does declare.
     *
     * @return array<string, mixed>
     */
    public static function spec(ClassSpec $spec, Core $core): array
    {
        self::procedures($spec);

        return self::submit($spec, $core);
    }

    /**
     * The record, over the ABI, and the verdict back.
     *
     * @return array<string, mixed>
     */
    private static function submit(ClassSpec $spec, Core $core): array
    {
        $verdict = null;
        try {
            $derived = $core->checkClass(Json::encode(HostClassDecl::of($spec)), $verdict);
        } catch (FlezhaException $e) {
            throw new DeclarationException($spec->factoryId, null, $e->getMessage(), self::refusals($verdict));
        }

        /** @var array<string, mixed> $decoded */
        $decoded = Json::decode($derived);
        self::bindEdges($spec, $decoded);

        return $decoded;
    }

    /**
     * The core's own names for what it refused, read off the verdict it hands
     * back as data beside the rendered message. Empty when the failing call
     * carried no verdict — a refusal about the context rather than a judgement
     * on the record.
     *
     * @return list<string>
     */
    private static function refusals(?string $verdict): array
    {
        if ($verdict === null) {
            return [];
        }

        /** @var array{issues?: list<array{name: string, text: string}>} $decoded */
        $decoded = Json::decode($verdict);

        return array_map(static fn (array $issue): string => $issue['name'], $decoded['issues'] ?? []);
    }

    /**
     * The consumers the CORE derived, each carrying the closures of the chain
     * that produced it: the builder held them aside per chain, and the derived
     * edge names the chain. Which edge survives a re-declaration is therefore
     * the core's answer, not a rule this package repeats.
     *
     * @param array<string, mixed> $derived
     */
    private static function bindEdges(ClassSpec $spec, array $derived): void
    {
        /** @var list<array{index: int, consumers: list<array{procedure: int, chain: int}>}> $channels */
        $channels = $derived['channels'];
        foreach ($channels as $channel) {
            $target = $spec->channels[$channel['index']];
            $consumers = [];
            foreach ($channel['consumers'] as $edge) {
                $consumers[] = $target->chainEdges[$edge['chain']];
            }
            $target->consumers = $consumers;
        }
    }

    private static function refuse(ClassSpec $spec, ?string $handler, string $defect): never
    {
        throw new DeclarationException($spec->factoryId, $handler, $defect);
    }

    private static function procedures(ClassSpec $spec): void
    {
        foreach ($spec->procedures as $node) {
            self::clocks($spec, $node);
            self::schema($spec, $node);
        }
    }

    /**
     * A clock is a promise about a method, and a record carries the clock but
     * not the class: only this package can see that the method is missing.
     */
    private static function clocks(ClassSpec $spec, ProcNode $node): void
    {
        $definition = $node->definition;
        $clock = $definition->decl->clock;

        if ($clock === Clock::EventTime && !$definition->declares('eventTime')) {
            self::refuse(
                $spec,
                $node->name,
                'declares an event_time clock but no eventTime method; the core evaluates '
                . 'the declared clock per input and has nothing to evaluate'
            );
        }
        if ($clock !== Clock::EventTime && $definition->declares('eventTime')) {
            self::refuse($spec, $node->name, sprintf(
                'declares an eventTime method with clock `%s`; the extractor is only '
                . 'consulted by an event_time clock, so one of the two is not what was meant',
                $clock->value,
            ));
        }
    }

    /**
     * A schema floor promises a migration, and the migration is a method: the
     * record carries the versions and this package carries the class.
     */
    private static function schema(ClassSpec $spec, ProcNode $node): void
    {
        $definition = $node->definition;
        $decl = $definition->decl;
        $floor = $decl->minReadableSchemaVersion;
        if ($floor === null) {
            return;
        }

        if ($floor > $decl->schemaVersion) {
            self::refuse($spec, $node->name, sprintf(
                'declares minReadableSchemaVersion %d above its schemaVersion %d',
                $floor,
                $decl->schemaVersion,
            ));
        }
        if ($floor < $decl->schemaVersion
            && !$definition->declares('migrateState')
            && !$definition->declares('migrateChange')) {
            self::refuse($spec, $node->name, sprintf(
                'declares it reads back to schema version %d but supplies no migration; a '
                . 'floor without one fails at the first old record instead of at startup',
                $floor,
            ));
        }
    }
}
