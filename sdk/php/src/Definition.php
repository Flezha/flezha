<?php

declare(strict_types=1);

namespace Flezha;

use Flezha\Attribute\Proc;
use Flezha\Codec\Json;
use Flezha\Codec\TypeCodec;
use Flezha\Codec\ValueCodec;
use Flezha\Exception\DeclarationException;
use ReflectionAttribute;
use ReflectionClass;
use ReflectionMethod;

/**
 * What a declared procedure resolves to: the scalar declarations, and the one
 * instance whose methods the dispatcher calls.
 *
 * Procedures hold no state of their own — an entity's state lives in the
 * registry and is handed to each invocation — so one instance per class is
 * enough and the dispatcher never allocates per activation.
 *
 * Which optional hooks a procedure declares is read from the methods it
 * overrides, not from a second list: `applyChange` overridden means it folds
 * changes, `onTimer` overridden means it may register timers. A declaration
 * and its implementation therefore cannot disagree.
 */
final class Definition
{
    /** @var array<class-string, self> */
    private static array $cache = [];

    public readonly string $name;
    public readonly Proc $decl;
    public readonly Procedure $instance;
    public readonly ?ValueCodec $stateCodec;
    public readonly ?ValueCodec $changeCodec;

    /** @var array<string, bool> */
    private readonly array $overrides;

    /** @param class-string<Procedure> $class */
    private function __construct(public readonly string $class)
    {
        $reflection = new ReflectionClass($class);
        $this->decl = self::declarationOf($reflection);
        $this->name = $this->decl->name ?? self::defaultName($reflection->getShortName());
        $this->overrides = self::overrides($reflection);
        $this->stateCodec = $this->decl->stateType === null
            ? null
            : new TypeCodec($this->decl->stateType, 'state');
        $this->changeCodec = $this->decl->changeType === null
            ? null
            : new TypeCodec($this->decl->changeType, 'change');

        $this->refuseContradictions();
        $this->instance = new $class();

        // A state that cannot be encoded cannot be restored, and the first
        // anyone would learn of it is a restart. Walk the declared starting
        // state now.
        if ($this->declares('state') && $this->stateCodec === null) {
            Json::assertRepresentable("$this->name.state", $this->instance->state(null));
        }
    }

    /**
     * The resolved declaration, built once per class: it instantiates the
     * procedure, and the dispatcher reads it on the hot path.
     *
     * @param class-string<Procedure> $class
     */
    public static function of(string $class): self
    {
        return self::$cache[$class] ??= new self($class);
    }

    /** `single` | `static` | `dynamic`, as the core's declaration spells it. */
    public function entityMode(): string
    {
        if ($this->decl->entityKeys !== null) {
            return 'static';
        }

        return $this->decl->dynamicEntities ? 'dynamic' : 'single';
    }

    /** Whether the procedure overrides one of the base class's hooks. */
    public function declares(string $method): bool
    {
        return $this->overrides[$method] ?? false;
    }

    public function initialState(?string $entityKey): mixed
    {
        return $this->declares('state') ? $this->instance->state($entityKey) : null;
    }

    /**
     * Encode a durable State or Change: the type this procedure declared, or
     * the JSON default.
     *
     * The choice lives here, once, because it is asked at four places — a
     * recorded change, a snapshot, a live read, a migration handover — and a
     * class encoded one way live and another on replay is the failure the
     * whole codec exists to prevent.
     */
    public function encode(string $which, mixed $value): string
    {
        $codec = $this->codecFor($which);

        return $codec !== null
            ? $codec->encode($value)
            : Json::guard($this->name, static fn (): string => Json::encode($value));
    }

    /** The inverse. `''` decodes to null, as an absent body does. */
    public function decode(string $which, string $bytes): mixed
    {
        $codec = $this->codecFor($which);

        return $codec !== null ? $codec->decode($bytes) : Json::decode($bytes);
    }

    /** @return ValueCodec|null the declared codec for `state` or `change` */
    public function codecFor(string $which): ?ValueCodec
    {
        return $which === 'state' ? $this->stateCodec : $this->changeCodec;
    }

    public function foldChange(mixed &$state, mixed $change): void
    {
        $this->instance->applyChange($state, $change);
    }

    /** @return list<string> */
    public function entitiesFor(mixed $input): array
    {
        return array_map(strval(...), $this->instance->entitiesFor($input));
    }

    public function eventTimeOf(mixed $input): int
    {
        return $this->instance->eventTime($input);
    }

    /**
     * The declaration a class carries, merged **field by field** up its
     * hierarchy.
     *
     * PHP does not inherit attributes, and taking that literally is a trap:
     * `class Sub extends Base {}` would silently come out single-entity with
     * the default mailbox capacity and schema version 1, and nothing would
     * report the difference until the topology was wrong at runtime.
     *
     * Field by field rather than nearest-attribute-wins, because the common
     * case is a subclass that renames the node and changes nothing else —
     * `#[Proc(name: 'stress_b')]` on a child of a static-keyed parent — and
     * whole-attribute inheritance would silently drop the key set exactly
     * there. The one field never inherited is `name`: a node identity is per
     * class, and an inherited one would make two procedures claim one node.
     *
     * @param ReflectionClass<Procedure> $reflection
     */
    private static function declarationOf(ReflectionClass $reflection): Proc
    {
        $chain = [];
        for ($class = $reflection; $class !== false; $class = $class->getParentClass()) {
            $chain[] = $class;
        }

        $arguments = [];
        // Ancestor first, so a derived class's own argument wins.
        foreach (array_reverse($chain) as $class) {
            $attributes = $class->getAttributes(Proc::class);
            if ($attributes === []) {
                continue;
            }
            $own = self::namedArguments($attributes[0]);
            if ($class->getName() !== $reflection->getName()) {
                unset($own['name']);
            }
            $arguments = [...$arguments, ...$own];
        }

        return new Proc(...$arguments);
    }

    /**
     * One attribute's written arguments, keyed by parameter name — positional
     * ones included, so an author who writes them that way is merged the same.
     *
     * @param ReflectionAttribute<Proc> $attribute
     * @return array<string, mixed>
     */
    private static function namedArguments(ReflectionAttribute $attribute): array
    {
        $parameters = (new ReflectionClass(Proc::class))->getConstructor()?->getParameters() ?? [];
        $named = [];
        foreach ($attribute->getArguments() as $key => $value) {
            $name = is_int($key) ? ($parameters[$key]?->getName() ?? (string) $key) : $key;
            $named[$name] = $value;
        }

        return $named;
    }

    /**
     * Which of the base class's overridable hooks this class actually
     * replaced. Reflection once, at registration, so the dispatcher asks an
     * array.
     *
     * @param ReflectionClass<Procedure> $reflection
     * @return array<string, bool>
     */
    private static function overrides(ReflectionClass $reflection): array
    {
        $overrides = [];
        $hooks = [
            'state', 'applyChange', 'onTimer', 'entitiesFor', 'eventTime', 'compensate',
            'migrateState', 'migrateChange',
        ];
        foreach ($hooks as $hook) {
            $method = new ReflectionMethod($reflection->getName(), $hook);
            $overrides[$hook] = $method->getDeclaringClass()->getName() !== Procedure::class;
        }

        return $overrides;
    }

    /**
     * The declarations that contradict each other, refused where they are
     * written rather than at the far end of the graph.
     */
    private function refuseContradictions(): void
    {
        if ($this->decl->entityKeys !== null && $this->decl->dynamicEntities) {
            throw new DeclarationException(
                $this->class,
                $this->name,
                'declares both a static key set and dynamic entities; the two modes are exclusive'
            );
        }
        if ($this->decl->entityKeys === []) {
            throw new DeclarationException(
                $this->class,
                $this->name,
                'declares an empty static key set; a static procedure names its entities'
            );
        }
        // State without a fold is legitimate: a value seeded at start and read
        // but never mutated has nothing to journal, and `Ctx::change()` already
        // refuses at the point of use. A fold with no state to fold into is not.
        if (!$this->declares('state') && $this->declares('applyChange')) {
            throw new DeclarationException(
                $this->class,
                $this->name,
                'declares applyChange but no state; there is nothing for a change to fold into'
            );
        }
    }

    /** The class's short name in snake_case, which is what an author would type. */
    private static function defaultName(string $shortName): string
    {
        return strtolower((string) preg_replace(
            ['/([A-Z]+)([A-Z][a-z])/', '/([a-z\d])([A-Z])/'],
            ['$1_$2', '$1_$2'],
            $shortName,
        ));
    }
}
