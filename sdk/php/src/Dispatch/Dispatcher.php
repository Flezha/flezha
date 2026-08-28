<?php

declare(strict_types=1);

namespace Flezha\Dispatch;

use Flezha\Codec\ActivationCodec;
use Flezha\Codec\Json;
use Flezha\Ctx;
use Flezha\Exception\FlezhaException;
use Flezha\Ffi\Core;
use Flezha\Graph\ChannelSpec;
use Flezha\Graph\ProcNode;
use Flezha\Logger;
use Flezha\SpawnBridge;
use Flezha\Timer;
use Flezha\Transforms;
use Flezha\WorkflowCtx;
use Throwable;

/**
 * Turning one activation into one answer, exactly once.
 *
 * Per-entity exclusivity IS the handout: the core holds an entity's next
 * activation until this one is answered, so an activation that returns
 * unanswered retires that entity for the life of the process. Every path out
 * of {@see dispatch()} therefore ends in a completion or an abandon, through a
 * `finally` rather than a rule applied at each return site — a throwing
 * handler, an encoding failure and a killed worker all reach the same guard.
 */
final class Dispatcher
{
    public function __construct(
        private readonly Registry $registry,
        private readonly Core $core,
    ) {
    }

    /**
     * Dispatch one activation and answer it exactly once.
     *
     * The guard is the contract, not politeness: a throw from the dispatcher,
     * from encoding the result, or from `complete` itself abandons rather than
     * logging and returning.
     */
    public function dispatch(string $encoded): void
    {
        $token = null;
        $answered = false;
        try {
            $activation = ActivationCodec::decodeActivation($encoded);
            $token = $activation['token'];
            $this->core->complete(ActivationCodec::encodeResult($this->answer($activation)));
            $answered = true;
        } catch (Throwable $e) {
            Logger::current()->error('activation processing failed: ' . self::message($e));
        } finally {
            if (!$answered && $token !== null) {
                try {
                    $this->core->abandon($token);
                } catch (Throwable $e) {
                    // The core refuses an unknown token: `complete` had
                    // already landed and only the bookkeeping after it threw.
                    // Nothing is wedged.
                    Logger::current()->warn(
                        "abandoning activation $token failed: " . self::message($e),
                    );
                }
            }
        }
    }

    /**
     * Produce the answer for one activation.
     *
     * Throwing is not an option here: every failure becomes a result the core
     * can act on, because the alternative is an entity that never receives
     * work again.
     *
     * @param array<string, mixed> $activation
     * @return array<string, mixed>
     */
    public function answer(array $activation): array
    {
        try {
            $registered = $this->registry->get((string) $activation['factory']);
            if ($registered === null) {
                return self::failed($activation['token'], sprintf(
                    'no class registered for factory `%s`',
                    $activation['factory'],
                ), true);
            }

            // The workflow body is the one kind addressed to a class rather
            // than to a procedure, so it is answered before the procedure is
            // resolved — which is what lets every arm below hold one.
            if ($activation['kind'] === 'workflow_run') {
                return $this->runWorkflow($activation, $registered);
            }

            $node = $registered->spec->procedures[$activation['procedure']] ?? null;
            if ($node === null) {
                return self::failed($activation['token'], sprintf(
                    'class `%s` has no procedure at index %d',
                    $activation['factory'],
                    $activation['procedure'],
                ), true);
            }

            return match ($activation['kind']) {
                'work' => $this->runWork($activation, $registered, $node),
                'restore_fold' => $this->restoreFold($activation, $registered, $node),
                'snapshot_request', 'live_state_read', 'migration_serialize' =>
                    $this->serializedState($activation, $registered, $node),
                'apply_state' => $this->applyState($activation, $registered, $node),
                'key_request' => $this->prepare($activation, $registered),
                'compensate_run' => $this->compensate($activation, $registered, $node),
                default => self::failed($activation['token'], sprintf(
                    'unknown activation kind `%s`',
                    $activation['kind'],
                ), true),
            };
        } catch (Throwable $e) {
            return self::failed($activation['token'], self::message($e), self::nonRetryable($e));
        }
    }

    /**
     * Run user code against one entity, and keep the state it produced.
     *
     * Every activation kind that reaches a handler goes through here — an
     * input, a timer firing, a compensation — so the context they receive and
     * the state they leave behind cannot differ between them. The state is
     * written back whether or not the body threw: a change recorded before the
     * throw is already journaled, and dropping it here would make the live
     * entity disagree with its own log.
     *
     * @param array<string, mixed> $activation
     */
    public static function invoke(
        array $activation,
        RegisteredClass $registered,
        ProcNode $node,
        SpawnBridge $bridge,
        int $now,
        bool $replay,
        callable $body,
    ): Ctx {
        $ctx = new Ctx(
            spec: $registered->spec,
            node: $node,
            outgoing: $registered->outgoing[$node->index],
            operative: (string) $activation['operative'],
            entityKey: $activation['entity_key'],
            changeSeq: (int) $activation['change_seq'],
            now: $now,
            replay: $replay,
            state: $registered->stateOf($node, $activation['entity_key']),
            bridge: $bridge,
        );

        try {
            $body($ctx);
        } finally {
            $registered->setState($node, $activation['entity_key'], $ctx->state);
        }

        return $ctx;
    }

    /**
     * @param array<string, mixed> $activation
     * @return array<string, mixed>
     */
    private function runWork(array $activation, RegisteredClass $registered, ProcNode $node): array
    {
        $effects = [];

        foreach ($activation['items'] as $item) {
            $effect = $this->runItem($activation, $registered, $node, $item);
            $effects[] = $effect;

            if ($effect['verdict'] !== 'ok') {
                // Items after a failure are re-handed by the core, so the
                // batch stops here rather than reporting effects the entity
                // never saw.
                break;
            }
        }

        return ['token' => $activation['token'], 'outcome' => 'work', 'effects' => $effects];
    }

    /**
     * One work item, and its effect however it ended.
     *
     * The `try` is the whole reason this is its own method: an item that
     * throws must become a verdict the core's ladder can act on, and a `catch`
     * wrapped around the loop instead would lose the effects of the items that
     * already succeeded.
     *
     * @param array<string, mixed> $activation
     * @param array<string, mixed> $item
     * @return array<string, mixed>
     */
    private function runItem(
        array $activation,
        RegisteredClass $registered,
        ProcNode $node,
        array $item,
    ): array {
        $now = (int) ($item['now'] ?? $item['scheduled_at'] ?? 0);

        try {
            $ctx = self::invoke(
                $activation,
                $registered,
                $node,
                $this->core,
                $now,
                (bool) ($item['replay'] ?? false),
                fn (Ctx $ctx) => $this->serve($ctx, $registered, $node, $item, $now),
            );

            return $ctx->effect($item['consumed_offset'] ?? null);
        } catch (Throwable $e) {
            // The backtrace is a host diagnostic, not a record: the verdict
            // below travels to the dead-letter entry and the controller's log.
            Logger::current()->debug(sprintf(
                "%s failed an item: %s\n%s",
                $node->name,
                self::message($e),
                $e->getTraceAsString(),
            ));

            return [
                'verdict' => self::nonRetryable($e) ? 'non_retryable' : 'retryable',
                'message' => self::message($e),
                'event_time' => $now,
                'consumed_offset' => $item['consumed_offset'] ?? null,
            ];
        }
    }

    /**
     * Hand one item to user code: a timer firing, or a decoded input.
     *
     * @param array<string, mixed> $item
     */
    private function serve(Ctx $ctx, RegisteredClass $registered, ProcNode $node, array $item, int $now): void
    {
        $definition = $node->definition;

        if ($item['kind'] === 'timer') {
            if (!$definition->declares('onTimer')) {
                throw new FlezhaException(sprintf(
                    '%s registered a timer but declares no onTimer; registration is a runtime '
                    . 'call, so this cannot be refused at declaration time',
                    $node->name,
                ));
            }
            $definition->instance->onTimer(new Timer((string) $item['timer_kind'], $now), $ctx);

            return;
        }

        $channel = $registered->spec->channels[$item['channel']] ?? null;
        // A filtered value is consumed and reaches no handler. It still
        // produces an effect, so its offset advances and nothing is retried.
        // A fanned one reaches the handler once per value.
        foreach (self::valuesOf($channel, (string) $item['payload']) as $input) {
            $ctx->emitReturned($definition->instance->run($input, $ctx));
        }
    }

    /**
     * @param array<string, mixed> $activation
     * @return array<string, mixed>
     */
    private function restoreFold(array $activation, RegisteredClass $registered, ProcNode $node): array
    {
        $registered->setState($node, $activation['entity_key'], Restore::fold(
            $node->definition,
            ['snapshot' => $activation['snapshot'], 'changes' => $activation['changes']],
            $activation['entity_key'],
            $node->name,
        ));

        return ['token' => $activation['token'], 'outcome' => 'restored'];
    }

    /**
     * @param array<string, mixed> $activation
     * @return array<string, mixed>
     */
    private function applyState(array $activation, RegisteredClass $registered, ProcNode $node): array
    {
        $registered->setState(
            $node,
            $activation['entity_key'],
            $node->definition->decode('state', (string) $activation['state_json']),
        );

        return ['token' => $activation['token'], 'outcome' => 'state_applied'];
    }

    /**
     * @param array<string, mixed> $activation
     * @return array<string, mixed>
     */
    private function prepare(array $activation, RegisteredClass $registered): array
    {
        $request = $activation['key_request'];
        $channel = $registered->spec->channels[$request['channel']] ?? null;
        $consumer = $channel?->consumers[0] ?? null;
        $node = $consumer === null ? null : ($registered->spec->procedures[$consumer] ?? null);
        $definition = $node?->definition;

        $answers = [];
        foreach ($request['payloads'] as $payload) {
            try {
                $values = self::valuesOf($channel, (string) $payload);
                // A filtered payload answers no key. For a dynamic consumer
                // that is what tells the core not to route it at all — the
                // drop happens before an entity is minted rather than inside one.
                if ($values === []) {
                    $answers[] = ['keys' => []];
                    continue;
                }

                $keys = [];
                if ($request['want_keys']) {
                    foreach ($values as $value) {
                        if ($channel?->keyBy !== null) {
                            $keys[] = (string) ($channel->keyBy)($value);
                        } elseif ($definition?->declares('entitiesFor') === true) {
                            array_push($keys, ...$definition->entitiesFor($value));
                        }
                    }
                }

                $answers[] = [
                    'keys' => array_values(array_unique($keys)),
                    'event_time' => $request['want_event_time'] && $definition?->declares('eventTime') === true
                        ? $definition->eventTimeOf($values[0])
                        : null,
                ];
            } catch (Throwable $e) {
                // A per-payload refusal enters the input channel's retry
                // ladder; it is not a failure of the crossing, whose other
                // payloads still have answers.
                $answers[] = ['error' => self::message($e), 'non_retryable' => self::nonRetryable($e)];
            }
        }

        return ['token' => $activation['token'], 'outcome' => 'prepared', 'prepared' => $answers];
    }

    /**
     * @param array<string, mixed> $activation
     * @return array<string, mixed>
     */
    private function runWorkflow(array $activation, RegisteredClass $registered): array
    {
        $run = $registered->spec->run;
        if ($run === null) {
            return self::failed($activation['token'], sprintf(
                'class `%s` was handed a workflow body but declares none',
                $activation['factory'],
            ), true);
        }

        $ctx = new WorkflowCtx((string) $activation['operative'], $this->core);
        $args = $activation['args'] === '' ? null : Json::decode((string) $activation['args']);
        $returned = $run($args, $ctx);
        $completed = $ctx->isCompleted() || $returned !== null;

        return [
            'token' => $activation['token'],
            'outcome' => 'workflow_done',
            'workflow_completed' => $completed,
            'workflow_result' => Json::encode($ctx->isCompleted() ? $ctx->result() : $returned),
        ];
    }

    /**
     * @param array<string, mixed> $activation
     * @return array<string, mixed>
     */
    private function compensate(array $activation, RegisteredClass $registered, ProcNode $node): array
    {
        $definition = $node->definition;
        if (!$definition->declares('compensate')) {
            return self::failed($activation['token'], "$node->name declares no compensate hook", true);
        }

        $outputs = array_map(
            static fn (string $bytes): mixed => Json::decode($bytes),
            $activation['outputs'],
        );
        $ctx = self::invoke(
            $activation,
            $registered,
            $node,
            $this->core,
            0,
            false,
            static fn (Ctx $ctx) => $definition->instance->compensate($outputs, $ctx),
        );

        return [
            'token' => $activation['token'],
            'outcome' => 'work',
            'effects' => [$ctx->effect(null)],
        ];
    }

    /**
     * The three kinds that answer "the state, encoded".
     *
     * One arm, because they differ only in the field the core reads the bytes
     * out of: this codec IS JSON, so a compaction snapshot, a live read and a
     * migration handover carry the same bytes. There is no second encoding to
     * convert between — the one place this host is simpler than a MessagePack
     * one rather than merely different.
     *
     * @param array<string, mixed> $activation
     * @return array<string, mixed>
     */
    private function serializedState(array $activation, RegisteredClass $registered, ProcNode $node): array
    {
        [$outcome, $field] = match ($activation['kind']) {
            'snapshot_request' => ['snapshot', 'state'],
            'live_state_read' => ['state_json', 'json'],
            default => ['migration_state', 'json'],
        };

        return [
            'token' => $activation['token'],
            'outcome' => $outcome,
            $field => $node->definition->encode(
                'state',
                $registered->stateOf($node, $activation['entity_key']),
            ),
        ];
    }

    /**
     * A payload as the handler sees it: the channel's declared decoder (or the
     * JSON default), then its transforms in declaration order.
     *
     * One function for the two places a payload crosses in — an input item and
     * a payload at the prepare crossing — so a filter cannot mean one thing on
     * one path and another on the other.
     *
     * @return list<mixed>
     */
    private static function valuesOf(?ChannelSpec $channel, string $payload): array
    {
        $decoded = $channel?->decode !== null ? ($channel->decode)($payload) : Json::decode($payload);

        return Transforms::apply($channel?->transforms ?? [], $decoded);
    }

    /** @return array<string, mixed> */
    private static function failed(int $token, string $message, bool $nonRetryable): array
    {
        return [
            'token' => $token,
            'outcome' => 'failed',
            'error' => $message,
            'non_retryable' => $nonRetryable,
        ];
    }

    /**
     * What a failure says on the wire.
     *
     * `Class: message`, in family with what the other SDKs send — this string
     * is stored in a dead-letter entry and read by an operator, and a
     * multi-line backtrace there is noise the retry ladder multiplies.
     */
    private static function message(Throwable $e): string
    {
        return $e::class . ': ' . $e->getMessage();
    }

    /**
     * A handler declares an error non-retryable by answering `isNonRetryable`,
     * so any exception class can carry the verdict rather than only this SDK's.
     */
    private static function nonRetryable(Throwable $e): bool
    {
        return method_exists($e, 'isNonRetryable') && $e->isNonRetryable();
    }
}
