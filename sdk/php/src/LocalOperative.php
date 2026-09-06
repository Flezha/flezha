<?php

declare(strict_types=1);

namespace Flezha;

use Flezha\Codec\ActivationCodec;
use Flezha\Codec\Json;
use Flezha\Dispatch\Dispatcher;
use Flezha\Dispatch\Registry;
use Flezha\Exception\FlezhaException;
use Flezha\Ffi\Binding;
use Flezha\Ffi\Core;
use Flezha\Graph\HostClassDecl;
use Flezha\Graph\Validate;

/**
 * Running a declared operative graph in this process, with no controller.
 *
 * {@see start()} takes the operative class you deploy and runs the
 * `channels()` graph it already declares — no local-mode member on the class,
 * no second declaration. What runs is the deployed machinery: entry decode,
 * edge transforms and keying, one Entity per resolved key with its own
 * mailbox, the retry ladder, and inter-procedure delivery performed by the
 * runtime.
 *
 * **This host has one thread.** Under the default `sync` executor the caller
 * IS the poll loop, so {@see runUntilIdle()} drives it rather than parking in
 * the core: a thread parked there would be waiting on work it has stopped
 * doing. The core is still what says whether the graph has settled.
 *
 * State is held in memory and nothing is durable. Durability, plugins,
 * placement and migration, the dead-letter queue, observability export, authz
 * and multi-runner scale all need a controller; the run says so on stderr
 * every time it starts. `docs/quickstart.md` walks through it.
 */
final class LocalOperative
{
    /** One poll turn. Short, because a turn that hands nothing back is a probe. */
    private const POLL_MS = 20;

    /** The idle probe's own budget: two observations a millisecond apart. */
    private const IDLE_PROBE_MS = 20;

    /** @var array<string, callable(SinkOutput): void> */
    private array $sinks = [];

    private bool $closed = false;

    private function __construct(
        private readonly Core $core,
        private readonly Dispatcher $dispatcher,
    ) {
    }

    /**
     * Register `$class`'s declared graph and start it locally.
     *
     * @param class-string<Operative> $class
     *
     * @throws FlezhaException naming the capability when the declaration needs
     *                         a controller
     */
    public static function start(string $class): self
    {
        $core = Core::create();

        try {
            $spec = $class::spec();
            // `Validate::spec` is what writes the derived edges back into the
            // spec, so it runs before anything is declared.
            Validate::spec($spec, $core);
            $registry = new Registry();
            $registry->add($spec);
            $core->declareClass(Json::encode(HostClassDecl::of($spec)));
            // Not `start`, which is the one call that dials a controller.
            $core->localStart($spec->factoryId, $spec->factoryId . '-local');
        } catch (\Throwable $failure) {
            $core->free();

            throw $failure;
        }

        return new self($core, new Dispatcher($registry, $core));
    }

    /**
     * Deliver every value released on the sink `$channel` to `$callback`, in
     * release order. A sink channel takes the producing procedure's name
     * unless the declaration named it.
     *
     * @param callable(SinkOutput): void $callback
     */
    public function onSink(string $channel, callable $callback): self
    {
        // Registered with the core first: a channel the declaration does not
        // carry is refused there, naming it, rather than silently accepted.
        $this->core->localOnSink($channel);
        $this->sinks[$channel] = $callback;

        return $this;
    }

    /** Feed one value's wire bytes to a declared entry channel. */
    public function feed(string $channel, string $payload): self
    {
        $this->core->localFeed($channel, $payload);

        return $this;
    }

    /**
     * Drive the graph until it has nothing left to do.
     *
     * One turn is: take an activation if the core has one and answer it, then
     * hand on whatever the sinks released. When a turn hands nothing back, ask
     * the core whether the graph has settled — a refusal there means it has
     * not, and the turn repeats until `$budgetMs` runs out.
     *
     * @throws FlezhaException when the budget elapses first, which is reported
     *                         rather than passed off as done
     */
    public function runUntilIdle(int $budgetMs): void
    {
        $deadline = hrtime(true) + $budgetMs * 1_000_000;
        $why = 'the graph did not settle';
        while (hrtime(true) < $deadline) {
            $bytes = null;
            $status = $this->core->poll(self::POLL_MS, $bytes);
            if ($status === Binding::POLL_ACTIVATION && $bytes !== null) {
                $this->dispatcher->dispatch($bytes);
                $this->drainOutputs();

                continue;
            }
            $this->drainOutputs();
            try {
                $this->core->localRunUntilIdle(self::IDLE_PROBE_MS);

                // Settled: one last pass, because a value released on the
                // turn that settled is still queued.
                $this->drainOutputs();

                return;
            } catch (FlezhaException $notYet) {
                $why = $notYet->getMessage();
            }
        }

        throw new FlezhaException(sprintf('local mode ran for %dms and %s', $budgetMs, $why));
    }

    /** End the run and release its Entities. Idempotent. */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $this->core->localStop();
        // Not `shutdown`: that stops the process-wide poll core, and a process
        // may start another local run after this one.
        $this->core->free();
    }

    /** Everything the sinks have released, handed on and then forgotten. */
    private function drainOutputs(): void
    {
        while (true) {
            $bytes = null;
            if ($this->core->localNextOutput(0, $bytes) !== Binding::POLL_ACTIVATION || $bytes === null) {
                return;
            }
            $this->deliver(ActivationCodec::decodeOutput($bytes));
        }
    }

    /** @param array<string, mixed> $output */
    private function deliver(array $output): void
    {
        if ($output['target'] !== 'plugin') {
            // The core queues what a sink released and nothing else; an edge
            // output is an in-process mailbox push that never leaves.
            throw new FlezhaException('local mode released a ' . (string) $output['target'] . ' output');
        }
        $channel = (string) $output['plugin_channel'];
        $callback = $this->sinks[$channel] ?? null;
        if ($callback === null) {
            return;
        }
        /** @var array<string, string> $metadata */
        $metadata = $output['metadata'];
        $callback(new SinkOutput($channel, (string) $output['payload'], $metadata));
    }
}
