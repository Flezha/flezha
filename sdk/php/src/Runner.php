<?php

declare(strict_types=1);

namespace Flezha;

use Flezha\Codec\ActivationCodec;
use Flezha\Codec\Json;
use Flezha\Dispatch\Dispatcher;
use Flezha\Dispatch\Registry;
use Flezha\Exception\FlezhaException;
use Flezha\Executor\Executor;
use Flezha\Executor\Executors;
use Flezha\Executor\LoopState;
use Flezha\Ffi\Binding;
use Flezha\Ffi\Core;
use Flezha\Graph\HostClassDecl;
use Flezha\Graph\Validate;

/**
 * The runner: what an author starts, and everything it owns while running.
 *
 * Classes are registered explicitly, by naming them. There is no autoload
 * scan and no package walk: what runs is what the author said runs, and a
 * class that is defined but not registered never reaches the core.
 *
 * ## One runner per process
 *
 * The SDK never forks, spawns or supervises. N processes are N runners, which
 * is what the controller's placement already spreads operatives across, and it
 * keeps this layer agnostic between Kubernetes, systemd and a supervisor's own
 * process management. Threads inside one process are the executor's business,
 * not this class's.
 *
 * ## Re-entry from a worker thread
 *
 * A `SwooleExecutor` worker bootstraps this same entry script, so `run()` has
 * two paths: the first caller creates the context, declares the classes and
 * connects; a worker finds the attachment its bootstrap left, rebuilds only
 * the PHP-side registry and serves. `flz_core_declare_class` is per context,
 * not per thread, and must not run twice.
 */
final class Runner
{
    /** How long a loop parks in one `poll`. */
    public const POLL_TIMEOUT_MS = 250;

    /**
     * The context and loop state a worker thread was handed, or null on the
     * thread that creates them.
     *
     * A static, which on a thread-safe build is already per-thread: each
     * `Swoole\Thread` is a separate interpreter with its own statics, so this
     * is invisible to every thread but the one whose bootstrap wrote it.
     *
     * @var array{int, LoopState}|null
     */
    private static ?array $attachment = null;

    /** @var list<class-string<Operative>> */
    private array $classes = [];
    private readonly Registry $registry;
    private ?Core $core = null;
    private ?Executor $executor = null;
    private ?Dispatcher $dispatcher = null;
    private ?EntryIdle $idle = null;
    private ?int $maxActivations = null;
    private Shutdown $shutdown;

    public function __construct(public readonly string $namespace)
    {
        $this->registry = new Registry();
        $this->shutdown = Shutdown::unwatched();
        $this->maxActivations = self::budgetFromEnvironment();
    }

    /**
     * Leave the context and loop state a worker thread should attach to.
     *
     * Called by the Swoole adapter's thread bootstrap before it re-enters the
     * entry script; nothing else has any reason to.
     */
    public static function attachInThread(int $address, LoopState $state): void
    {
        self::$attachment = [$address, $state];
    }

    /** Register a stream operative class. @param class-string<Operative> $class */
    public function defineStream(string $class): self
    {
        $this->classes[] = $class;

        return $this;
    }

    /**
     * Register a workflow operative class.
     *
     * The same registration as a stream, and deliberately the same code: what
     * a class declares is read from the class, so the two names are for the
     * reader rather than for the runner.
     *
     * @param class-string<Operative> $class
     */
    public function defineWorkflow(string $class): self
    {
        return $this->defineStream($class);
    }

    /** The inverse of `defineStream` / `defineWorkflow`. */
    public function removeType(string $class): self
    {
        $this->classes = array_values(array_filter(
            $this->classes,
            static fn (string $registered): bool => $registered !== $class,
        ));

        return $this;
    }

    /** Which executor drives the poll loop. `SyncExecutor` when never called. */
    public function executor(Executor $executor): self
    {
        $this->executor = $executor;

        return $this;
    }

    public function logger(Logger $logger): self
    {
        Logger::use($logger);

        return $this;
    }

    /**
     * When the runner last delivered a frame to entry channel `$channel`, or
     * null before its first frame (and for a name no declared operative has —
     * both mean "nothing has arrived here"). Legal before `start()`; reading
     * never blocks delivery.
     *
     * The instant is the *signal*; whether the silence since then is anomalous
     * is the application's call. Register an observer with `onEntryIdle`
     * instead of polling this in a loop.
     */
    public function lastEntryDelivery(string $channel): ?\DateTimeImmutable
    {
        return EntryIdle::lastEntryDelivery($this->core ?? Core::contextFree(), $channel);
    }

    /**
     * Observe silence on entry channel `$channel`: `$observer` is notified
     * once per idle episode, when the gap since the last delivered frame first
     * exceeds `$thresholdMs`, and once again when a frame resumes it. Silence
     * before the channel's very first frame is never an episode.
     *
     * Legal before and after `start()`. Evaluation runs between polls of the
     * thread that started the runner — never inside a body — so an observer
     * that throws costs the data plane nothing. Several observers may watch
     * one channel.
     *
     * A worker interpreter's handle registers nothing: the entry script re-runs
     * in each one, and only the thread that started the runner owns its
     * observers, as it owns its signals and its standard input.
     */
    public function onEntryIdle(string $channel, int $thresholdMs, IdleObserver|callable $observer): self
    {
        if (self::$attachment !== null) {
            return $this;
        }
        $this->idle ??= new EntryIdle($this->core ?? Core::contextFree());
        $this->idle->register($channel, $thresholdMs, $observer);

        return $this;
    }

    /** Advance the idle observers — between polls, on the thread that owns them. */
    public function observeIdle(): void
    {
        $this->idle?->tick();
    }

    /**
     * Shut down cleanly after this many activations, so an external
     * supervisor can recycle a long-lived process without data loss.
     *
     * Unlimited by default. A clean shutdown leaves unpolled work journaled
     * controller-side, to be re-forwarded on the next bind.
     */
    public function maxActivations(?int $count): self
    {
        $this->maxActivations = $count;

        return $this;
    }

    /**
     * Declare, connect and serve until shutdown.
     *
     * On a worker thread this attaches to the context the pool shares and
     * serves against it, which is why the entry script must be re-enterable.
     */
    public function run(): void
    {
        $attachment = self::$attachment;
        if ($attachment !== null) {
            [$address, $state] = $attachment;
            $this->core = Core::attach($address);
            $this->buildRegistry();
            $this->dispatcher = new Dispatcher($this->registry, $this->core);
            $this->serve($state);

            return;
        }

        $this->start();
        $this->awaitTermination();
    }

    /**
     * Declare, connect and start serving — everything up to and including the
     * first poll, and nothing that blocks.
     *
     * Separate from `run` so a host with its own supervision can own the
     * waiting, and so the startup order is something a test can observe.
     */
    public function start(): self
    {
        // Before anything else: a shim built against a different activation
        // schema is refused here, with no controller connection attempted.
        $this->core = Core::create();
        $this->buildRegistry();
        $this->declareAll();
        $this->dispatcher = new Dispatcher($this->registry, $this->core);
        $this->executor ??= Executors::fromEnvironment($this->maxActivations);

        $this->core->start($this->namespace);
        $this->shutdown = Shutdown::watch($this->stop(...));

        Logger::current()->info(sprintf(
            'runner up on namespace %s: %s executor, %d operative class(es)',
            $this->namespace,
            $this->executor->name(),
            count($this->classes),
        ));

        return $this;
    }

    /** Serve until shutdown, then drain and release the core. */
    public function awaitTermination(): void
    {
        $executor = $this->executor ?? throw new FlezhaException('the runner was not started');
        try {
            $executor->run($this);
        } finally {
            $this->drain();
        }
    }

    /** Stop fetching. What is in flight finishes; nothing new is polled. */
    public function stop(): void
    {
        // The core says when: every loop keeps polling until its poll reports
        // the core shutting down, which the core reports only once the
        // shutdown sequence has shipped every Entity's state through these
        // same loops. The executor's own stop is the bound `drain` applies.
        $this->core?->shutdown();
    }

    /**
     * The poll loop, and the one place the three answers `poll` can give are
     * interpreted.
     *
     * Every executor runs this, so what a timeout, an activation and a
     * shutdown each mean live here rather than in one correct copy per
     * executor. Each thread polls, dispatches and completes within itself, so
     * no activation crosses a thread boundary and there is no queue, no
     * saturation and no backpressure question — a busy thread simply does not
     * poll.
     */
    public function serve(LoopState $state): void
    {
        $core = $this->core ?? throw new FlezhaException('the runner has no core to poll');
        $dispatcher = $this->dispatcher ?? throw new FlezhaException('the runner has no dispatcher');

        while (!$state->stopping()) {
            $this->observeShutdown();
            $this->observeIdle();

            $bytes = null;
            $status = $core->poll(self::POLL_TIMEOUT_MS, $bytes);

            if ($status === Binding::POLL_SHUTTING_DOWN) {
                $state->stop();

                return;
            }
            if ($status !== Binding::POLL_ACTIVATION || $bytes === null) {
                continue;
            }
            if (!$state->claim()) {
                // The budget is spent: this activation is not ours to run, so
                // hand it back untouched and let the runner exit cleanly. The
                // core re-forwards it on the next bind.
                $state->stop();
                $core->abandon(ActivationCodec::tokenOf($bytes));

                return;
            }

            $started = hrtime(true);
            $dispatcher->dispatch($bytes);
            $state->observe(hrtime(true) - $started);
        }
    }

    /**
     * Observe the two ways a runner is asked to stop that are not the core's
     * own, between polls. A worker thread's handle watches nothing — only the
     * thread that started the runner owns its signals and its standard input.
     */
    public function observeShutdown(): void
    {
        if ($this->shutdown->observe()) {
            Logger::current()->info('standard input reached end of file; draining');
            $this->stop();
        }
    }

    public function core(): Core
    {
        return $this->core ?? throw new FlezhaException('the runner has no core');
    }

    /** The context address a worker thread attaches to. */
    public function coreAddress(): int
    {
        return $this->core()->address();
    }

    /** The autoloader a worker thread's fresh interpreter has to load first. */
    public function autoloadPath(): string
    {
        // Installed, the package sits at `vendor/flezha/sdk/src`; in a
        // checkout of this repository it is `sdk/php/src`.
        $candidates = [
            dirname(__DIR__, 3) . '/autoload.php',
            dirname(__DIR__) . '/vendor/autoload.php',
        ];
        foreach ($candidates as $candidate) {
            if (is_file($candidate)) {
                return $candidate;
            }
        }

        throw new FlezhaException(
            'a threaded executor bootstraps each worker from Composer\'s autoloader and this '
            . 'process was not started through one; install the package with composer, or use '
            . 'the default SyncExecutor.',
        );
    }

    /** The entry script a worker thread re-enters to rebuild the registry. */
    public function entryScript(): string
    {
        $script = $_SERVER['SCRIPT_FILENAME'] ?? '';

        return is_string($script) && $script !== ''
            ? $script
            : throw new FlezhaException('the runner cannot find its own entry script');
    }

    /**
     * Give up the context without freeing it.
     *
     * A worker the bounded drain gave up on is inside user code and may yet
     * call the core; freeing the context under it would be a use-after-free in
     * native code. The process is on its way out either way, so the kernel
     * reclaims what this skips.
     */
    public function abandonContext(): void
    {
        $this->core = null;
    }

    /** Stop fetching, drain what is in flight, and release the core. */
    private function drain(): void
    {
        Logger::current()->info('draining');

        // By now every loop has left on the core's word, or is inside a body
        // that never returned — the executor's stop bounds the latter. A loop
        // that left on its own activation budget never asked the core, so it
        // is asked here.
        $this->executor?->stop();
        $this->core?->shutdown();

        $state = $this->executor?->state();
        if ($state !== null) {
            self::reportSizing($state);
        }

        $this->idle?->stop();

        // The context is released only when nothing is still running against
        // it; `abandonContext` is what a bounded drain that gave up leaves.
        $this->core?->free();
        $this->core = null;
    }

    /** @param class-string<Operative> $class */
    private function buildRegistry(): void
    {
        foreach ($this->classes as $class) {
            $spec = $class::spec();
            Validate::spec($spec);
            $this->registry->add($spec);
        }
    }

    private function declareAll(): void
    {
        $core = $this->core();
        foreach ($this->registry->all() as $registered) {
            $core->declareClass(Json::encode(HostClassDecl::of($registered->spec)));
        }
    }

    private static function reportSizing(LoopState $state): void
    {
        [$activations, $meanBodyNanos] = $state->report();
        if ($activations === 0) {
            return;
        }

        Logger::current()->info(sprintf(
            'served %d activation(s), mean procedure body %.3f ms; one loop sustains about %.0f/s '
            . 'at that latency, and in-flight concurrency is the loop count',
            $activations,
            $meanBodyNanos / 1e6,
            $meanBodyNanos === 0 ? 0.0 : 1e9 / $meanBodyNanos,
        ));
    }

    private static function budgetFromEnvironment(): ?int
    {
        $declared = (string) (getenv('FLEZHA_MAX_ACTIVATIONS') ?: '');

        return ctype_digit($declared) && (int) $declared > 0 ? (int) $declared : null;
    }
}
