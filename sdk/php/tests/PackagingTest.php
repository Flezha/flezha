<?php

declare(strict_types=1);

namespace Flezha\Tests;

/**
 * The optional-dependency posture, enforced rather than reviewed.
 *
 * Swoole is real and shipped, but it needs a thread-safe build most
 * distributions do not provide, so requiring it would make the common case
 * uninstallable. What makes the option real is that the adapter is inert on
 * disk until someone instantiates it — PHP autoloads per class, so a file no
 * shared code names is never parsed.
 */
final class PackagingTest extends RepositoryTestCase
{
    /** The adapter's own files: the only ones allowed to name a Swoole type. */
    private const ADAPTER = '/^(Swoole\w*\.php|swoole_\w+\.php)$/';

    /** @return array<string, mixed> */
    private function composer(): array
    {
        return json_decode((string) file_get_contents(dirname(__DIR__) . '/composer.json'), true);
    }

    public function testThePackageRequiresOnlyPhpItsExtensionsAndThePluginApi(): void
    {
        self::assertSame(['php', 'ext-ffi', 'ext-mbstring', 'composer-plugin-api'], array_keys($this->composer()['require']));
    }

    public function testTheDeclaredFloorIsTheSupportedPhpLine(): void
    {
        self::assertSame('>=8.4', $this->composer()['require']['php']);
    }

    public function testSwooleIsSuggestedAndNothingElseIs(): void
    {
        $suggest = $this->composer()['suggest'];

        self::assertSame(['ext-swoole'], array_keys($suggest));
        self::assertStringContainsString('Swoole\Thread', $suggest['ext-swoole']);
        self::assertStringContainsString('--enable-swoole-thread', $suggest['ext-swoole']);
    }

    /**
     * The discipline that makes optionality real, checked rather than
     * remembered: not in the `Executor` interface, not as a parameter or
     * return type, and not in a `use` statement in shared code.
     */
    public function testNoSwooleSymbolAppearsOutsideTheAdaptersOwnFiles(): void
    {
        $offenders = [];

        foreach ($this->sources() as $path) {
            if (preg_match(self::ADAPTER, basename($path)) === 1) {
                continue;
            }
            if (preg_match('/\bSwoole\\\\|\bswoole_[a-z_]+\s*\(/', $this->code($path)) === 1) {
                $offenders[] = substr($path, strlen(dirname(__DIR__)) + 1);
            }
        }

        self::assertSame([], $offenders, sprintf(
            'these files name a Swoole symbol outside the adapter: %s. PHP autoloads per class, '
            . 'so naming one here would parse the adapter on a build that has no extension.',
            implode(', ', $offenders),
        ));
    }

    /** The adapter's files are never autoloaded by shared code naming them. */
    public function testTheAdapterIsReachedOnlyThroughTheFactory(): void
    {
        $mentions = [];

        foreach ($this->sources() as $path) {
            $name = basename($path);
            if ($name === 'Executors.php' || preg_match(self::ADAPTER, $name) === 1) {
                continue;
            }
            if (str_contains($this->code($path), 'SwooleExecutor')) {
                $mentions[] = substr($path, strlen(dirname(__DIR__)) + 1);
            }
        }

        self::assertSame([], $mentions, sprintf(
            'these files name SwooleExecutor: %s. Executors::swoole() is the one door, so the '
            . 'adapter stays unparsed until a host asks for it.',
            implode(', ', $mentions),
        ));
    }

    /**
     * A source file with its comments removed.
     *
     * The rule is about what PHP parses and autoloads, and a doc comment
     * explaining why the seam is shaped as it is parses nothing — so the check
     * reads code, not prose. Written the other way round it would forbid
     * exactly the explanation the next reader needs.
     */
    private function code(string $path): string
    {
        $code = '';
        foreach (token_get_all((string) file_get_contents($path)) as $token) {
            if (is_string($token)) {
                $code .= $token;
                continue;
            }
            if ($token[0] !== T_COMMENT && $token[0] !== T_DOC_COMMENT) {
                $code .= $token[1];
            }
        }

        return $code;
    }

    /** @return list<string> */
    private function sources(): array
    {
        $files = [];
        $walk = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator(dirname(__DIR__) . '/src', \FilesystemIterator::SKIP_DOTS),
        );
        foreach ($walk as $file) {
            if ($file->getExtension() === 'php') {
                $files[] = $file->getPathname();
            }
        }
        sort($files);

        return $files;
    }
}
