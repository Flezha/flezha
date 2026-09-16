<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Exception\FlezhaException;
use Flezha\Ffi\Binding;
use Flezha\Ffi\Core;
use PHPUnit\Framework\TestCase;

/**
 * The package the way a registry serves it: the tree a tag would carry, with
 * none of the build output this checkout has in `lib/`, installed by Composer
 * into a project of its own.
 *
 * Every other test loads the checkout's library, so none of them can see a
 * package that installs without one.
 */
final class InstallTest extends TestCase
{
    private const VERSION = '0.1.0';

    private static string $scratch;
    private static string $package;

    public static function setUpBeforeClass(): void
    {
        self::$scratch = sys_get_temp_dir() . '/flezha-install-' . bin2hex(random_bytes(4));
        self::$package = self::$scratch . '/package';
        mkdir(self::$package, 0o755, true);

        // What `git archive` of a tag would hold, taken from the working tree
        // through a scratch index so the checkout's own index is never touched.
        $root = dirname(__DIR__);
        $index = ['GIT_INDEX_FILE' => self::$scratch . '/index'];
        self::succeed(['git', '-c', 'safe.directory=*', 'add', '-A', '--', '.'], $root, $index);
        $tree = trim(self::succeed(['git', '-c', 'safe.directory=*', 'write-tree'], $root, $index));
        self::succeed(['git', '-c', 'safe.directory=*', 'archive', '-o', self::$scratch . '/package.tar', $tree], $root);
        self::succeed(['tar', '-xf', self::$scratch . '/package.tar', '-C', self::$package], $root);
    }

    public static function tearDownAfterClass(): void
    {
        self::remove(self::$scratch);
    }

    public function testATagArchiveCarriesNoLibraryAndInstallsOneThatLoads(): void
    {
        self::assertDirectoryDoesNotExist(self::$package . '/lib');

        $library = Binding::libraryPath();
        $project = self::project('installs');
        [$status, $output] = self::composer($project, self::release('good', $library));
        self::assertSame(0, $status, $output);

        $installed = sprintf('%s/vendor/flezha/sdk/lib/%s/%s', $project, Binding::platform(), basename($library));
        self::assertFileEquals($library, $installed);

        self::assertSame(
            $installed . '|' . Core::SCHEMA_VERSION,
            self::php($project, 'echo Flezha\Ffi\Binding::libraryPath(), "|", Flezha\Ffi\Core::create()->coreSchemaVersion();'),
        );
    }

    public function testAChecksumMismatchRefusesTheInstallAndLeavesNothing(): void
    {
        $library = Binding::libraryPath();
        $tampered = self::$scratch . '/tampered.so';
        file_put_contents($tampered, file_get_contents($library) . "\0");
        $release = self::release('tampered', $tampered, hash_file('sha256', $library));

        $project = self::project('tampered');
        [$status, $output] = self::composer($project, $release);

        self::assertNotSame(0, $status, $output);
        self::assertStringContainsString('its SHA-256 is ' . hash_file('sha256', $tampered), $output);
        self::assertDirectoryDoesNotExist($project . '/vendor/flezha/sdk', $output);
    }

    /**
     * A platform no release publishes for needs a library built for it; a
     * fetch that failed needs a retry. Different actions, so different words.
     */
    public function testNoPublishedBinaryAndAFailedFetchAreDifferentRefusals(): void
    {
        $project = self::project('offline');
        [$status, $failedFetch] = self::composer($project, 'file://' . self::$scratch . '/no-release');
        self::assertNotSame(0, $status, $failedFetch);
        self::assertStringContainsString('could not fetch libflz_core_ffi for ' . Binding::platform(), $failedFetch);

        try {
            Binding::obtain('plan9-x64', self::VERSION, static fn (string $url): string => self::fail("fetched {$url}"));
            self::fail('a platform no release publishes for was fetched for');
        } catch (FlezhaException $e) {
            $unpublished = $e->getMessage();
        }

        self::assertStringContainsString('No release publishes it for plan9-x64', $unpublished);
        self::assertStringContainsString('build it for this platform', $unpublished);
        self::assertStringNotContainsString('could not fetch', $unpublished);
        self::assertStringNotContainsString('No release publishes it', $failedFetch);
    }

    /** `--no-plugins` is a real setting, so the loader names the way to finish by hand. */
    public function testAnInstallWithoutThePluginRefusesAtLoadNamingHowToFinishIt(): void
    {
        $project = self::project('no-plugins');
        [$status, $output] = self::composer($project, 'file://' . self::$scratch . '/unused', '--no-plugins');
        self::assertSame(0, $status, $output);

        $refusal = self::php(
            $project,
            'try { Flezha\Ffi\Binding::libraryPath(); } catch (Flezha\Exception\FlezhaException $e) { echo $e->getMessage(); }',
        );

        $directory = sprintf('%s/vendor/flezha/sdk/lib/%s', $project, Binding::platform());
        foreach ([
            Binding::platform(),
            $directory,
            sprintf('/v%s/libflz_core_ffi-%s.', self::VERSION, Binding::platform()),
            '/v' . self::VERSION . '/SHA256SUMS.txt',
            'composer reinstall flezha/sdk',
            Binding::NATIVE_LIB_DIR_ENV,
        ] as $named) {
            self::assertStringContainsString($named, $refusal);
        }
    }

    /**
     * A release directory holding `$asset` as the host's library and a manifest naming `$sha256`.
     *
     * The asset name is spelled here rather than read from the package, so a
     * package that asked for another name would fail to install.
     */
    private static function release(string $name, string $asset, ?string $sha256 = null): string
    {
        $published = sprintf(
            'libflz_core_ffi-%s.%s',
            Binding::platform(),
            pathinfo(Binding::libraryPath(), PATHINFO_EXTENSION),
        );
        $directory = self::$scratch . '/releases/' . $name . '/v' . self::VERSION;
        mkdir($directory, 0o755, true);
        copy($asset, $directory . '/' . $published);
        file_put_contents(
            $directory . '/SHA256SUMS.txt',
            ($sha256 ?? hash_file('sha256', $asset)) . '  ' . $published . "\n",
        );

        return 'file://' . dirname($directory);
    }

    private static function project(string $name): string
    {
        $directory = self::$scratch . '/' . $name;
        mkdir($directory, 0o755, true);
        file_put_contents($directory . '/composer.json', json_encode([
            'repositories' => [
                ['packagist.org' => false],
                [
                    'type' => 'path',
                    'url' => self::$package,
                    'options' => ['symlink' => false, 'versions' => [Binding::PACKAGE => self::VERSION]],
                ],
            ],
            'require' => [Binding::PACKAGE => self::VERSION],
            'config' => ['allow-plugins' => [Binding::PACKAGE => true]],
        ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));

        return $directory;
    }

    /** @return array{int, string} */
    private static function composer(string $project, string $releases, string ...$flags): array
    {
        return self::execute(['composer', 'install', '--no-interaction', '--no-progress', ...$flags], $project, [
            'COMPOSER_HOME' => self::$scratch . '/composer-home',
            'COMPOSER_CACHE_DIR' => self::$scratch . '/composer-cache',
            'COMPOSER_NO_AUDIT' => '1',
            Binding::RELEASE_BASE_URL_ENV => $releases,
            Binding::NATIVE_LIB_DIR_ENV => false,
        ]);
    }

    /** Run `$code` in the project, in a process of its own: this one has already bound the checkout's library. */
    private static function php(string $project, string $code): string
    {
        return self::succeed([PHP_BINARY, '-r', 'require "vendor/autoload.php"; ' . $code], $project);
    }

    /**
     * @param list<string> $command
     * @param array<string, string|false> $env overrides; false removes the variable
     */
    private static function succeed(array $command, string $cwd, array $env = []): string
    {
        [$status, $output] = self::execute($command, $cwd, $env);
        self::assertSame(0, $status, implode(' ', $command) . ":\n" . $output);

        return $output;
    }

    /**
     * @param list<string> $command
     * @param array<string, string|false> $env overrides; false removes the variable
     *
     * @return array{int, string} the exit status and the combined output
     */
    private static function execute(array $command, string $cwd, array $env = []): array
    {
        $environment = array_filter([...getenv(), ...$env], static fn (string|false $value): bool => $value !== false);
        $process = proc_open($command, [1 => ['pipe', 'w'], 2 => ['redirect', 1]], $pipes, $cwd, $environment);
        self::assertIsResource($process, 'could not start ' . $command[0]);
        $output = (string) stream_get_contents($pipes[1]);
        fclose($pipes[1]);

        return [proc_close($process), $output];
    }

    private static function remove(string $directory): void
    {
        $walk = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($directory, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST,
        );
        foreach ($walk as $entry) {
            $entry->isDir() && !$entry->isLink() ? rmdir($entry->getPathname()) : unlink($entry->getPathname());
        }
        rmdir($directory);
    }
}
