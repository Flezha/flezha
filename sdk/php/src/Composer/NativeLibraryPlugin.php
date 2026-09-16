<?php

declare(strict_types=1);

namespace Flezha\Composer;

use Composer\Composer;
use Composer\IO\IOInterface;
use Composer\Plugin\PluginInterface;
use Flezha\Ffi\Binding;

/**
 * Fetches the host's `libflz_core_ffi` when Composer installs the package.
 *
 * A package published from a version-control tag carries no build output, so
 * the library cannot ride inside it. The work is done in `activate()` because
 * that is the only hook Composer runs for a dependency under `--no-scripts`:
 * package and script events are skipped there, and a dependency's own
 * `scripts` never run at all. Composer activates a plugin right after writing
 * its files and again whenever it loads plugins, so a library that is already
 * present costs one file check.
 *
 * A failure throws, which makes Composer roll the package back rather than
 * leave an installation that cannot load.
 */
final class NativeLibraryPlugin implements PluginInterface
{
    public function activate(Composer $composer, IOInterface $io): void
    {
        $package = $composer->getRepositoryManager()->getLocalRepository()->findPackage(Binding::PACKAGE, '*');
        if ($package === null) {
            return;
        }

        $http = $composer->getLoop()->getHttpDownloader();
        $installed = Binding::obtain(
            Binding::platform(),
            $package->getPrettyVersion(),
            static fn (string $url): string => (string) $http->get($url)->getBody(),
        );
        if ($installed !== null) {
            $io->write(sprintf('<info>%s: fetched libflz_core_ffi into %s</info>', Binding::PACKAGE, $installed));
        }
    }

    public function deactivate(Composer $composer, IOInterface $io): void
    {
    }

    public function uninstall(Composer $composer, IOInterface $io): void
    {
    }
}
