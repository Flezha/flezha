<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Ffi\Binding;
use ReflectionClass;

/**
 * The declared FFI surface against what the library actually exports.
 *
 * A shim that binds the ABI directly has no exported symbols of its own to
 * compare, so what stands in for that is this: every function the `cdef`
 * declares must exist in the built library, and the declaration must name no
 * entry point the ABI does not have. Both directions matter — a typo in a
 * declaration is a runtime failure at the first call rather than a link error,
 * which is the one way binding a C ABI from a dynamic language is weaker than
 * a compiled veneer, and this is what closes it.
 */
final class AbiSurfaceTest extends RepositoryTestCase
{
    /** @return list<string> the functions the shim declares */
    private function declared(): array
    {
        $cdef = (new ReflectionClass(Binding::class))->getConstant('CDEF');
        preg_match_all('/\b(flz_core_\w+)\s*\(/', (string) $cdef, $matches);
        $names = array_values(array_unique($matches[1]));
        sort($names);

        return $names;
    }

    /** @return list<string> the entry points `flz_core_ffi/src/lib.rs` exports */
    private function exported(): array
    {
        // Two files, because the C ABI is two: the general entry points and
        // the local-mode ones. Reading only the first reports a name the
        // crate does export as one it does not.
        $source = '';
        foreach (['flz_core_ffi/src/lib.rs', 'flz_core_ffi/src/local.rs'] as $file) {
            $source .= (string) file_get_contents($this->repositoryFile($file));
        }
        preg_match_all('/extern "C" fn (flz_core_\w+)/', $source, $matches);
        $names = array_values(array_unique($matches[1]));
        sort($names);

        return $names;
    }

    public function testTheShimDeclaresEveryAbiEntryPoint(): void
    {
        $missing = array_diff($this->exported(), $this->declared());

        self::assertSame([], array_values($missing), sprintf(
            'the ABI exports %s and the PHP shim declares none of them; a host that cannot '
            . 'reach a kernel capability the ABI has is a defect of the shim',
            implode(', ', $missing),
        ));
    }

    public function testTheShimDeclaresNothingTheAbiDoesNotExport(): void
    {
        $extra = array_diff($this->declared(), $this->exported());

        self::assertSame([], array_values($extra), sprintf(
            'the PHP shim declares %s, which the ABI does not export; the first call would fail '
            . 'at runtime rather than at load',
            implode(', ', $extra),
        ));
    }

    /** Every declared function resolves in the library this build loaded. */
    public function testEveryDeclaredFunctionIsCallable(): void
    {
        $binding = Binding::current();

        foreach ($this->declared() as $name) {
            self::assertTrue(
                is_callable([$binding, $name]),
                "$name is declared but the loaded library does not resolve it",
            );
        }
    }
}
