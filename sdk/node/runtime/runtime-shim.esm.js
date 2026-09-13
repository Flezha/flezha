// ESM half of the runtime shim. See src/runtime-shim.d.ts.
import { createRequire } from 'node:module';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

export const runtimeDir = dirname(fileURLToPath(import.meta.url));

const localRequire = createRequire(import.meta.url);

export function requireNative(specifier) {
  return localRequire(specifier);
}

export function importSpecifier(specifier) {
  return import(specifier);
}
