/**
 * The three things that differ between this package's ESM and CommonJS builds
 * and cannot be written once in TypeScript: the directory the built files sit
 * in, a `require` that can load a native `.node` addon, and an `import()` that
 * the CommonJS compile leaves alone.
 *
 * The implementations are hand-written JavaScript, one per module system,
 * copied into each build output as `runtime-shim.js`. Everything else in the
 * package is compiled from this one source tree.
 */

/** The directory the running build's files sit in. */
export declare const runtimeDir: string;

/** A `require` bound to that directory, for loading the native addon. */
export declare function requireNative(specifier: string): unknown;

/**
 * A dynamic `import()`. The CommonJS compile rewrites one written in TypeScript
 * into `require()`, which cannot load the `file:` URL a handler path becomes.
 */
export declare function importSpecifier(specifier: string): Promise<unknown>;
