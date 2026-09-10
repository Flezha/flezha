/**
 * The platforms a prebuilt binary is published for, and every spelling of one.
 *
 * Two readers: `native.ts` resolves the installed package for the host it is
 * running on, and `scripts/platform-package.mjs` writes the packages the
 * release publishes. A platform added to one and forgotten in the other is an
 * install that silently falls through to a source build, so both derive from
 * the rows below.
 */

/** The parent package; every per-platform package is this name plus a suffix. */
export const SDK_PACKAGE_NAME = '@flezha/sdk';

export interface NativePlatform {
  /** The package-name suffix, and the directory the build stages under. */
  suffix: string;
  /** `process.platform`, which is also npm's `os` for the platform package. */
  os: NodeJS.Platform;
  /** `process.arch`, which is also npm's `cpu`. */
  cpu: string;
  /** npm's `libc`, on platforms that have more than one. */
  libc?: string;
  /** The Rust target the addon is built for. */
  target: string;
}

export const NATIVE_PLATFORMS: readonly NativePlatform[] = [
  {
    suffix: 'linux-x64-gnu',
    os: 'linux',
    cpu: 'x64',
    libc: 'glibc',
    target: 'x86_64-unknown-linux-gnu',
  },
  {
    suffix: 'linux-arm64-gnu',
    os: 'linux',
    cpu: 'arm64',
    libc: 'glibc',
    target: 'aarch64-unknown-linux-gnu',
  },
  { suffix: 'darwin-x64', os: 'darwin', cpu: 'x64', target: 'x86_64-apple-darwin' },
  { suffix: 'darwin-arm64', os: 'darwin', cpu: 'arm64', target: 'aarch64-apple-darwin' },
  { suffix: 'win32-x64-msvc', os: 'win32', cpu: 'x64', target: 'x86_64-pc-windows-msvc' },
];

/** The published package carrying one platform's binary. */
export function platformPackageName(suffix: string): string {
  return `${SDK_PACKAGE_NAME}-${suffix}`;
}

/** The row for a host, or `null` where no binary is published for it. */
export function nativePlatformFor(os: NodeJS.Platform, cpu: string): NativePlatform | null {
  return NATIVE_PLATFORMS.find((entry) => entry.os === os && entry.cpu === cpu) ?? null;
}
