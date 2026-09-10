// How the SDK finds its native binary: the platforms it publishes for, what
// the release produces for them, and what an installed package does when it
// resolved one or none.
//
// Every other suite here consumes the SDK through a `file:` link to the working
// tree, where `native/flz_core_ffi.node` is always present. That link is why a
// package declaring five per-platform binaries that nothing produced stayed
// green for as long as it did: from inside the tree there is nothing to see.
// These tests pack the package and install it somewhere else, which is the only
// vantage point the defect is visible from.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { nativeCandidates, nativePackageName, nativeResolutionError } from '../dist/esm/native.js';
import { NATIVE_PLATFORMS, SDK_PACKAGE_NAME, platformPackageName } from '../dist/esm/platforms.js';

const here = dirname(fileURLToPath(import.meta.url));
const sdkRoot = resolve(here, '..');
const repoRoot = resolve(sdkRoot, '../..');
const manifest = JSON.parse(readFileSync(resolve(sdkRoot, 'package.json'), 'utf8'));

const npm = (args, cwd) =>
  execFileSync('npm', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });

/** `npm pack` over a directory, answering the tarball it wrote. */
const pack = (dir, dest) =>
  resolve(dest, npm(['pack', '--pack-destination', dest, dir], dir).trim());

/** The release's own packaging step, run the way the workflow runs it. */
const packagePlatforms = (addonDir, outDir, ...flags) =>
  execFileSync(
    process.execPath,
    [resolve(sdkRoot, 'scripts/platform-package.mjs'), addonDir, outDir, ...flags],
    { cwd: sdkRoot, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
  );

/** The one platform this host can actually install and load. */
const host = NATIVE_PLATFORMS.find(
  (platform) => platform.os === process.platform && platform.cpu === process.arch,
);

test('every published platform has a package name, and nothing else does', () => {
  // Derived from the table rather than transcribed from it: a transcription
  // passes over a platform the table has and the assertion forgot, which is
  // the drift the shared table exists to make impossible.
  for (const platform of NATIVE_PLATFORMS) {
    assert.equal(
      nativePackageName(platform.os, platform.cpu),
      platformPackageName(platform.suffix),
    );
  }
  assert.equal(nativePackageName('sunos', 'sparc'), null);
});

test('a supported platform resolves one binary package, not five', () => {
  const candidates = nativeCandidates('linux', 'x64', undefined);
  assert.equal(candidates[0], platformPackageName('linux-x64-gnu'));
  assert.equal(
    candidates.filter((candidate) => candidate.startsWith(`${SDK_PACKAGE_NAME}-`)).length,
    1,
    'exactly one per-platform package is consulted',
  );
  assert.match(candidates.at(-1), /native\/flz_core_ffi\.node$/);
});

test('an unsupported platform is told what a source build needs', () => {
  const candidates = nativeCandidates('sunos', 'sparc', undefined);
  assert.deepEqual(
    candidates.filter((candidate) => candidate.startsWith(`${SDK_PACKAGE_NAME}-`)),
    [],
    'no per-platform package is published for this platform',
  );
  const error = nativeResolutionError(candidates, 'sunos', 'sparc');
  assert.match(error.message, /No prebuilt binary is published/);
  assert.match(error.message, /cargo build -p flz_core_ffi --features napi/);
  assert.match(error.message, /FLEZHA_NATIVE_BINARY/);
});

test('the build produces a package for every platform the SDK declares', () => {
  const scratch = mkdtempSync(resolve(tmpdir(), 'flezha-pack-'));
  try {
    for (const platform of NATIVE_PLATFORMS) {
      const staged = resolve(scratch, 'staging', platform.suffix);
      mkdirSync(staged, { recursive: true });
      writeFileSync(resolve(staged, 'flz_core_ffi.node'), '');
    }
    // The packaging step is also the check: it refuses unless the declared
    // optional dependencies and the platform table are one set at one version,
    // so running it here is what asserts that rule rather than restating it.
    packagePlatforms(resolve(scratch, 'staging'), resolve(scratch, 'out'), '--require-all');

    const produced = readdirSync(resolve(scratch, 'out')).sort();
    assert.deepEqual(
      produced,
      Object.keys(manifest.optionalDependencies)
        .map((name) => name.replace('/', '-'))
        .sort(),
      'one package per declared optional dependency, and no more',
    );
    for (const directory of produced) {
      const written = JSON.parse(
        readFileSync(resolve(scratch, 'out', directory, 'package.json'), 'utf8'),
      );
      assert.equal(
        manifest.optionalDependencies[written.name],
        written.version,
        `${written.name} must be published at the version the parent declares`,
      );
    }
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
});

test('every declared platform package has a build arm that produces it', () => {
  // The workflow's matrix is the build. A platform declared here with no arm
  // there is an optional dependency the release cannot publish, which npm then
  // skips in silence at install time.
  const workflow = readFileSync(resolve(repoRoot, '.github/workflows/node-sdk.yml'), 'utf8');
  const arms = [...workflow.matchAll(/^\s+package:\s*(\S+)$/gm)].map((match) => match[1]);
  assert.deepEqual(
    arms.sort(),
    NATIVE_PLATFORMS.map((platform) => platform.suffix).sort(),
    'the build matrix and the platform table name the same platforms',
  );
  for (const platform of NATIVE_PLATFORMS) {
    assert.ok(
      workflow.includes(`target: ${platform.target}`),
      `no build arm targets ${platform.target}, which ${platform.suffix} is built from`,
    );
  }
});

test('packaging refuses a build that is missing a platform', () => {
  const scratch = mkdtempSync(resolve(tmpdir(), 'flezha-pack-'));
  try {
    // One platform staged out of five: exactly the shape a release must not
    // publish, because the parent declares all of them.
    mkdirSync(resolve(scratch, 'staging', NATIVE_PLATFORMS[0].suffix), { recursive: true });
    writeFileSync(resolve(scratch, 'staging', NATIVE_PLATFORMS[0].suffix, 'flz_core_ffi.node'), '');
    assert.throws(
      () => packagePlatforms(resolve(scratch, 'staging'), resolve(scratch, 'out'), '--require-all'),
      (error) => {
        const said = `${error.stdout ?? ''}${error.stderr ?? ''}`;
        return (
          /4 of 5 platform packages have no addon/.test(said) &&
          said.includes(NATIVE_PLATFORMS[1].suffix)
        );
      },
    );
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
});

test('the packed parent carries no binary and the README its file list names', () => {
  const scratch = mkdtempSync(resolve(tmpdir(), 'flezha-pack-'));
  try {
    const listed = execFileSync('tar', ['tzf', pack(sdkRoot, scratch)], { encoding: 'utf8' });
    assert.deepEqual(
      listed.split('\n').filter((entry) => entry.endsWith('.node')),
      [],
      'the parent ships no native binary: the source fallback is a checkout path only',
    );
    assert.ok(listed.includes('package/README.md'), 'the file list names a README that must exist');
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
});

test('the README names every platform package the SDK declares', () => {
  const readme = readFileSync(resolve(sdkRoot, 'README.md'), 'utf8');
  for (const platform of NATIVE_PLATFORMS) {
    const name = platformPackageName(platform.suffix);
    assert.ok(readme.includes(name), `README.md does not name ${name}`);
  }
});

/**
 * Install the packed parent into a scratch project outside the repository,
 * optionally alongside this host's platform package, and load the core there.
 * Answers what the probe printed, or the error it refused with.
 */
function installAndLoad({ withPlatformPackage }) {
  const scratch = mkdtempSync(resolve(tmpdir(), 'flezha-install-'));
  try {
    const tarballs = [pack(sdkRoot, scratch)];

    if (withPlatformPackage) {
      const staged = resolve(scratch, 'staging', host.suffix);
      mkdirSync(staged, { recursive: true });
      cpSync(resolve(sdkRoot, 'native/flz_core_ffi.node'), resolve(staged, 'flz_core_ffi.node'));
      packagePlatforms(resolve(scratch, 'staging'), resolve(scratch, 'packages'));
      const built = resolve(
        scratch,
        'packages',
        platformPackageName(host.suffix).replace('/', '-'),
      );
      tarballs.push(pack(built, scratch));
    }

    const app = resolve(scratch, 'app');
    mkdirSync(app, { recursive: true });
    writeFileSync(
      resolve(app, 'package.json'),
      `${JSON.stringify({ name: 'probe', private: true, version: '1.0.0', type: 'module' })}\n`,
    );
    // Not a `file:` link: a link would resolve the working tree's `native/`
    // directory and prove nothing. `--prefer-offline` keeps the registry out
    // of the loop for everything the SDK's own install already cached.
    const installed = npm(
      ['install', '--no-audit', '--no-fund', '--prefer-offline', ...tarballs],
      app,
    );

    writeFileSync(
      resolve(app, 'probe.mjs'),
      "import { loadNative } from '@flezha/sdk';\n" +
        'try {\n' +
        '  console.log(JSON.stringify({ ok: true, schema: loadNative().schemaVersion() }));\n' +
        '} catch (error) {\n' +
        '  console.log(JSON.stringify({ ok: false, message: error.message }));\n' +
        '}\n',
    );
    const probed = execFileSync(process.execPath, ['probe.mjs'], { cwd: app, encoding: 'utf8' });
    return { installed, ...JSON.parse(probed) };
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
}

test('an install that resolved its platform package loads the core', { skip: !host }, () => {
  const answer = installAndLoad({ withPlatformPackage: true });
  assert.ok(answer.ok, `the installed package refused to load: ${answer.message}`);
  assert.equal(typeof answer.schema, 'number');
});

test('an install that resolved nothing refuses, naming what is missing', { skip: !host }, () => {
  const answer = installAndLoad({ withPlatformPackage: false });

  // The silence this whole change exists for: npm reports an unresolvable
  // optional dependency nowhere, so the install itself looks healthy.
  assert.doesNotMatch(answer.installed, new RegExp(platformPackageName(host.suffix)));

  assert.equal(answer.ok, false, 'a package with no binary must not appear to load');
  assert.match(answer.message, new RegExp(`${process.platform}-${process.arch}`));
  assert.ok(answer.message.includes(platformPackageName(host.suffix)));
  assert.match(answer.message, /cargo build -p flz_core_ffi --features napi --release/);
  assert.match(answer.message, /FLEZHA_NATIVE_BINARY/);
  assert.doesNotMatch(
    answer.message,
    /Require stack/,
    'the refusal replaces the loader diagnostic rather than reprinting it',
  );
});
