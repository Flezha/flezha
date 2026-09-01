# Every SDK's test toolchain is pinned, and CI reads the pin

**Provenance.** Reconstructed after the fact from the landed commit `152c0afd`
and the archived change directory, which is untracked and exists on one machine.
Nothing here is a measurement this note took.

## The rule existed nowhere

Fixing Ruby's undeclared `minitest` exposed that the rule it conforms to is
stated in **no spec**: across ~100 spec directories only `controller-packaging`
and `code-formatting` mention dependency pinning at all. **Three SDKs failed the
unwritten rule.** The requirement now lives in `code-quality-baseline`, beside
"a dependency SHALL resolve from a published version", and covers four things:
declaration, pinning, **CI reading the pin**, and — since a test app is not an
SDK — any directory that commits a lockfile.

**A pin nothing reads stops nothing.** That is the half every one of these failed.

## Python — grep the pin, do not let the installer resolve a range

`pytest>=8.0` was declared in a PEP 735 dependency group that **nothing
installs**: CI ran `pip install --quiet pytest pyright`, so **the workflow line
was the effective manifest** and the tree had already drifted to pytest 9 against
a group saying 8. The versions are now exact in that group and CI resolves them
by grepping it — the shape `format.yml` already uses for the `ruff` pin out of
the same file.

**The grep is anchored on `==` deliberately**: relaxing a pin empties the
substitution and `pip install "pytest=="` exits 1, where `pip --group` would
install a range as willingly as a pin, **silently**, in the one cell this change
exists to close.

## PHP — pin in `require-dev`, and no committed lock

`phpunit` is pinned exactly in `require-dev`. `composer.lock` **stays
gitignored**, for two reasons: this is a **library**, whose consumers never read
its lock; and **one lock across an 8.1/8.4 matrix does not install** — the
ignored local lock held `symfony/console` v8.1.5 and eight siblings requiring
php >= 8.4.1.

## Node — the lock existed and nothing read it

The test path used `npm install`, which **may resolve past the lock and rewrite
it**, and CI discards the rewrite, so **what actually ran is recorded nowhere**.
Now `npm ci`. `examples/node` is an **application** and gains a lock, which is
the case where the answer inverts from Ruby's.

## `test/apps/testnet` was worse than any of them

A committed lock behind `npm ci || npm install` **at three sites** — which looks
correct and falls back to a fresh resolve **exactly when `ci` fails**, which is
when you most want to know. `npm ci --dry-run` passes there, so the fallback is
removed rather than papered over, and the docker image and three READMEs that
instructed the same resolving install now honour the lock too.

## A spec that the change makes false is corrected in the same change

`controller-packaging` described the image building with
`npm install --omit=dev`, which this change makes false. All three of its
scenarios were carried across the `MODIFIED` block and **proved by per-scenario
hash rather than by eye** — which is the standing remedy for the fact that a
`## MODIFIED Requirements` block replaces the whole requirement and deletes any
scenario not carried.

## Verification

Six naming checks, the gate self-test and metrics all exit 0. Python 161 passed,
PHP 129 tests, Node 100 plus typecheck and both examples.
