#!/usr/bin/env bash
# Self-test for the immutable-tag guard in scripts/publish-image.sh, with a
# stub `docker` on PATH. Fails when: a prod publish overwrites an already
# published per-arch version tag, refuses a fresh one, or ignores REPUBLISH=1.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/bin"

# The stub registry: PUBLISHED seeds what exists, `push` adds to it, and
# `manifest inspect` answers from both. `build` leaves a marker; the rest of
# the docker surface the script touches succeeds silently.
cat > "$WORK/bin/docker" <<'STUB'
#!/usr/bin/env bash
case "$1 $2" in
  "manifest inspect")
    for ref in $PUBLISHED $(cat "$PUSHED" 2>/dev/null); do [ "$ref" = "$3" ] && exit 0; done
    exit 1 ;;
  "push "*)          echo "$2" >> "$PUSHED" ;;
  "build "*)         echo built >> "$BUILD_LOG" ;;
  "image inspect")   echo 1048576 ;;
esac
exit 0
STUB
chmod +x "$WORK/bin/docker"

version="$(sed -n 's/^ *version: *"\([^"]*\)".*/\1/p' "$ROOT/mix.exs" | head -1)"
case "$(uname -m)" in aarch64|arm64) arch=arm64 ;; *) arch=amd64 ;; esac
tag="flezha/flezha:$version-$arch"

# publish <published-refs> [REPUBLISH]: run the script against the stub.
publish() {
  : > "$WORK/build.log"; : > "$WORK/pushed"
  PATH="$WORK/bin:$PATH" BUILD_LOG="$WORK/build.log" PUSHED="$WORK/pushed" \
    PUBLISHED="$1" REPUBLISH="${2:-}" \
    bash "$ROOT/scripts/publish-image.sh" --prod >"$WORK/out" 2>&1
}
fail() { echo "[publish-image-test] FAIL: $*" >&2; cat "$WORK/out" >&2; exit 1; }

publish "$tag" && fail "published over the existing $tag"
grep -q 'already published' "$WORK/out" || fail "refusal did not say the tag exists"
[ ! -s "$WORK/build.log" ] || fail "built before refusing"

publish "" || fail "fresh $tag was refused"
[ -s "$WORK/build.log" ] || fail "fresh publish did not build"
grep -q "^$tag$" "$WORK/pushed" || fail "fresh publish did not push $tag"

publish "$tag" 1 || fail "REPUBLISH=1 was refused"
[ -s "$WORK/build.log" ] || fail "REPUBLISH=1 did not build"

echo "publish-image-test: OK (refuses $tag when published, builds when fresh, REPUBLISH=1 overrides)"
