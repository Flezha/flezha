#!/usr/bin/env bash
# Production lines per repository, the measure a campaign's line budget is held
# to. Prints one line per repository or crate and a total.
#
# Left out of every row:
# - tests, fixtures, examples, benches and docs, by path (EXCLUDE);
# - generated files: `@generated` in the first five lines (prost output);
# - Rust test code: items under `#[cfg(test)]` or `#[cfg(all(test, ..))]`, and
#   every file such an item declares as `mod x;` (with its `#[path]`, if any),
#   together with that file's own submodule directory. `cfg(any(test, ..))` is
#   counted, since a feature can build it into production.
set -euo pipefail
cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
export LC_ALL=C

EXCLUDE='(^|/)(test|tests|spec|fixtures|examples?|benches)/|_test\.|Test\.java$|\.md$|^flezha-sdk-examples/'

# Reads the file list on stdin, prints the lines of it that are not production.
# Strings and comments are blanked before braces are matched.
NONPROD_AWK='
BEGIN { while ((getline f < "/dev/stdin") > 0) ARGV[ARGC++] = f }
FNR == 1 { rs = FILENAME ~ /\.rs$/; gen = 0; st = 0; item = 0; seen[FILENAME] = 1 }
FNR <= 5 && /@generated/ { gen = gen_of[FILENAME] = 1 }
{ lines[FILENAME] = FNR }
!rs && !gen && FNR >= 5 { nextfile }
rs && !gen { rust_line() }
END {
  for (k = 1; k <= ndecl; k++) mark(resolve(dfile[k], dname[k], dpath[k]))
  for (f in seen) n += (gen_of[f] || test_file(f)) ? lines[f] : inline[f]
  print n + 0
}

function rust_line(   code) {
  code = lex($0)
  if (!item && match(code, /#\[cfg\((test\)|all\(test[ ,)])/)) {
    item = 1; depth = nest = 0; head = ""; ipath = ""; code = substr(code, RSTART)
  }
  if (!item) return
  inline[FILENAME]++
  if (depth == 0 && match($0, /#\[path *= *"[^"]*"\]/)) {
    ipath = substr($0, RSTART, RLENGTH); sub(/^[^"]*"/, "", ipath); sub(/".*/, "", ipath)
  }
  if (depth == 0) head = head " " code
  if (item_ends(code)) item = 0
}

# Advances the brace and bracket depth over one line of a test item; 1 once the
# item ends, recording it when it is a `mod x;` declaration.
function item_ends(code,   i, c) {
  for (i = 1; i <= length(code); i++) {
    c = substr(code, i, 1)
    if (c == "{") depth++
    else if (c == "(" || c == "[") nest++
    else if (c == ")" || c == "]") nest--
    else if (c == "}" && --depth <= 0) return 1
    else if (c == ";" && depth == 0 && nest <= 0) {
      if (match(head, /mod[ \t]+[A-Za-z0-9_]+[ \t]*;/)) {
        c = substr(head, RSTART + 3, RLENGTH - 3); gsub(/[ \t;]/, "", c)
        dfile[++ndecl] = FILENAME; dname[ndecl] = c; dpath[ndecl] = ipath
      }
      return 1
    }
  }
  return 0
}

# One line with string, char and comment contents removed; st carries a
# block comment (1), string (2) or raw string (3) over to the next line.
function lex(s,   out, c, t, p) {
  out = ""
  while (s != "") {
    if (st == 1) {
      if (!match(s, /\/\*|\*\//)) break
      t = substr(s, RSTART, 2); s = substr(s, RSTART + 2)
      if (t == "/*") bd++; else if (--bd == 0) st = 0
    } else if (st == 2) {
      if (!match(s, /\\.|"/)) break
      t = substr(s, RSTART, RLENGTH); s = substr(s, RSTART + RLENGTH)
      if (t == "\"") st = 0
    } else if (st == 3) {
      if (!(p = index(s, closer))) break
      s = substr(s, p + length(closer)); st = 0
    } else {
      if (!match(s, /["\047\/]/)) return out s
      out = out substr(s, 1, RSTART - 1); c = substr(s, RSTART, 1); s = substr(s, RSTART)
      if (c == "\"") {
        st = 2; s = substr(s, 2)
        if (match(out, /(^|[^A-Za-z0-9_])b?r#*$/)) {
          st = 3; t = substr(out, RSTART, RLENGTH); gsub(/[^#]/, "", t); closer = "\"" t
        }
      } else if (c == "/") {
        if (substr(s, 1, 2) == "//") return out
        if (substr(s, 1, 2) == "/*") { st = 1; bd = 1; s = substr(s, 3) }
        else { out = out "/"; s = substr(s, 2) }
      } else if (match(s, /^\047(\\[^\047]+|\\\047|[^\\\047]|[\200-\377]+)\047/)) {
        s = substr(s, RLENGTH + 1)
      } else { out = out c; s = substr(s, 2) }
    }
  }
  return out
}

function dir(f) { return f ~ /\// ? substr(f, 1, match(f, /\/[^\/]*$/) - 1) : "" }
function join(d, x) { return d == "" ? x : d "/" x }

function resolve(f, name, path,   d, base) {
  d = dir(f); base = substr(f, length(d) + (d != "") + 1)
  if (path != "") return norm(join(d, path))
  if (base !~ /^(mod|lib|main)\.rs$/) d = join(d, substr(base, 1, length(base) - 3))
  return (join(d, name ".rs") in seen) ? join(d, name ".rs") : join(d, name "/mod.rs")
}

function norm(p,   parts, n, i, out, k) {
  n = split(p, parts, "/"); k = 0
  for (i = 1; i <= n; i++) {
    if (parts[i] == "..") k--
    else if (parts[i] != "." && parts[i] != "") out[++k] = parts[i]
  }
  p = out[1]; for (i = 2; i <= k; i++) p = p "/" out[i]
  return p
}

function mark(t) {
  testf[t] = 1
  tree[t ~ /\/mod\.rs$/ ? dir(t) "/" : substr(t, 1, length(t) - 3) "/"] = 1
}

function test_file(f,   p) {
  if (f in testf) return 1
  for (p in tree) if (index(f, p) == 1) return 1
  return 0
}
'

count() {
  local repo=$1; shift
  local files
  files=$(cd "$repo" && git ls-files -- "$@" | grep -Ev "$EXCLUDE" || true)
  (cd "$repo" && echo $(($(xargs -r cat <<<"$files" | wc -l) - $(awk "$NONPROD_AWK" <<<"$files"))))
}

total=0
row() { printf '%-28s %7d\n' "$1" "$2"; total=$((total + $2)); }

row core-lib            "$(count . lib)"
row core-flz_core       "$(count . flz_core/src)"
row core-flz_core_ffi   "$(count . flz_core_ffi/src)"
row core-flz_nif        "$(count . flz_nif/src)"
row core-flz_transport  "$(count . flz_transport/src)"
row core-cli            "$(count . cli/src)"
row sdk-elixir          "$(count sdk/elixir lib)"
row sdk-elixir-native   "$(count sdk/elixir ':(glob)native/*/src/**')"
row sdk-java            "$(count sdk/java ':(glob)*/src/main/**')"
row sdk-node            "$(count sdk/node src)"
row sdk-php             "$(count sdk/php src)"
row sdk-python          "$(count sdk/python src)"
row sdk-ruby            "$(count sdk/ruby lib)"
row sdk-rust            "$(count sdk/rust src)"
row sdk-rust-macros     "$(count sdk/rust macros/src)"
for p in plugins/*/; do
  row "$(basename "$p")" "$(count "$p" lib)"
done
printf '%-28s %7d\n' total "$total"
