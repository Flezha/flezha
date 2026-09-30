# The capture helpers every guide walk shares; sourced by a walk that has set
# OUT (where its output goes), `page` (the page being walked) and `n` (the next
# step's number, reset to 0 per page).

step() { # name, command (run by bash, shown as typed)
  local file
  file=$(printf '%s/%s/%02d-%s.txt' "$OUT" "$page" "$n" "$1")
  n=$((n + 1))
  mkdir -p "$OUT/$page"
  { echo "\$ $2"; bash -c "$2" </dev/null 2>&1; } >"$file"
  echo "  $(basename "$file")"
}
note() { echo "$*" >>"$OUT/$page/notes.txt"; }

# Waits a bounded time for what a step reads next, so the capture is not a race.
until_ok() { # seconds, command
  local deadline=$((SECONDS + $1))
  while [ $SECONDS -lt "$deadline" ]; do bash -c "$2" </dev/null >/dev/null 2>&1 && return 0; sleep 1; done
  note "gave up after $1 s waiting for: $2"
}
