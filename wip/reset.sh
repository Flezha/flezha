#!/usr/bin/env bash
# Close the current campaign and start a clean one, keeping the structure.
#
#   wip/reset.sh <new-campaign-id>
#
# It archives the campaign, drains the issue inbox into the standing bug list,
# and restores the boards from wip/templates/. INVARIANTS.md, templates/,
# briefs/ and the scripts are never touched.
#
# It refuses to run while work is in flight, because a reset mid-item loses the
# only record of what a worker was doing.
set -euo pipefail

cd "$(dirname "$0")/.."
WIP=wip
NEW="${1:-}"
[ -n "$NEW" ] || { echo "usage: wip/reset.sh <new-campaign-id>"; exit 2; }

# 1. Refuse if the tree is dirty beyond the editor's own file, or if a row is
#    still in flight.
dirty=$(git status --porcelain | grep -v '\.vscode/settings.json' || true)
if [ -n "$dirty" ]; then
  echo "The tree is not clean. Commit or explain these first:"; echo "$dirty"; exit 1
fi
if [ -f "$WIP/WORK.md" ] &&
   grep -qE '\| *(reproducing|coding|review|verifying) *\|' "$WIP/WORK.md"; then
  echo "WORK.md still has a row in flight. Finish or park it before resetting."; exit 1
fi

OLD=unnamed
[ -f "$WIP/RULES.md" ] &&
  OLD=$(grep -m1 -oP '(?<=\*\*Campaign\*\*: `)[^`]+' "$WIP/RULES.md" || echo unnamed)
DEST="$WIP/archive/$(date +%Y-%m-%d)-$OLD"
mkdir -p "$DEST"

# 2. Drain the inbox into the standing list. Every row that is still open moves;
#    nothing lives in two places.
open_rows=$(python3 "$WIP/status.py" 2>/dev/null | awk '/^found/ {print $2}')
if [ "${open_rows:-0}" != "0" ]; then
  echo "NEW-ISSUES.md still holds $open_rows open row(s). Move them into"
  echo "docs/known-bugs.md first, then rerun. Draining is a judgement, not a copy."
  exit 1
fi

# 3. Archive the boards, restore the blanks.
# A missing board is not an error: this is also how a fresh clone, which carries
# the structure and none of the work, gets its first campaign.
moved=0
for f in RULES STATE WORK DONE NEW-ISSUES LOG DECISIONS; do
  if [ -f "$WIP/$f.md" ]; then mv "$WIP/$f.md" "$DEST/$f.md"; moved=1; fi
  cp "$WIP/templates/$f.md" "$WIP/$f.md"
done

sed -i "s/\*\*Campaign\*\*: \`<id>\`/**Campaign**: \`$NEW\`/" "$WIP/RULES.md" "$WIP/STATE.md"
sed -i "s/\*\*Opened\*\*: \`<YYYY-MM-DD>\`/**Opened**: \`$(date +%F)\`/" "$WIP/RULES.md"

if [ "$moved" = 1 ]; then
  echo "Archived $OLD to $DEST"
else
  rmdir "$DEST" 2>/dev/null || true
  echo "No previous campaign to archive: the boards were missing, as in a fresh clone."
fi
echo "Opened $NEW. Fill wip/RULES.md, put rows in wip/WORK.md, then run /cx-wip."
