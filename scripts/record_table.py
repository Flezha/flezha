"""Reading a published record: a markdown table found by its header row.

The publication-readiness and feature-maturity gates each judge a table in a
document under `docs/publishing/`. A table is found by its header rather than
by position, because a record may carry more than one table and a gate that
took "the first table" would silently read the wrong one the day someone
reorders the document.
"""

import re

# A published record may not carry a coordinate that only exists on a
# maintainer's box or in a working document the repository does not track.
FORBIDDEN = (
    (re.compile(r"(?:/home/|/Users/|~/)[A-Za-z0-9._-]+"), "a home-directory path"),
    (re.compile(r"\bK[BI]-\d+\b"), "a defect-ledger id"),
)


def fields(line):
    """The fields of a markdown table row, outer pipes stripped."""
    return [c.strip() for c in line.strip().strip("|").split("|")]


def rows(lines, columns):
    """Every row under the header spelling `columns`, or None when no header does.

    A row is a dict keyed by column with its 1-based `lineno`; a row of the
    wrong width carries `_width` and its first field as `id` instead, so the
    caller reports it rather than reading a shifted cell.
    """
    start = next((i for i, line in enumerate(lines)
                  if line.startswith("|") and tuple(fields(line)) == tuple(columns)), None)
    if start is None:
        return None
    found = []
    # +2 skips the header and the `---` separator beneath it.
    for lineno, line in enumerate(lines[start + 2:], start=start + 3):
        if not line.startswith("|"):
            break
        row = fields(line)
        if len(row) != len(columns):
            found.append({"lineno": lineno, "_width": len(row), "id": row[0] if row else "?"})
            continue
        found.append({**dict(zip(columns, row)), "lineno": lineno})
    return found


def forbidden(path, lines):
    """One finding per coordinate the record may not carry."""
    return [f"{path}:{lineno}: the record carries {what} ({found.group(0)})"
            for lineno, line in enumerate(lines, start=1)
            for pattern, what in FORBIDDEN
            for found in [pattern.search(line)] if found]
