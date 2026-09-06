# Contributing to Flezha

## Contributor License Agreement

Every contribution requires a signed [Individual CLA](CLA.md). The
cla-assistant bot asks for the signature on your first pull request; signing
once covers all later contributions. Your contribution is licensed under the
license of the path it lands in — MPL-2.0 or Apache-2.0 per
[`REUSE.toml`](REUSE.toml) — and the CLA is what lets the project keep that
split coherent over time.

## Before opening a pull request

Run the same gates CI runs:

```bash
python3 scripts/naming_gate.py --run @naming    # every gate CI's naming job runs
python3 scripts/naming_gate.py --run @format    # formatters, reuse lint, notices
python3 scripts/naming_gate.py --list           # every declared gate and its command
```

Every gate is a row in `scripts/naming_gate.py`, and `--run` is the one reader.
CI calls the same runner, so a gate cannot exist and go uncalled: adding one is
a row, not a row plus a workflow step somebody has to remember. `--run <id>`
runs one, and each gate's own script still runs on its own when that is what you
want.

`check-layer-direction.sh` reads `lib/flezha/` against the controller's
dependency order: a file in one directory may not name a module whose own file
lives in a higher one, and every module should live in some directory. A
module's layer is where its FILE sits, not what it is called, so moving a file
into the directory that owns its feature is what closes an entry.
`scripts/layer-direction-baseline.txt` records what the tree held when the gate
landed — the gate fails on anything outside that list and on any entry the tree
no longer holds, so the list may only shrink. A directory the gate's layer
table does not rank makes it exit 3, the gate with no answer: rank the new
directory in the script before adding code to it.

The runner reports every gate it can and ends on one of three non-zero
statuses: **1** when a gate found something, **2** when a tool a gate needs is
not installed on your box, and **3** when a tool ran and did not finish, or a
gate reached no verdict. The summary names which gates landed where, and every
other gate is still reported whichever it is. Only **1** says anything about
your source: on **2** and **3** that gate has checked nothing, so neither is a
pass. CI fails on all three; your box may choose to skip on **2**.

Two repo-wide rules that are easy to miss:

- **One change, every SDK.** Any surface all SDKs carry — declarations, wire
  protocol, defaults, bench and e2e fixtures — changes in Rust, Java and Elixir
  in the same change, or the change names what it defers. Normative text:
  `docs/architecture/runner.md`.
- **No build artifact is committed.** Native libraries and fat jars are build
  output; `scripts/check-build-artifacts.sh` fails a pull request that tracks
  one. Build the host triple once with `scripts/build-native.sh` and every Java
  test path works offline — see README "Building".

Keep a change to one concern.

## Security issues

Do not open a public issue — see [SECURITY.md](SECURITY.md).
