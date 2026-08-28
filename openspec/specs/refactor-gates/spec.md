# refactor-gates Specification

## Purpose
TBD - created by archiving change refactor-0-baseline. Update Purpose after archive.
## Requirements
### Requirement: The repository measures its own structure and ratchets it

The repository SHALL carry one measuring script, `scripts/refactor_metrics.py`, that reports per area: production and test line counts, comment density split into doc and plain comments, the count of history-narrating comment markers, files over 800 lines, functions over 50 lines, production names over 32 characters, duplicate-code clusters, Elixir dependency cycles, the share of `async: false` ExUnit files, sleep and `refute_receive` counts in tests, lint warning counts, and suite wall-clock when a log is supplied. It SHALL write a baseline (`docs/refactor/baseline.json`), render a target list (`docs/refactor/targets.md`), and check the current tree against the baseline.

#### Scenario: A ratcheted metric regresses in a blocking area

- **WHEN** `scripts/refactor_metrics.py --check` runs on a tree where an area listed as `blocking` in the baseline has a higher value for any ratcheted metric than the baseline records
- **THEN** the script SHALL exit non-zero and name the area, the metric, and the file that caused the regression

#### Scenario: A measuring-only area regresses

- **WHEN** `--check` runs and the regressing area is not listed as `blocking`
- **THEN** the script SHALL print the regression and exit zero

#### Scenario: The baseline records where it was measured

- **WHEN** `--write-baseline` runs
- **THEN** the baseline SHALL carry the hostname, CPU model and governor beside every timing or bench number, and a later `--check` SHALL compare timings only against a baseline from the same hostname

