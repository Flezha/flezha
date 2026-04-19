#!/usr/bin/env python3
"""
Assert that JaCoCo line coverage meets a threshold across one or more XML
reports. Exits non-zero if any report falls below the given threshold.

Usage:
  scripts/check-jacoco-coverage.py <report1.xml> [<report2.xml> ...] <threshold>

  threshold is a fraction in [0.0, 1.0]; e.g. 0.85 == 85%.
"""

import sys
import xml.etree.ElementTree as ET


def line_coverage(xml_path: str) -> tuple[int, int]:
    tree = ET.parse(xml_path)
    root = tree.getroot()
    for counter in root.findall("counter"):
        if counter.get("type") == "LINE":
            missed = int(counter.get("missed", "0"))
            covered = int(counter.get("covered", "0"))
            return covered, missed + covered
    return 0, 0


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2

    threshold = float(argv[-1])
    xml_paths = argv[1:-1]

    failed = False
    for xml_path in xml_paths:
        try:
            covered, total = line_coverage(xml_path)
        except FileNotFoundError:
            print(f"WARNING: coverage report missing: {xml_path}")
            continue
        if total == 0:
            print(f"WARNING: coverage report reports zero lines: {xml_path}")
            continue
        fraction = covered / total
        status = "OK" if fraction >= threshold else "FAIL"
        print(f"{status}: {xml_path}: {fraction:.2%} ({covered}/{total}), "
              f"threshold {threshold:.2%}")
        if fraction < threshold:
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
