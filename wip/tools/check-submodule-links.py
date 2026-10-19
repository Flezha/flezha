"""Dead links in the Markdown the SDK and plugin submodules track.

The citation gate reads only what the core tracks, so a submodule's README is
judged nowhere else. A relative link must reach a file from the citing page,
without leaving the submodule, since a submodule is published as a repository
of its own where a path into the core is a 404. A link into a Flezha repository
on GitHub must name a path that repository holds here: the core's own tree, or
the submodule its URL names.

    python3 -B wip/tools/check-submodule-links.py

Prints one line per dead link and exits 1 when there is any.
"""
import configparser
import os
import re
import subprocess
import sys

ROOT = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True,
                      text=True, check=True).stdout.strip()
os.chdir(ROOT)

modules = configparser.ConfigParser()
modules.read(".gitmodules")
paths = [modules[s]["path"] for s in modules.sections()
         if modules[s]["path"].startswith(("sdk/", "plugins/"))]
# The GitHub repository name of each submodule, so a URL into one resolves here.
repo_dir = {"flezha": "."}
for s in modules.sections():
    name = re.sub(r"\.git$", "", modules[s]["url"].rsplit("/", 1)[-1]).lower()
    repo_dir[name] = modules[s]["path"]

LINK = re.compile(r'(?<!!)\[[^\]]*\]\(([^)\s]+)(?:\s+"[^"]*")?\)')
FENCE = re.compile(r"^\s*(```|~~~)")
GITHUB = re.compile(r"^https://github\.com/flezha/([A-Za-z0-9_.-]+)/(?:blob|tree)/main/([^#?]*)", re.I)

dead = []
for sub in paths:
    files = subprocess.run(["git", "-C", sub, "ls-files", "*.md"], capture_output=True,
                           text=True).stdout.split()
    for rel in files:
        page = os.path.join(sub, rel)
        if not os.path.exists(page):
            continue
        in_fence = False
        for number, line in enumerate(open(page, errors="replace"), 1):
            if FENCE.match(line):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            for target in LINK.findall(line):
                found = GITHUB.match(target)
                if found:
                    base = repo_dir.get(found.group(1).lower())
                    if base is None:
                        continue
                    resolved = os.path.normpath(os.path.join(base, found.group(2)))
                elif re.match(r"^[a-z]+:", target) or target.startswith(("#", "<")):
                    continue
                else:
                    path = target.split("#")[0]
                    if not path:
                        continue
                    resolved = os.path.normpath(os.path.join(os.path.dirname(page), path))
                    if not (resolved + "/").startswith(sub + "/"):
                        dead.append(f"{page}:{number}: {target} (leaves {sub})")
                        continue
                if not os.path.exists(resolved):
                    dead.append(f"{page}:{number}: {target}")

for row in dead:
    print(row)
print(f"{len(dead)} dead links", file=sys.stderr)
sys.exit(1 if dead else 0)
