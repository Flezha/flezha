<!--
SPDX-FileCopyrightText: 2026 Anuar Alfetahe

SPDX-License-Identifier: MPL-2.0
-->

# Publication readiness

Publishing is a one-way door. While nothing is published, a wrong name costs a commit; the
day after, it costs a deprecation cycle, an accept-both parser and a migration note. The
standing rule that this project carries no legacy shims because the library is unpublished
is what makes much of its open work cheap, and **it expires on the day of the first public
release**. This record is where that expiry is written down.

Each entry below is a decision the first public release makes irreversible. It is not a
list of tasks: ordinary work is tracked wherever ordinary work is tracked, and an item that
is merely a good idea is not an entry here.

## The test

An entry is **blocking** when settling it differently after the first public release would
need at least one of:

- **(a)** an action outside the project's control, such as a registry operator's, a
  registrar's or another person's consent;
- **(b)** an act that cannot be undone, such as recalling a clone, un-disclosing a
  vulnerability, or reaching a copy someone already took;
- **(c)** a compatibility break for someone who already depends on the current answer.

Every other entry is **owed**. Owed is not unimportant: it means a later fix costs no more
than an early one, which is exactly the property that makes it safe to carry through the
door. An entry that fails all three tests is not an entry at all.

Every entry names its letter. That is the discipline: an author who cannot name the letter
has not shown the decision is one-way.

## How this record is read

The `class` column carries `blocking` or `owed`, and may add `decision` when the entry is a
choice rather than a task, or `settled` once it is closed. A `settled` entry names its
evidence in `settled-by`: a commit, a requirement or a file in the tree. An entry that says
settled and points nowhere can be checked by nobody but its author.

`scripts/check-publish-readiness.sh` reads this file. Its shape arm runs on every pull
request and judges the table's form alone. Its release arm runs on a version tag, before
any release artefact is produced, and refuses the release while any **blocking** entry is
open. An owed entry never stops a release, because if it did every entry would drift into
`owed` to get a release out.

This record states rules, not the instances that currently trip them. A published list of
one's own violations ages the moment one is fixed, and cites coordinates the tree may not
carry.

## The record

| id | class | door | rule | check | owner | settled-by |
|---|---|---|---|---|---|---|
| H1 | blocking | (b) a disclosed key cannot be un-disclosed | No key material is reachable from any ref. | Scan every blob reachable from every branch and tag for private-key and certificate headers; the result is empty. | maintainer | |
| H2 | blocking | (b) a published coordinate cannot be recalled | No production coordinate, endpoint, internal address or host name is reachable from any ref. | The same scan, for address and URL shapes, over blobs no longer present at the tip. | maintainer | |
| H3 | blocking | (b) a clone cannot be recalled | Material the repository excludes from the tip is excluded from the history, or the tip-only exclusion is deliberately accepted and recorded as such. | For each excluded path family, the log of additions across all refs is empty, or this record carries the acceptance. | maintainer | |
| H4 | blocking | (b) a rewrite that misses a lineage removes nothing | Every lineage in the object store is accounted for before any history rewrite, and the remote is confirmed to hold no unreferenced pre-rewrite objects. | The root commits and the branches and tags holding each lineage are enumerated and named; the remote's object count is confirmed after collection. | maintainer | |
| H5 | blocking | (b) a clone cannot be recalled | No tracked file names a filesystem path, host, product or organisation outside this project. | The tree greps clean for home-directory paths, for sibling-product names, and for captured output from a system that is not this one. | maintainer | |
| H6 | blocking · decision | (b) an identity in a published history is permanent | The author identity the history publishes is the one intended. | The distinct author identities across all commits are enumerated and each is intended. | owner | |
| N1 | blocking | (a) a registry name is granted by its user | Every name the project will ever publish under is claimed in one pass, on every registry, including the per-platform package names a split distribution will need later. The per-platform names a release now produces are `@flezha/sdk-linux-x64-gnu`, `@flezha/sdk-linux-arm64-gnu`, `@flezha/sdk-darwin-x64`, `@flezha/sdk-darwin-arm64` and `@flezha/sdk-win32-x64-msvc`, beside the `flezha` parent, the `flezha` wheel and the `flezha_sdk` Hex package. | For each registry the project will publish to, the name is held by us. | owner | |
| N2 | blocking | (a) a namespace is verified by the registry operator | The Java group coordinate is decided, and the namespace it needs is verifiable by whoever will publish it. | The group coordinate in the poms matches a namespace we can prove control of. | owner | |
| N3 | blocking | (a)(c) a container registry does not redirect a renamed repository | The container image namespace is decided before the first push. | No tracked file, manifest, document or specification names an image under a namespace we do not intend to keep. | owner | |
| N4 | blocking · decision | (c) every published name freezes a spelling | The product has one spelling, and each deliberate exception is recorded with its reason. | For every published package name, binary name, symbol prefix and namespace root, the spelling is the intended one. | owner | |
| N5 | owed | — | The hosting account or organisation is decided. The host keeps redirects for a renamed or transferred repository, so this is not one-way; what is permanent is the URL already frozen into published package metadata, which is B6. | The account or organisation that will hold the repository is named. | owner | |
| C1 | blocking | (c) after publication everything reachable is depended on | A written compatibility promise names, per surface, what is public and what is internal. | The promise exists, and for each surface a reader can tell which side a given name is on. Which features are stable and which are not yet settled is answered by `docs/publishing/maturity.md`; the public-versus-internal half is still open. | maintainer | |
| C2 | blocking · decision | (c) a version number promises what the next one may break | The first release's version and the versioning policy are decided and written down. | One document states the scheme and what a bump of each position promises. | owner | |
| C3 | blocking | (c) adding a version field once users run mixed versions is itself a flag day | Every negotiated or persisted format either carries a version and a refusal path, or is recorded as deliberately frozen. | For each negotiated and each persisted format, either a version field and a mismatch refusal exists, or this record says why none is needed. | maintainer | |
| C4 | blocking | (c) a wrong value in records already on users' disks cannot be corrected, only reinterpreted | No field written into a durable record carries a value that is not true. | For each field of the durable header, every writer sets it from its own source. | maintainer | |
| C5 | blocking | (c) a rename costs an alias, a duplicate emission or a broken dashboard | Every user-visible name has been reviewed once, deliberately, before the first release. In scope: command verbs, flags and exit codes; environment variables; exposed metric names; telemetry event names; default ports; default paths. | The review happened and its outcome is recorded. | maintainer | |
| C6 | blocking | (c) a name installed globally reaches every dependent project | The package claims no globally shared name it does not own. | Every build-tool task, plugin or extension point the package installs is under the project's own namespace. | maintainer | |
| B1 | blocking | (a)(c) a registry may refuse to publish while a source dependency is declared | Every dependency resolves from a published release of a package registry. | The manifest declares no dependency that resolves from a source host, and the package builds for publication. | maintainer | |
| B2 | blocking | (b) a wrong publish cannot be withdrawn | The first publication runs from a reviewed, repeatable job, not by hand. | A job exists that publishes each artefact, and it has been rehearsed against a scratch name. | maintainer | |
| B3 | owed | — | The bundled-versus-per-platform artefact shape is decided. A later split is a packaging break with a migration note, so it is expensive rather than one-way; the per-platform names it will need are registry entries and belong to N1. **Settled for Node, Python and PHP**: Node is per-platform, one binary per package, resolved through `optionalDependencies`; Python is one `abi3` wheel per platform and one free-threaded wheel per platform per supported free-threaded interpreter. Ruby is one precompiled gem per platform carrying one library per supported Ruby minor, with the source gem as the fallback that needs cargo and git; the minors move yearly, so that list is a standing maintenance line. Elixir is one socket-client archive per platform, which `rustler_precompiled` downloads when `flezha_sdk` compiles and verifies against the checksum file the package carries, with a source build as the fallback that needs a Rust toolchain. All of these are produced by `scripts/build-dist.sh` and attached to the release. PHP obtains one binary at install: its Composer plugin fetches the host's library from the release matching the package's version and verifies it against the release's SHA-256 manifest, because a package published from a tag cannot carry build output. Java keeps the bundled shape. | The shape is recorded, whenever it is chosen. | owner | |
| B4 | blocking · decision | (b)(c) withdrawing a published SDK breaks the people who took it | The first release names the SDKs it publishes and the ones it does not. | Every published matrix and document agrees with that list. | owner | |
| B5 | owed | — | One source of truth for the version, and a check that the artefacts agree. Painful to keep by hand, but a later fix costs no more than an early one. | One place states the version and a check refuses an artefact that disagrees. | maintainer | |
| B6 | owed | — | Package metadata is correct: description, readme, keywords, links, publish visibility, and the file list a package ships. Immutable per version, so a mistake is corrected by the next release rather than never. | For each package, the metadata resolves and the visibility is explicit. | maintainer | |
| L1 | blocking | (a) relicensing an unsigned contribution needs its author found again | Contribution-agreement automation is live before the repository can accept its first external contribution. | The bot is installed and a test pull request is blocked until signed. | maintainer | |
| L2 | blocking · decision | (a) every signatory would have to be asked again | The contribution agreement says whether the grant runs to a successor cell. | Read the agreement; the answer is present. | owner | |
| L3 | blocking | (b) a vulnerability reported in public cannot be un-published | A security contact receives mail, and private reporting is enabled, on the day the repository becomes public. | A message to the published address is received, and the private reporting channel accepts a report. | maintainer | |
| L4 | blocking | (b) copies already taken cannot be relicensed | Material the documentation tells a user to copy carries the licence intended for copied material. | For each example the docs present as a starting point, its effective licence is the SDK's. | maintainer | |
| L5 | owed | — | Trademark registration. The name is already reserved in the notice file and registration can follow publication. | The registration is filed, or the decision not to file is recorded. | owner | |
| R1 | owed | — | Tracked tooling runs for someone who is not the maintainer: no script requires one absolute checkout path, and no gate depends on a file the repository deliberately does not carry. | A fresh clone runs the tracked tooling without editing it. | maintainer | |
| R2 | owed | — | Every workflow declares least-privilege permissions. All runners are hosted and no untrusted trigger is used, so this is hardening rather than a door. | Each workflow declares a top-level permissions block. | maintainer | |
| R3 | owed | — | Community files exist: a code of conduct, issue and pull-request templates, and a contribution path. | The files are present and reachable from the readme. | maintainer | |
| R4 | owed | — | No test in the default paths reaches a live third-party endpoint, because every fork of a public repository inherits it. | The default test paths run with no outbound third-party call. | maintainer | |

## Decisions the owner owes

Six entries are choices rather than tasks, and one entry group is a choice about method.
They are collected here so they can be settled from one page.

| # | question | options and cost |
|---|---|---|
| H6 | the author identity in the published history | leave it (free, permanent) · rewrite it in the same pass as H1 to H4 (free now, since that pass is happening anyway) |
| H1 to H4 | how the history is published | one rewrite pass over every lineage, branch and tag (a few hours, invalidates every clone, needs doing once) · publish a new repository with a truncated history (cheapest and safest, loses the record of how the system got here) · publish as-is (not an option while H1 stands) |
| N2 | the Java group coordinate | a domain-derived coordinate (needs the domain, reads as a project) · an account-derived one (needs nothing, reads as personal, and is equally permanent) |
| N4 | the product's spelling | keep the split and record the rule (free, and every registry shows both) · unify before publishing (a rename across every registry name, free today, impossible after) |
| C2 | the first version number | stay pre-1.0 (keeps the freedom this project currently relies on) · release 1.0 (spends it on the first day) |
| L2 | whether the contribution grant runs to a successor cell | settle before the first external contribution (free) · after (every signatory asked again) |
| B4 | which SDKs the first release publishes | all of them (one compatibility promise per SDK, at once) · a named subset with the rest marked unreleased (adding later is additive) |

## Three checks this repository cannot answer

Whether a name is still free on a registry, whether a mailbox receives, and whether the
remote holds unreferenced objects are all true or false somewhere other than here. They are
written as checks anyway, with the owner as their owner, because an item that is only
verifiable elsewhere is still one-way.
