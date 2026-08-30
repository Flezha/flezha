# java-build-runs-twice — implementation notes

Every second Java build in a checkout failed. The change was filed against the
aggregator's missing sources; that is only what makes the failure total.

## The warning one line above the error is the whole mechanism

The failing run says it itself:

```
[WARNING] JAR will be empty - no content was marked for inclusion!
[ERROR] ... maven-shade-plugin:3.6.0:shade (default) on project flezha-sdk:
        Error creating shaded jar: duplicate entry: META-INF/THIRD_PARTY_NOTICES.md
```

Read the two runs side by side and the tell is not the warning — it fires on the
green run too — but the line that follows it:

```
run 1 (clean target/)   [WARNING] JAR will be empty …
                        [INFO] Building jar: …/flezha-sdk-0.1.0.jar     <- present
run 2 (same command)    [WARNING] JAR will be empty …
                                                                        <- absent
```

`maven-shade-plugin` **replaces the jar plugin's output file in place** — it
writes `…-shaded.jar`, renames the jar plugin's artifact to
`original-…jar`, and moves the shaded one onto the main artifact path. On the
next build `maven-jar-plugin` runs its up-to-date check against that path and
finds a 15 MB fat jar newer than everything it would package, so it skips.
`IncludeResourceTransformer` then attaches `META-INF/THIRD_PARTY_NOTICES.md` to a
jar that already carries one.

`forceCreation` is false by default, which is what makes the skip possible.

## "No sources" is not the condition — it only makes the skip permanent

`flezha-sdk` has no `src/main/java`, so the jar plugin has nothing to compare
and skips unconditionally. A module **with** sources skips too, whenever nothing
is newer than the fat jar shade left behind. `test/apps/java_runner` is exactly
that, and it carried the identical defect while exiting 0 on every run: its
filter excludes `META-INF/THIRD_PARTY_NOTICES.md` from *every* artifact,
including the project's own, so the re-shade is a fixpoint and nothing complains.
It is visible only in a byte count — after a second `package`,
`original-java_runner.jar` came out **16,082,694** bytes (the fat jar) instead of
**209,955** (the thin one). A jar that re-shades itself keeps whatever the last
build put in it, which is how a native binary deleted from the source tree
survives into the next release.

So the requirement this change adds is "packaging SHALL NOT consume its own
previous output", not "an aggregator shall not".

## Which of the design's three shapes, and why not the other two

`<forceCreation>true</forceCreation>` on `maven-jar-plugin`, in every module that
shades, **at a pinned version**. That is **shape 3** — the inherited artifact is
refused rather than shaded — in the only form it can take. The literal reading,
*make the empty-jar warning fatal*, is unimplementable: the warning fires on the
correct first run too, so a build that failed on it would never succeed at all.

Two traps in that one line, both found by measurement:

- **`jar.forceCreation` is not an alternative.** Setting the plugin's user
  property in `<properties>` avoids declaring the plugin at all, which is
  tempting because declaring it without a version makes Maven warn. It works on
  maven-jar-plugin **2.x** and is silently inert on **3.x** — the `property =`
  alias was dropped. Under a 3.4.1 pin the property form reproduces
  `duplicate entry: META-INF/THIRD_PARTY_NOTICES.md` exactly as before. It looks
  green on a box whose Maven resolves 2.4 and ships broken to one that does not.
- **So the version has to be pinned**, and that is part of the fix rather than
  hygiene beside it. Nothing pinned `maven-jar-plugin`, so each environment took
  its Maven's super-POM default: **2.4** under the Maven 3.8.7 on this box,
  **3.4.1** under the Maven 3.9.9 in `docker/build/Dockerfile.dist`. The pin is
  3.4.1 — what the release image already resolves — so the published artifact's
  packaging does not move and the developer box comes up to meet it.

**Shape 1, bind shade to a module that has its own artifact**, is refuted by
measurement rather than by cost. `java_runner` has its own sources, its own
`target/classes` and its own artifact, and it reads a jar it wrote. Having an
artifact is not what protects a module; regenerating it is.

**Shape 2, a distinct `finalName` or classifier**, is the expensive one and
fixes half the problem. With `shadedArtifactAttached`, the artifact installed as
`com.flezha:flezha-sdk` becomes the manifest-only jar — and that coordinate is
what `test/apps/java_runner/pom.xml` compiles and links against. The fat jar is
reached by the glob `flezha-sdk-*.jar` in **two** places a second jar in
`target/` would silently change — `scripts/build-dist.sh:297` and
`.github/workflows/java-sdk.yml:89` — and `sdk/java/flezha-sdk/dist/flezha-sdk.jar`
is named in `README.md`, `docs/sdk/java_sdk.md`, `docker/dev/README.md`,
`docker/build/README.md`, `docs/runbooks/purge-build-artifacts.md`,
`scripts/check-build-artifacts.sh`, `.dockerignore` and three specs. All of that
to leave `java_runner` still re-shading.

Adding `clean` to the conformance command was the non-fix: it hides the pom and
slows every build.

## What the `clean`s elsewhere were for

Two call sites carried this defect as a workaround and say so in a comment:

- `scripts/ci-local.sh` — the comment named the shade refusal outright. Both
  `clean`s are gone; the file now mirrors `java-sdk.yml`, which never had them.
- `scripts/build-dist.sh` — `clean` **stays**, for a different and still-true
  reason: `maven-resources-plugin` copies into `target/classes` and never
  deletes, so a native triple removed from the source tree would still ship. The
  comment said shade; it now says that. Measured, not assumed: a file added to
  `flezha-sdk-native/src/main/resources`, built, then deleted from the source
  tree is still in `target/classes` and still in the jar after the next build.

A third site said something about shade that stopped being true earlier and for
another reason: `sdk/java/README.md` warned that `scripts/build-dist.sh` "will
happily shade a stale artifact from your local `~/.m2`". That script's own SDK
step is `mvn … -P native-build -DskipTests clean install` over the whole
reactor, so it installs the tree it was handed and nothing stale is reachable.
The blockquote is gone.

## The proof is the second run

A single green run is what hid this for months, so nothing here is one run.

- the conformance `build.test` command verbatim (`build.bench` is the same
  string, asserted rather than assumed), **three** times in one checkout with no
  `clean`, starting from the `target/` that had just failed: 0, 0, 0 — and again
  three times under the 3.4.1 pin, with `[INFO] Building jar:` present on every
  run and both `original-*.jar` staying thin (2,254 and 210,102 bytes)
- the negative control: with the two configurations removed, `mvn clean` and then
  the same command twice gives **0 then 1**, and with any build in `target/`
  beforehand it fails on the first
- `unzip -l` on the fat jar: one `META-INF/THIRD_PARTY_NOTICES.md`, exactly the
  five `native/<os>-<arch>/libflz_core_ffi.*` and no other native library, no
  duplicated entry name, 1833 entries — the count before the change
- `java_runner.jar` likewise: one notices file, five natives, 2070 entries
- `mvn -B -f sdk/java/pom.xml -P coverage,native-verify clean verify` green, so
  the clean path did not regress
- `FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke` — 1859 passed, then a
  red on `sdk_smoke_e2e_test.exs:55` at **2× the wall time** (133.9 s vs 68.3 s),
  then 1859 passed again, all three against a `java_runner.jar` whose md5 never
  changed because no build ran between them. The artifact is not the variable;
  see KI-18.

## Owed

The requirement has no executable gate. The invariant — a pom that binds
`shade` also sets `forceCreation` on `maven-jar-plugin` — is stated in a comment
in each pom and nowhere a fourth shading module would trip over it. There is no
existing home for it: `test/sdk_conformance/gate_test.exs` is scoped to the
conformance manifests and `scripts/check-build-artifacts.sh` to what git tracks.
