package com.flezha.sdk.runtime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A member of the contract set, read through the directory its repository-relative paths hang off.
 *
 * <p>The set is the cross-language contracts (the declaration keys, the knob parse rule, the
 * reference topologies, the C ABI surface), and {@code test/fixtures/contract.txt} names every one.
 * The release publishes them as one archive, so an SDK developed outside the core's repository
 * vendors them at {@code contract/} rather than walking to a tree it has not got.
 *
 * <p>The directory is resolved once, in the order every SDK uses: {@code $FLEZHA_CONTRACT_DIR},
 * else {@code contract/} at the package root, else the tree this module sits in. Resolving nowhere
 * is a failure naming the three, never a skip.
 */
public final class RepositoryFixture {

    /** The member every candidate is recognised by. */
    private static final String MANIFEST = "test/fixtures/contract.txt";

    private static Path ROOT;

    private RepositoryFixture() {}

    /**
     * Every line of the named repository-relative file, with comments and blanks dropped. The LINE
     * is trimmed, so a caller that splits it keeps the whitespace inside a column and loses it from
     * the first and the last one. A fixture whose outer column measures a stray space is not this
     * method's to read.
     */
    public static List<String> lines(String path) throws IOException {
        List<String> lines = new ArrayList<>();
        for (String line : Files.readAllLines(resolve(path))) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
                lines.add(trimmed);
            }
        }
        return lines;
    }

    /**
     * Every row of a {@code |}-separated fixture, as its trimmed columns. A fixture where the
     * whitespace around an interior value is itself the case reads {@link #lines(String)} and
     * splits the line itself.
     */
    public static List<List<String>> rows(String path) throws IOException {
        List<List<String>> rows = new ArrayList<>();
        for (String line : lines(path)) {
            rows.add(Arrays.stream(line.split("\\|")).map(String::trim).toList());
        }
        return rows;
    }

    /** The contract set's root, resolved once for every reader in this suite. */
    private static Path contractRoot() {
        if (ROOT != null) {
            return ROOT;
        }
        List<Path> candidates = candidates();
        for (Path candidate : candidates) {
            if (candidate != null && Files.isRegularFile(candidate.resolve(MANIFEST))) {
                return ROOT = candidate;
            }
        }
        List<String> unresolved = List.of(
                "$FLEZHA_CONTRACT_DIR (unset)",
                "",
                "no directory above " + Path.of("").toAbsolutePath() + " carries " + MANIFEST);
        StringBuilder tried = new StringBuilder();
        for (int at = 0; at < candidates.size(); at++) {
            Path candidate = candidates.get(at);
            tried.append("\n  ").append(candidate == null ? unresolved.get(at) : candidate);
        }
        throw new IllegalStateException("the contract set is nowhere this suite looked:" + tried
                + "\nUnpack flezha-fixtures-<version>.tar.gz at contract/, or set FLEZHA_CONTRACT_DIR.");
    }

    private static List<Path> candidates() {
        String configured = System.getenv("FLEZHA_CONTRACT_DIR");
        // A module directory is one below the package root, whatever the suite
        // was started from; the tree is whichever ancestor carries the manifest.
        return Arrays.asList(
                configured == null || configured.isEmpty() ? null : Path.of(configured),
                Path.of("..", "contract").normalize(),
                enclosingTree());
    }

    /** The nearest ancestor of the working directory that carries the manifest. */
    private static Path enclosingTree() {
        for (Path at = Path.of("").toAbsolutePath(); at != null; at = at.getParent()) {
            if (Files.isRegularFile(at.resolve(MANIFEST))) {
                return at;
            }
        }
        return null;
    }

    private static Path resolve(String path) {
        return contractRoot().resolve(path).normalize();
    }
}
