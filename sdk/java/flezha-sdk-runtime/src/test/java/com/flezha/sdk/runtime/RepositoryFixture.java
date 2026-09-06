package com.flezha.sdk.runtime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A file of {@code test/fixtures/}, read from the repository this module sits in.
 *
 * <p>Those fixtures are the cross-language contracts (the declaration keys, the knob parse rule,
 * the reference topologies), so what this suite reads is what every other SDK reads. The suite runs
 * from the module directory and the fixtures are the repository's, which is the whole of what this
 * class knows: one resolver, so a fixture cannot be found by one test and missed by another.
 */
public final class RepositoryFixture {

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

    private static Path resolve(String path) {
        Path fromModule = Path.of("..", "..", "..").resolve(path).normalize();
        return Files.exists(fromModule) ? fromModule : Path.of(path);
    }
}
