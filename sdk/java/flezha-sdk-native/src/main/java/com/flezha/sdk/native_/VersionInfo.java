package com.flezha.sdk.native_;

/**
 * Build-time SDK version string, mirrored from the Maven {@code project.version}.
 * Used as a path component during native-library extraction; wire-version drift
 * is caught by the poll core's schema negotiation at context creation.
 */
public final class VersionInfo {
    /**
     * Keep this in sync with the Maven project version. The Maven build could
     * generate this file in the future via a templated source plugin; for now
     * it is a hand-maintained constant that CI validates against the pom.
     */
    public static final String VERSION = "0.1.0";

    private VersionInfo() {}
}
