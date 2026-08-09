package com.flezha.sdk.native_;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Extracts a bundled native library ({@code libflz_core_ffi}) from the
 * classpath resource {@code /native/<os>-<arch>/} into a writable directory
 * and loads it via {@link System#load(String)}. Safe under concurrent JVMs
 * because the target path includes a content hash of the binary.
 */
final class NativeLibraryLoader {

    static final String RESOURCE_PREFIX = "/native/";
    static final String NATIVE_LIB_DIR_ENV = "FLEZHA_NATIVE_LIB_DIR";

    private static final Set<String> SUPPORTED =
            Set.of("linux-x64", "linux-arm64", "macos-x64", "macos-arm64", "windows-x64");

    private static final Set<String> LOADED = ConcurrentHashMap.newKeySet();

    private NativeLibraryLoader() {}

    /** Extract and load the named bundled library exactly once per JVM. */
    static synchronized void loadNativeLibrary(String baseName) {
        if (LOADED.contains(baseName)) {
            return;
        }

        String platform = detectPlatform();
        String extension = extensionFor(platform);
        String resourcePath = RESOURCE_PREFIX + platform + "/" + baseName + "." + extension;

        byte[] libraryBytes;
        try (InputStream in = NativeLibraryLoader.class.getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new UnsatisfiedLinkError("Missing bundled native library for " + platform
                        + " (resource " + resourcePath + " not on classpath). "
                        + "Supported platforms: " + SUPPORTED);
            }
            libraryBytes = in.readAllBytes();
        } catch (IOException e) {
            UnsatisfiedLinkError err =
                    new UnsatisfiedLinkError("Failed to read bundled native library from classpath: " + resourcePath);
            err.initCause(e);
            throw err;
        }

        String hash = sha256(libraryBytes);
        String version = VersionInfo.VERSION;

        Path extractionDir = resolveExtractionDir().resolve("flezha-" + version + "-" + hash);
        Path target = extractionDir.resolve(baseName + "." + extension);

        try {
            Files.createDirectories(extractionDir);

            if (!Files.exists(target) || Files.size(target) != libraryBytes.length) {
                Path tmp = Files.createTempFile(extractionDir, baseName + "-", "." + extension + ".tmp");
                Files.write(tmp, libraryBytes);
                try {
                    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException atomicFallback) {
                    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        } catch (IOException e) {
            UnsatisfiedLinkError err = new UnsatisfiedLinkError("Failed to extract native library to " + extractionDir);
            err.initCause(e);
            throw err;
        }

        try {
            System.load(target.toAbsolutePath().toString());
        } catch (UnsatisfiedLinkError e) {
            throw new UnsatisfiedLinkError("System.load failed for " + target + ": " + e.getMessage());
        }

        LOADED.add(baseName);
    }

    static String detectPlatform() {
        String osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String osArch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);

        String osKey;
        if (osName.contains("linux")) {
            osKey = "linux";
        } else if (osName.contains("mac") || osName.contains("darwin")) {
            osKey = "macos";
        } else if (osName.contains("windows")) {
            osKey = "windows";
        } else {
            throw new UnsatisfiedLinkError(
                    "Unsupported OS: " + osName + " (arch=" + osArch + "). " + "Supported: " + SUPPORTED);
        }

        String archKey;
        if (osArch.equals("amd64") || osArch.equals("x86_64") || osArch.equals("x64")) {
            archKey = "x64";
        } else if (osArch.equals("aarch64") || osArch.equals("arm64")) {
            archKey = "arm64";
        } else {
            throw new UnsatisfiedLinkError(
                    "Unsupported CPU arch: " + osArch + " (os=" + osName + "). " + "Supported: " + SUPPORTED);
        }

        String platform = osKey + "-" + archKey;
        if (!SUPPORTED.contains(platform)) {
            throw new UnsatisfiedLinkError(
                    "Unsupported os/arch combination: " + platform + ". " + "Supported: " + SUPPORTED);
        }
        return platform;
    }

    static String extensionFor(String platform) {
        if (platform.startsWith("linux")) {
            return "so";
        }
        if (platform.startsWith("macos")) {
            return "dylib";
        }
        if (platform.startsWith("windows")) {
            return "dll";
        }
        throw new IllegalArgumentException("Unknown platform: " + platform);
    }

    private static Path resolveExtractionDir() {
        String override = System.getenv(NATIVE_LIB_DIR_ENV);
        if (override != null && !override.isBlank()) {
            return Paths.get(override);
        }
        String tmp = System.getProperty("java.io.tmpdir");
        return Paths.get(tmp == null || tmp.isBlank() ? "/tmp" : tmp);
    }

    static String sha256(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] raw = digest.digest(bytes);
            StringBuilder sb = new StringBuilder(raw.length * 2);
            for (byte b : raw) {
                String hex = Integer.toHexString(b & 0xFF);
                if (hex.length() == 1) {
                    sb.append('0');
                }
                sb.append(hex);
            }
            return sb.substring(0, 16); // 16 hex chars is plenty for path uniqueness
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
