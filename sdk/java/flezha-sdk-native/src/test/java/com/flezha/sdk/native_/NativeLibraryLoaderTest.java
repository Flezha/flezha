package com.flezha.sdk.native_;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class NativeLibraryLoaderTest {

    @Test
    @DisplayName("detectPlatform maps os+arch to a supported triple slug")
    void detectPlatform() {
        String platform = NativeLibraryLoader.detectPlatform();
        assertThat(platform).matches("(linux|macos|windows)-(x64|arm64)");
    }

    @Test
    @DisplayName("extensionFor returns so/dylib/dll by os key")
    void extensionFor() {
        assertThat(NativeLibraryLoader.extensionFor("linux-x64")).isEqualTo("so");
        assertThat(NativeLibraryLoader.extensionFor("macos-arm64")).isEqualTo("dylib");
        assertThat(NativeLibraryLoader.extensionFor("windows-x64")).isEqualTo("dll");
        assertThatThrownBy(() -> NativeLibraryLoader.extensionFor("beos-x64"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("sha256 produces a 16-char hex prefix")
    void sha256Hex() {
        String h = NativeLibraryLoader.sha256("hello".getBytes());
        assertThat(h).hasSize(16).matches("[0-9a-f]+");
    }
}
