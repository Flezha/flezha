package com.flezha.sdk.native_;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Transport-layer configuration passed to
 * {@link NativeTransport#contextNew(FlezhaConfig)}. Mirrors the C FFI
 * {@code FlezhaConfig} struct. All fields honor the defaults documented in
 * {@code docs/sdk_protocol_spec.md} §2.
 */
public record FlezhaConfig(
        @Nonnull FlezhaTransport transport,
        @Nonnull String quicAddress,
        @Nonnull String caCertPath,
        @Nullable String serverCertPath,
        @Nullable String serverKeyPath,
        @Nonnull String socketFilesPath,
        int workerPoolSize) {

    public static final String DEFAULT_QUIC_ADDRESS = "127.0.0.1:4433";
    public static final String DEFAULT_CA_CERT_PATH = "app_data/certs/ca.pem";
    public static final String DEFAULT_SERVER_CERT_PATH = "app_data/certs/cert.pem";
    public static final String DEFAULT_SERVER_KEY_PATH = "app_data/certs/key.pem";
    public static final String DEFAULT_SOCKET_FILES_PATH = "app_data/sockets";

    public FlezhaConfig {
        if (workerPoolSize < 0) {
            throw new IllegalArgumentException("workerPoolSize must be >= 0");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static FlezhaConfig defaults() {
        return builder().build();
    }

    public static final class Builder {
        private FlezhaTransport transport = FlezhaTransport.UDS;
        private String quicAddress = DEFAULT_QUIC_ADDRESS;
        private String caCertPath = DEFAULT_CA_CERT_PATH;
        @Nullable private String serverCertPath = DEFAULT_SERVER_CERT_PATH;
        @Nullable private String serverKeyPath = DEFAULT_SERVER_KEY_PATH;
        private String socketFilesPath = DEFAULT_SOCKET_FILES_PATH;
        private int workerPoolSize = 0;

        public Builder transport(@Nonnull FlezhaTransport transport) {
            this.transport = transport;
            return this;
        }

        public Builder quicAddress(@Nonnull String quicAddress) {
            this.quicAddress = quicAddress;
            return this;
        }

        public Builder caCertPath(@Nonnull String caCertPath) {
            this.caCertPath = caCertPath;
            return this;
        }

        public Builder serverCertPath(@Nullable String serverCertPath) {
            this.serverCertPath = serverCertPath;
            return this;
        }

        public Builder serverKeyPath(@Nullable String serverKeyPath) {
            this.serverKeyPath = serverKeyPath;
            return this;
        }

        public Builder socketFilesPath(@Nonnull String socketFilesPath) {
            this.socketFilesPath = socketFilesPath;
            return this;
        }

        public Builder workerPoolSize(int workerPoolSize) {
            this.workerPoolSize = workerPoolSize;
            return this;
        }

        public FlezhaConfig build() {
            return new FlezhaConfig(
                    transport,
                    quicAddress,
                    caCertPath,
                    serverCertPath,
                    serverKeyPath,
                    socketFilesPath,
                    workerPoolSize);
        }
    }
}
