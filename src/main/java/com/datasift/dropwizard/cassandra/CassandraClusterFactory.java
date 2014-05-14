package com.datasift.dropwizard.cassandra;

import com.datastax.driver.core.*;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import io.dropwizard.util.Size;
import io.dropwizard.util.SizeUnit;
import io.dropwizard.validation.MinDuration;
import io.dropwizard.validation.MinSize;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.constraints.Range;

import javax.net.ssl.SSLContext;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.net.InetAddress;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A factory for Cassandra {@link Cluster} client instances.
 *
 * @see Cluster
 */
public class CassandraClusterFactory {

    @NotEmpty
    private ImmutableList<InetAddress> hosts = ImmutableList.of();

    @Range(min = 1, max = 65535)
    private int port = ProtocolOptions.DEFAULT_PORT;

    @NotNull
    @NotEmpty
    @Valid
    private Optional<String> username = Optional.absent();

    @NotNull
    @NotEmpty
    @Valid
    private Optional<String> password = Optional.absent();

    @NotNull
    @Valid
    private SocketOptionsFactory socket = new SocketOptionsFactory();

    @NotNull
    @Valid
    private Optional<SSLOptionsFactory> ssl = Optional.absent();

    @NotNull
    private ProtocolOptions.Compression compression = ProtocolOptions.Compression.NONE;

    private Duration shutdownTimeout = Duration.seconds(30);

    public ImmutableList<InetAddress> getHosts() {
        return hosts;
    }

    public void setHosts(final ImmutableList<InetAddress> hosts) {
        this.hosts = hosts;
    }

    public int getPort() {
        return port;
    }

    public void setPort(final int port) {
        this.port = port;
    }

    public Optional<String> getUsername() {
        return username;
    }

    public void setUsername(final String username) {
        this.username = Optional.fromNullable(username);
    }

    public Optional<String> getPassword() {
        return password;
    }

    public void setPassword(final String password) {
        this.password = Optional.fromNullable(password);
    }

    public ProtocolOptions.Compression getCompression() {
        return compression;
    }

    public void setCompression(final ProtocolOptions.Compression compression) {
        this.compression = compression;
    }

    public Optional<SSLOptionsFactory> getSSLOptions() {
        return ssl;
    }

    public void setSSLOptions(final SSLOptionsFactory factory) {
        this.ssl = Optional.fromNullable(factory);
    }

    public SocketOptionsFactory getSocketOptions() {
        return socket;
    }

    public void setSocketOptions(final SocketOptionsFactory socketOptions) {
        this.socket = socketOptions;
    }

    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    public void setShutdownTimeout(final Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }

    public Cluster build(final Environment environment) {
        return build(environment, null);
    }

    public Cluster build(final Cluster.Builder builder, final Environment environment) {
        return build(builder, environment, null);
    }

    public Cluster build(final Environment environment, final String name) {
        return build(Cluster.builder(), environment, name);
    }

    public Cluster build(final Cluster.Builder builder,
                         final Environment environment,
                         final String name) {
        Preconditions.checkNotNull(builder, "default Cluster.Builder cannot be null");
        Preconditions.checkNotNull(environment, "Environment cannot be null");

        builder.addContactPoints(getHosts())
                .withClusterName(name)
                .withPort(getPort())
                .withCompression(getCompression())
                .withSocketOptions(getSocketOptions().build())
                .withoutJMXReporting(); // disable JMX reporting, delegate that to Dropwizard

        // optionally add plain-text authentication
        if (getUsername().isPresent()) {
            builder.withAuthProvider(
                    new PlainTextAuthProvider(getUsername().get(), getPassword().or("")));
        }

        // optionally configure SSL
        if (getSSLOptions().isPresent()) {
            builder.withSSL(getSSLOptions().get().build());
        }

        final Cluster cluster = builder.build();

        // manage the client as part of the application lifecycle
        environment.lifecycle().manage(new ManagedCassandraCluster(cluster, getShutdownTimeout()));

        // register the client metrics with the Dropwizard registry
        // since all metrics have already been defined, it's not necessary to attach a listener
        // note: we prefix the metrics with the client ID to prevent metrics from different clients
        // colliding
        environment.metrics().register(
                String.format("cassandra-%s", cluster.getClusterName()),
                cluster.getMetrics().getRegistry());

        return cluster;
    }

    public class SocketOptionsFactory {

        @MinDuration(value = 1, unit = TimeUnit.MILLISECONDS)
        @NotNull
        private Duration connectionTimeout =
                Duration.milliseconds(SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS);

        @MinDuration(value = 1, unit = TimeUnit.MILLISECONDS)
        @NotNull
        private Duration readTimeout =
                Duration.milliseconds(SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS);

        @NotNull
        private Optional<Boolean> keepAlive = Optional.absent();

        @NotNull
        private Optional<Boolean> reuseAddress = Optional.absent();

        @MinDuration(value = 0, unit = TimeUnit.SECONDS)
        @NotNull
        private Optional<Duration> linger = Optional.absent();

        @NotNull
        private Optional<Boolean> noDelay = Optional.absent();

        @MinSize(value = 0, unit = SizeUnit.BYTES)
        @NotNull
        private Optional<Size> receiveBufferSize = Optional.absent();

        @MinSize(value = 0, unit = SizeUnit.BYTES)
        @NotNull
        private Optional<Size> sendBufferSize = Optional.absent();

        public Duration getConnectionTimeout() {
            return connectionTimeout;
        }

        public void setConnectionTimeout(final Duration connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
        }

        public Optional<Boolean> getKeepAlive() {
            return keepAlive;
        }

        public void setKeepAlive(final Optional<Boolean> keepAlive) {
            this.keepAlive = keepAlive;
        }

        public Optional<Duration> getLinger() {
            return linger;
        }

        public void setLinger(final Optional<Duration> linger) {
            this.linger = linger;
        }

        public Optional<Boolean> getNoDelay() {
            return noDelay;
        }

        public void setNoDelay(final Optional<Boolean> noDelay) {
            this.noDelay = noDelay;
        }

        public Duration getReadTimeout() {
            return readTimeout;
        }

        public void setReadTimeout(final Duration readTimeout) {
            this.readTimeout = readTimeout;
        }

        public Optional<Size> getReceiveBufferSize() {
            return receiveBufferSize;
        }

        public void setReceiveBufferSize(final Optional<Size> receiveBufferSize) {
            this.receiveBufferSize = receiveBufferSize;
        }

        public Optional<Boolean> getReuseAddress() {
            return reuseAddress;
        }

        public void setReuseAddress(final Optional<Boolean> reuseAddress) {
            this.reuseAddress = reuseAddress;
        }

        public Optional<Size> getSendBufferSize() {
            return sendBufferSize;
        }

        public void setSendBufferSize(final Optional<Size> sendBufferSize) {
            this.sendBufferSize = sendBufferSize;
        }


        SocketOptions build() {
            final SocketOptions options = new SocketOptions();
            options.setConnectTimeoutMillis((int) connectionTimeout.toMilliseconds());
            options.setReadTimeoutMillis((int) readTimeout.toMilliseconds());

            if (keepAlive.isPresent()) {
                options.setKeepAlive(keepAlive.get());
            }

            if (reuseAddress.isPresent()) {
                options.setReuseAddress(reuseAddress.get());
            }

            if (linger.isPresent()) {
                options.setSoLinger((int) linger.get().toSeconds());
            }

            if (noDelay.isPresent()) {
                options.setTcpNoDelay(noDelay.get());
            }

            if (receiveBufferSize.isPresent()) {
                options.setReceiveBufferSize((int) receiveBufferSize.get().toBytes());
            }

            if (sendBufferSize.isPresent()) {
                options.setSendBufferSize((int) sendBufferSize.get().toBytes());
            }

            return options;
        }
    }

    public class SSLOptionsFactory {

        @NotNull
        private Set<String> excludedCipherSuites = ImmutableSet.of();

        public Set<String> getExcludedCipherSuites() {
            return excludedCipherSuites;
        }

        public void setExcludedCipherSuites(final Set<String> excludedCipherSuites) {
            this.excludedCipherSuites = excludedCipherSuites;
        }

        // todo: build this out based loosely on the syle in HttpsConnectorFactory
        // note: this will require a lot of work to build the context for the given settings

        SSLOptions build() {
            try {
                final SSLContext context = SSLContext.getInstance("TLS");
                context.init(null, null, null);

                // filter out excluded cipher suites
                final List<String> cipherSuites = new ArrayList<String>(
                        SSLOptions.DEFAULT_SSL_CIPHER_SUITES.length);
                for (final String suite : SSLOptions.DEFAULT_SSL_CIPHER_SUITES) {
                    if (!excludedCipherSuites.contains(suite)) {
                        cipherSuites.add(suite);
                    }
                }

                return new SSLOptions(context, cipherSuites.toArray(new String[cipherSuites.size()]));
            } catch (final NoSuchAlgorithmException e) {
                throw new RuntimeException("JVM doesn't support TLS, this should never happen");
            } catch (final KeyManagementException e) {
                throw new IllegalArgumentException("Cannot initialize SSL context", e);
            }
        }
    }
}
