package com.datasift.dropwizard.cassandra;

import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

/**
 * Manages a Cassandra {@link Cluster} connection as part of the application lifecycle.
 *
 * @see Managed
 * @see Cluster
 */
public class ManagedCassandraCluster implements Managed {

    private final Logger logger;

    private final Cluster cluster;
    private final Duration shutdownTimeout;

    public ManagedCassandraCluster(final Cluster cluster, final Duration shutdownTimeout) {
        this.cluster = cluster;
        this.shutdownTimeout = shutdownTimeout;
        this.logger = LoggerFactory.getLogger(String.format("cassandra-client-%s", cluster.getClusterName()));
    }


    @Override
    public void start() throws Exception {
        // eagerly initialise connection to cluster
        cluster.init();

        final Metadata metadata = cluster.getMetadata();
        logger.info("Connected to cluster: {}", metadata.getClusterName());

        if (logger.isDebugEnabled()) {
            for (final Host host : metadata.getAllHosts()) {
                logger.debug("Host: {}, DC: {}, Rack: {}, Version: {}",
                        host.getAddress(),
                        host.getDatacenter(),
                        host.getRack(),
                        host.getCassandraVersion());
            }
        }
    }

    @Override
    public void stop() throws Exception {
        final CloseFuture future = cluster.closeAsync();
        try {
            future.get(shutdownTimeout.getQuantity(), shutdownTimeout.getUnit());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (final TimeoutException e) {
            future.force();
            future.get();
        }
    }
}
