package com.datasift.dropwizard.cassandra;

import com.codahale.metrics.health.HealthCheck;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

/**
 * A Health Check for Cassandra that queries "SELECT now() FROM system.local"
 */
public class CassandraHealthCheck extends HealthCheck {

    private final Session session;

    public CassandraHealthCheck(Session session) {
        this.session = session;
    }

    /**
     * Perform a check Cassandra.
     *
     * @return if Cassandra is healthy, a healthy {@link com.codahale.metrics.health.HealthCheck.Result}; otherwise, an unhealthy {@link
     * com.codahale.metrics.health.HealthCheck.Result} with the exception
     * @throws Exception if there is an unhandled error during the health check; this will result in a failed health check
     */
    @Override
    protected Result check() throws Exception {
        session.execute(new SimpleStatement("SELECT now() FROM system.local;"));
        return Result.healthy();
    }

}
