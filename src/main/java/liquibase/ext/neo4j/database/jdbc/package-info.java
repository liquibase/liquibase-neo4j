/**
 * <p>This JDBC implementation is tailored for Liquibase.
 * This is not a general-purpose JDBC connector.</p>
 *
 * <p>In particular, there is no caching in place.
 * A native Neo4j driver is created every time a new connection is requested.
 * This suits the Liquibase usage pattern very well since it only ever uses a single connection through the whole
 * migrations' execution.</p>
 *
 * <p>The java.sql.Driver implementation is not registered as a service.
 * As a consequence, this connector is not available via the usual Driver SPI.</p>
 */
package liquibase.ext.neo4j.database.jdbc;
