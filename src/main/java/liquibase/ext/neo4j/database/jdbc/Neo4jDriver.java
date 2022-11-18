package liquibase.ext.neo4j.database.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import static liquibase.ext.neo4j.database.jdbc.SupportedUrlPredicate.IS_SUPPORTED_URL;
import static liquibase.ext.neo4j.database.jdbc.SupportedUrlPredicate.normalizeUri;

public class Neo4jDriver implements Driver {

    private static final ProjectVersion VERSION = ProjectVersion.parse();

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return new Neo4jConnection(normalizeUri(url), info);
    }

    @Override
    public boolean acceptsURL(String url) {
        return IS_SUPPORTED_URL.test(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return VERSION.getMajor();
    }

    @Override
    public int getMinorVersion() {
        return VERSION.getMinor();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        return null;
    }
}
