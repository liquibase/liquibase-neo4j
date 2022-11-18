package liquibase.ext.neo4j.database.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.util.Properties;
import java.util.logging.Logger;

import static liquibase.ext.neo4j.database.jdbc.SupportedJdbcUrl.IS_SUPPORTED_JDBC_URL;

public class Neo4jDriver implements Driver {

    static final ProjectVersion VERSION = ProjectVersion.parse();

    @Override
    public Connection connect(String url, Properties info) {
        return new Neo4jConnection(url, info);
    }

    @Override
    public boolean acceptsURL(String url) {
        return IS_SUPPORTED_JDBC_URL.test(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
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
